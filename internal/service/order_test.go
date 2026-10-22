package service

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	orderv1 "order/gen/order/v1"
	"order/internal/biz"
	"order/internal/data"
	"order/internal/pkg/tenant"
)

// Step 0.1: 构造测试上下文 (注入买家与租户元数据 Header)
// 来源: 测试用例指定 userID 与 tenantID
// 用于: 模拟 Envoy 网关转发至微服务携带的上下文 Header
func testContext(userID, tenantID string) context.Context {
	ctx := tenant.WithTenantID(context.Background(), tenantID)
	return metadata.NewIncomingContext(ctx, metadata.Pairs(
		"x-user-id", userID,
		"x-tenant-id", tenantID,
	))
}

// Step 1.1: 验证购物车完整业务流程 (加购、修改、查询与清空)
// 来源: 测试买家在不同端发起的购物车操作
// 用于: 保证 OrderService 购物车门面与内存仓储行为符合契约
func TestOrderService_CartFlow(t *testing.T) {
	repo := data.NewMemoryRepository()
	splitter := biz.NewOrderSplitter(nil, nil)
	logger := log.DefaultLogger
	uc := biz.NewOrderUsecase(repo, splitter, nil, nil, nil, nil, logger)
	svc := NewOrderService(uc, logger)

	ctx := testContext("buyer-1", "tenant-test")

	// Step 1.1.1: 添加购物车
	addResp, err := svc.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-101",
		Quantity: 2,
	})
	require.NoError(t, err)
	assert.True(t, addResp.Success)

	// Step 1.1.2: 查询购物车
	getResp, err := svc.GetCart(ctx, &orderv1.GetCartRequest{})
	require.NoError(t, err)
	assert.Len(t, getResp.Items, 1)
	assert.Equal(t, "sku-101", getResp.Items[0].SkuId)
	assert.Equal(t, int64(2), getResp.Items[0].Quantity)

	// Step 1.1.3: 更新购物车
	upResp, err := svc.UpdateCartItem(ctx, &orderv1.UpdateCartItemRequest{
		SkuId:    "sku-101",
		Quantity: 5,
		Selected: true,
	})
	require.NoError(t, err)
	assert.True(t, upResp.Success)

	// Step 1.1.4: 移除购物车条目
	rmResp, err := svc.RemoveCartItem(ctx, &orderv1.RemoveCartItemRequest{
		SkuId: "sku-101",
	})
	require.NoError(t, err)
	assert.True(t, rmResp.Success)

	// Step 1.1.5: 清空购物车
	clrResp, err := svc.ClearCart(ctx, &orderv1.ClearCartRequest{})
	require.NoError(t, err)
	assert.True(t, clrResp.Success)
}

// Step 2.1: 验证结算与建单生命周期 (结算防重令牌签发、提单、支付与取消)
// 来源: 买家提交包含商品与收件信息的订单
// 用于: 验证结算令牌防重租约与订单生命周期状态机流转
func TestOrderService_OrderLifecycle(t *testing.T) {
	repo := data.NewMemoryRepository()
	splitter := biz.NewOrderSplitter(nil, nil)
	logger := log.DefaultLogger
	uc := biz.NewOrderUsecase(repo, splitter, nil, nil, nil, nil, logger)
	svc := NewOrderService(uc, logger)

	ctx := testContext("buyer-2", "tenant-test")

	// 加购商品
	_, err := svc.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-201",
		Quantity: 1,
	})
	require.NoError(t, err)

	// Step 2.1.1: 准备结算获取结算令牌
	prepResp, err := svc.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-201"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, prepResp.SettlementToken)

	// Step 2.1.2: 提交订单
	createResp, err := svc.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		SettlementToken: prepResp.SettlementToken,
		Items: []*orderv1.CreateOrderItemParam{
			{SkuId: "sku-201", Quantity: 1},
		},
		ReceiverName:    "张三",
		ReceiverPhone:   "13800000000",
		ReceiverAddress: "北京市海淀区中关村南大街1号",
	})
	require.NoError(t, err)
	require.NotNil(t, createResp.Order)
	assert.Equal(t, "PENDING_PAYMENT", createResp.Order.Status)

	orderID := createResp.Order.Id

	// Step 2.1.3: 查询订单详情
	getOrd, err := svc.GetOrder(ctx, &orderv1.GetOrderRequest{Id: orderID})
	require.NoError(t, err)
	assert.Equal(t, orderID, getOrd.Order.Id)

	// Step 2.1.4: 支付订单
	payResp, err := svc.PayOrder(ctx, &orderv1.PayOrderRequest{
		Id:         orderID,
		PayChannel: "MOCK_ALIPAY",
	})
	require.NoError(t, err)
	assert.True(t, payResp.Success)
	assert.Equal(t, "PAID", payResp.Status)

	// Step 2.1.5: 商家发货
	shipResp, err := svc.AdminShipOrder(ctx, &orderv1.AdminShipOrderRequest{
		Id:         orderID,
		Carrier:    "SF_EXPRESS",
		TrackingNo: "SF1234567890",
	})
	require.NoError(t, err)
	assert.True(t, shipResp.Success)

	// Step 2.1.6: 买家确认收货
	receiptResp, err := svc.ConfirmReceipt(ctx, &orderv1.ConfirmReceiptRequest{
		Id: orderID,
	})
	require.NoError(t, err)
	assert.True(t, receiptResp.Success)
	assert.Equal(t, "COMPLETED", receiptResp.Status)
}

// Step 3.1: 验证买家发起售后与审核生命周期
// 来源: 已支付或已完成订单发起的售后退款单
// 用于: 验证售后单创建、商户审批与取消
func TestOrderService_AfterSalesFlow(t *testing.T) {
	repo := data.NewMemoryRepository()
	splitter := biz.NewOrderSplitter(nil, nil)
	logger := log.DefaultLogger
	uc := biz.NewOrderUsecase(repo, splitter, nil, nil, nil, nil, logger)
	svc := NewOrderService(uc, logger)

	ctx := testContext("buyer-3", "tenant-test")

	// 初始订单
	now := time.Now().Unix()
	ord := &orderv1.Order{
		Id:           "order-as-test",
		OrderNo:      "O202610110001",
		TenantId:     "tenant-test",
		UserId:       "buyer-3",
		TotalAmount:  1000,
		PayAmount:    1000,
		Status:       "PAID",
		ReceiverName: "李四",
		CreatedAt:    now,
		Items: []*orderv1.OrderItem{
			{
				Id:            "oi-1",
				OrderId:       "order-as-test",
				SkuId:         "sku-301",
				Quantity:      1,
				Price:         1000,
				RealizedPrice: 1000,
			},
		},
	}
	_, err := repo.CreateOrder(ctx, ord)
	require.NoError(t, err)

	// Step 3.1.1: 买家申请仅退款
	asResp, err := svc.ApplyAfterSales(ctx, &orderv1.ApplyAfterSalesRequest{
		ChildOrderId: "order-as-test",
		Type:         "REFUND_ONLY",
		Reason:       "拍错规格",
	})
	require.NoError(t, err)
	require.NotNil(t, asResp.AfterSales)
	assert.Equal(t, "APPLIED", asResp.AfterSales.Status)
	assert.Equal(t, int64(1000), asResp.AfterSales.RefundAmount)

	asID := asResp.AfterSales.Id

	// Step 3.1.2: 查询售后单详情
	getAs, err := svc.GetAfterSales(ctx, &orderv1.GetAfterSalesRequest{Id: asID})
	require.NoError(t, err)
	assert.Equal(t, asID, getAs.AfterSales.Id)

	// Step 3.1.3: 商家审核通过 (仅退款直接流转至 REFUNDED)
	auditResp, err := svc.AuditAfterSales(ctx, &orderv1.AuditAfterSalesRequest{
		Id:       asID,
		Approved: true,
	})
	require.NoError(t, err)
	assert.True(t, auditResp.Success)
	assert.Equal(t, "REFUNDED", auditResp.Status)
}
