package service

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"order/internal/biz"
	"order/internal/data"
	orderv1 "order/gen/order/v1"
)

// Step 1.1: 验证主订单编号由结算令牌确定性推导
// 来源: biz.GenerateSettlementToken 与 biz.DeriveParentOrderNo
// 用于: 同一令牌在任何节点、任何次重试中必须推导出完全相同的订单号，
//
//	否则 DTM Saga 全局事务 ID、库存幂等键与数据库唯一索引将无法命中同一条业务记录
func TestSettlementToken_DeterministicParentOrderNo(t *testing.T) {
	token := biz.GenerateSettlementToken()

	no1 := biz.DeriveParentOrderNo("tenant-a", token)
	no2 := biz.DeriveParentOrderNo("tenant-a", token)
	assert.Equal(t, no1, no2, "同一租户同一令牌必须推导出完全相同的订单号")

	no3 := biz.DeriveParentOrderNo("tenant-b", token)
	assert.NotEqual(t, no1, no3, "跨租户同令牌必须推导出不同订单号，避免跨租户撞单")

	otherToken := biz.GenerateSettlementToken()
	assert.NotEqual(t, no1, biz.DeriveParentOrderNo("tenant-a", otherToken), "不同令牌必须推导出不同订单号")

	assert.Contains(t, no1, "PORD", "订单号必须保留平台统一前缀")
}

// Step 2.1: 验证租约续期心跳可在长耗时建单期间持续持有租约
// 来源: 内存仓储的 AcquireSettlementToken 与 RenewSettlementToken
// 用于: 杜绝 DTM Saga 慢链路耗时超过租约窗口后被他人接管并重复建单
func TestSettlementToken_LeaseRenewalKeepsLeaseAlive(t *testing.T) {
	repo := data.NewMemoryRepository()
	ctx := testContext("buyer-lease", "tenant-lease")
	token := "st-1700000000000-lease-token"
	require.NoError(t, repo.SaveSettlementToken(ctx, "buyer-lease", token, biz.SettlementTokenInitTTL))

	lease := 200 * time.Millisecond
	res, err := repo.AcquireSettlementToken(ctx, "buyer-lease", token, lease)
	require.NoError(t, err)
	require.Equal(t, "PROCEED", res.Action)
	require.NotZero(t, res.LockTime, "抢占成功必须返回租约起算点，供续期 CAS 比对")

	// Step 2.1.1: 租约窗口内执行一次续期心跳
	time.Sleep(150 * time.Millisecond)
	newLock, ok, err := repo.RenewSettlementToken(ctx, token, res.LockTime, biz.SettlementTokenKeyTTL)
	require.NoError(t, err)
	require.True(t, ok, "持有者必须能成功续期")
	require.NotEqual(t, res.LockTime, newLock, "续期必须刷新租约起算点")

	// Step 2.1.2: 此刻已远超原始租约窗口 (200ms)，但因续期仍在持有中，连击必须被拦截
	time.Sleep(150 * time.Millisecond)
	res2, err := repo.AcquireSettlementToken(ctx, "buyer-lease", token, lease)
	require.NoError(t, err)
	assert.Equal(t, "STILL_PROCESSING", res2.Action, "续期后连击必须继续被阻断，不得触发租约自愈接管")
}

// Step 2.2: 验证续期 CAS 拒绝已被他人接管的租约
// 来源: 租约超时自愈后原持有者的续期请求
// 用于: 防止已失去租约的僵尸节点覆盖新持有者的租约起算点
func TestSettlementToken_LeaseRenewalRejectsStolenLease(t *testing.T) {
	repo := data.NewMemoryRepository()
	ctx := testContext("buyer-stolen", "tenant-stolen")
	token := "st-1700000000000-stolen-token"
	require.NoError(t, repo.SaveSettlementToken(ctx, "buyer-stolen", token, biz.SettlementTokenInitTTL))

	lease := 100 * time.Millisecond
	first, err := repo.AcquireSettlementToken(ctx, "buyer-stolen", token, lease)
	require.NoError(t, err)
	require.Equal(t, "PROCEED", first.Action)

	// Step 2.2.1: 等待租约超时后由另一节点接管
	time.Sleep(150 * time.Millisecond)
	second, err := repo.AcquireSettlementToken(ctx, "buyer-stolen", token, lease)
	require.NoError(t, err)
	require.Equal(t, "PROCEED", second.Action, "租约超时后必须允许自愈接管")
	require.NotEqual(t, first.LockTime, second.LockTime, "接管者必须写入新的租约起算点")

	// Step 2.2.2: 原持有者续期必须失败，立即感知自己已失去租约
	newLock, ok, err := repo.RenewSettlementToken(ctx, token, first.LockTime, biz.SettlementTokenKeyTTL)
	require.NoError(t, err)
	assert.False(t, ok, "已被接管的租约不得续期成功")
	assert.Zero(t, newLock)
}

// Step 3.1: 验证数据库唯一索引兜底阻断同一结算令牌重复建单
// 来源: 内存仓储模拟的 UNIQUE (tenant_id, settlement_token) 语义
// 用于: Redis 脑裂丢数据或租约自愈重入时，由数据库层阻断重复订单
func TestSettlementToken_UniqueIndexBlocksDuplicateParentOrder(t *testing.T) {
	repo := data.NewMemoryRepository()
	ctx := testContext("buyer-dup", "tenant-dup")
	token := "st-1700000000000-dup-token"

	parent := &orderv1.ParentOrder{
		OrderNo: "PORD20260101000000ABCDEF",
		UserId:  "buyer-dup",
		Status:  "PENDING_PAYMENT",
	}

	_, _, err := repo.CreateParentOrder(ctx, parent, nil, token)
	require.NoError(t, err, "首次建单必须成功")

	_, _, err = repo.CreateParentOrder(ctx, parent, nil, token)
	require.ErrorIs(t, err, biz.ErrDuplicateSettlementToken, "同一令牌第二次建单必须被唯一索引阻断")
}

// countingOrderRepo 统计主订单读取次数的仓储装饰器
// 来源: 包裹真实仓储实例
// 用于: 断言弱网重试的幂等回显链路完全不穿透 MySQL
type countingOrderRepo struct {
	biz.OrderRepo
	getParentCalls int
}

func (c *countingOrderRepo) GetParentOrder(ctx context.Context, parentID string) (*orderv1.ParentOrder, error) {
	c.getParentCalls++
	return c.OrderRepo.GetParentOrder(ctx, parentID)
}

// Step 5.1: 验证弱网重试的幂等回显纯内存完成，不穿透 MySQL
// 来源: 买家携带同一结算令牌重复发起提单
// 用于: 断言回显请求只走一次 Redis 原子探测, 主单快照直接从 Redis Hash 提取
func TestSettlementToken_ReplayIsPureMemory(t *testing.T) {
	base := data.NewMemoryRepository()
	repo := &countingOrderRepo{OrderRepo: base}
	splitter := biz.NewOrderSplitter(nil, nil)
	logger := log.DefaultLogger
	uc := biz.NewOrderUsecase(repo, splitter, nil, nil, nil, nil, logger)
	svc := NewOrderService(uc, logger)

	ctx := testContext("buyer-snapshot", "tenant-snapshot")

	_, err := svc.AddCartItem(ctx, &orderv1.AddCartItemRequest{SkuId: "sku-951", Quantity: 1})
	require.NoError(t, err)

	prepResp, err := svc.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{SkuIds: []string{"sku-951"}})
	require.NoError(t, err)
	require.NotEmpty(t, prepResp.SettlementToken)

	createReq := &orderv1.CreateOrderRequest{
		SettlementToken: prepResp.SettlementToken,
		Items:           []*orderv1.CreateOrderItemParam{{SkuId: "sku-951", Quantity: 1}},
		ReceiverName:    "赵六",
		ReceiverPhone:   "13700000000",
		ReceiverAddress: "广州市天河区珠江新城花城大道1号",
	}

	first, err := svc.CreateOrder(ctx, createReq)
	require.NoError(t, err)
	require.NotNil(t, first.ParentOrder)
	assert.Equal(t, 0, repo.getParentCalls, "首次建单不应读取主订单表")

	// Step 5.1.1: 弱网超时重试
	second, err := svc.CreateOrder(ctx, createReq)
	require.NoError(t, err)
	require.NotNil(t, second.ParentOrder)

	assert.Equal(t, 0, repo.getParentCalls, "幂等回显必须纯内存完成，绝不读取 MySQL 主订单表")
	assert.Equal(t, first.ParentOrder.Id, second.ParentOrder.Id)
	assert.Equal(t, first.ParentOrder.OrderNo, second.ParentOrder.OrderNo)
	require.Len(t, second.ParentOrder.ChildOrders, len(first.ParentOrder.ChildOrders))
	require.Len(t, second.ChildOrders, len(first.ChildOrders))
	assert.Equal(t, first.ChildOrders[0].OrderNo, second.ChildOrders[0].OrderNo)
	assert.Equal(t, first.Order.Id, second.Order.Id)
}

// 来源: 买家携带同一结算令牌重复发起提单
// 用于: 验证 SUCCESS 终态回显链路，重复请求不会二次落库
func TestSettlementToken_RetryReplaysSingleOrder(t *testing.T) {
	repo := data.NewMemoryRepository()
	splitter := biz.NewOrderSplitter(nil, nil)
	logger := log.DefaultLogger
	uc := biz.NewOrderUsecase(repo, splitter, nil, nil, nil, nil, logger)
	svc := NewOrderService(uc, logger)

	ctx := testContext("buyer-retry", "tenant-retry")

	_, err := svc.AddCartItem(ctx, &orderv1.AddCartItemRequest{SkuId: "sku-901", Quantity: 1})
	require.NoError(t, err)

	prepResp, err := svc.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{SkuIds: []string{"sku-901"}})
	require.NoError(t, err)
	require.NotEmpty(t, prepResp.SettlementToken)

	createReq := &orderv1.CreateOrderRequest{
		SettlementToken: prepResp.SettlementToken,
		Items:           []*orderv1.CreateOrderItemParam{{SkuId: "sku-901", Quantity: 1}},
		ReceiverName:    "王五",
		ReceiverPhone:   "13900000000",
		ReceiverAddress: "上海市浦东新区世纪大道100号",
	}

	first, err := svc.CreateOrder(ctx, createReq)
	require.NoError(t, err)
	require.NotNil(t, first.Order)
	require.NotNil(t, first.ParentOrder)

	// Step 4.1.1: 模拟弱网超时后的自动重试 (携带完全相同的结算令牌)
	second, err := svc.CreateOrder(ctx, createReq)
	require.NoError(t, err)
	require.NotNil(t, second.Order)

	assert.Equal(t, first.Order.Id, second.Order.Id, "重试必须回显同一张订单，绝不能产生第二笔订单")
	assert.Equal(t, first.ParentOrder.Id, second.ParentOrder.Id)
	assert.Equal(t, first.ParentOrder.OrderNo, second.ParentOrder.OrderNo)

	// Step 4.1.2: 断言全库仅存在一张主订单
	listResp, err := svc.ListParentOrders(ctx, &orderv1.ListParentOrdersRequest{Page: 1, PageSize: 50})
	require.NoError(t, err)
	assert.Equal(t, int64(1), listResp.Total, "同一结算令牌在全库只能留下一张主订单")
}
