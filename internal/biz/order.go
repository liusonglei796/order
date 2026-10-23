package biz

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"

	inventoryv1 "order/gen/inventory/v1"
	orderv1 "order/gen/order/v1"
	productv1 "order/gen/product/v1"
	walletv1 "order/gen/wallet/v1"
)

type OrderUsecase struct {
	repo            OrderRepo
	productClient   productv1.ProductServiceClient
	inventoryClient inventoryv1.InventoryServiceClient
	walletClient    walletv1.WalletServiceClient
	splitter        *OrderSplitter
	fulfiller       Fulfiller
	log             *log.Helper
}

// Step 0.1: 构造 OrderUsecase 领域用例实例
func NewOrderUsecase(
	repo OrderRepo,
	splitter *OrderSplitter,
	fulfiller Fulfiller,
	productClient productv1.ProductServiceClient,
	inventoryClient inventoryv1.InventoryServiceClient,
	walletClient walletv1.WalletServiceClient,
	logger log.Logger,
) *OrderUsecase {
	return &OrderUsecase{
		repo:            repo,
		splitter:        splitter,
		fulfiller:       fulfiller,
		productClient:   productClient,
		inventoryClient: inventoryClient,
		walletClient:    walletClient,
		log:             log.NewHelper(logger),
	}
}

// Step 1.1: 添加商品到购物车
func (uc *OrderUsecase) AddCartItem(ctx context.Context, userID, skuID string, quantity int64) error {
	existingItems, _ := uc.repo.GetCartItems(ctx, userID)
	qty := quantity
	for _, it := range existingItems {
		if it.SkuID == skuID {
			qty += it.Quantity
			break
		}
	}
	return uc.repo.SetCartItem(ctx, userID, &CartItemData{
		SkuID:    skuID,
		Quantity: qty,
		Selected: true,
	})
}

// Step 1.2: 更新购物车条目
func (uc *OrderUsecase) UpdateCartItem(ctx context.Context, userID, skuID string, quantity int64, selected bool) error {
	return uc.repo.SetCartItem(ctx, userID, &CartItemData{
		SkuID:    skuID,
		Quantity: quantity,
		Selected: selected,
	})
}

// Step 1.3: 移除购物车条目
func (uc *OrderUsecase) RemoveCartItem(ctx context.Context, userID, skuID string) error {
	return uc.repo.RemoveCartItem(ctx, userID, skuID)
}

// Step 1.4: 清空购物车
func (uc *OrderUsecase) ClearCart(ctx context.Context, userID string) error {
	return uc.repo.ClearCart(ctx, userID)
}

// Step 1.5: 查询购物车详情与最新价格
func (uc *OrderUsecase) GetCart(ctx context.Context, userID string) (*orderv1.GetCartResponse, error) {
	rawItems, err := uc.repo.GetCartItems(ctx, userID)
	if err != nil {
		return nil, err
	}
	if len(rawItems) == 0 {
		return &orderv1.GetCartResponse{
			Items:       []*orderv1.CartItem{},
			TotalAmount: 0,
			TotalCount:  0,
		}, nil
	}

	skuIDs := make([]string, len(rawItems))
	for i, r := range rawItems {
		skuIDs[i] = r.SkuID
	}

	skuMap := make(map[string]*productv1.SKU)
	if uc.productClient != nil {
		pResp, err := uc.productClient.BatchGetSKUs(ctx, &productv1.BatchGetSKUsRequest{SkuIds: skuIDs})
		if err == nil && pResp != nil {
			for _, sku := range pResp.Skus {
				skuMap[sku.Id] = sku
			}
		}
	}

	cartItems := make([]*orderv1.CartItem, len(rawItems))
	var totalAmount, totalCount int64

	for i, raw := range rawItems {
		sku, found := skuMap[raw.SkuID]
		title := raw.SkuID
		var image, attrsJSON string
		var price int64
		available := false
		tid := "tenant-default"

		if found {
			title = sku.Title
			image = sku.Image
			attrsJSON = sku.AttrsJson
			price = sku.Price
			available = (sku.Status == 1)
			if sku.TenantId != "" {
				tid = sku.TenantId
			}
		}

		subtotal := price * raw.Quantity
		if raw.Selected && available {
			totalAmount += subtotal
			totalCount += raw.Quantity
		}

		cartItems[i] = &orderv1.CartItem{
			SkuId:     raw.SkuID,
			Quantity:  raw.Quantity,
			Selected:  raw.Selected,
			Title:     title,
			Image:     image,
			AttrsJson: attrsJSON,
			Price:     price,
			Subtotal:  subtotal,
			Available: available,
			TenantId:  tid,
			StoreName: fmt.Sprintf("商户店铺 (%s)", tid),
		}
	}

	return &orderv1.GetCartResponse{
		Items:       cartItems,
		TotalAmount: totalAmount,
		TotalCount:  totalCount,
	}, nil
}







// Step 2.1: 创建订单与跨商家拆单
// 来源: 买家在前端确认收货信息与待结算条目后发起 CreateOrder 请求
// 用于: 执行跨店拆单、库存预占、持久化主子订单拓扑并写入订单超时关单延迟队列
func (uc *OrderUsecase) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest, userID string) (*orderv1.CreateOrderResponse, error) {
	// Step 2.1.1: 提取待购 SKU 列表并校验商品有效性
	// 来源: req.GetItems()
	// 用于: 向商品中心批量获取 SKU 定价与状态
	skuIDs := make([]string, len(req.GetItems()))
	for i, it := range req.GetItems() {
		skuIDs[i] = it.SkuId
	}

	skuMap := make(map[string]*productv1.SKU)
	if uc.productClient != nil {
		pResp, err := uc.productClient.BatchGetSKUs(ctx, &productv1.BatchGetSKUsRequest{SkuIds: skuIDs})
		if err != nil || pResp == nil {
			return nil, errors.New("failed to query product service for pricing")
		}
		for _, sku := range pResp.Skus {
			skuMap[sku.Id] = sku
		}
	}

	rawSplitItems := make([]*RawSplitItem, len(req.GetItems()))
	for i, it := range req.GetItems() {
		sku, ok := skuMap[it.SkuId]
		if !ok {
			if uc.productClient == nil {
				sku = &productv1.SKU{
					Id:       it.SkuId,
					TenantId: "tenant-default",
					Title:    it.SkuId,
					Price:    1000,
					Status:   1,
				}
			} else {
				return nil, fmt.Errorf("sku %s is invalid or discontinued", it.SkuId)
			}
		} else if sku.Status != 1 {
			return nil, fmt.Errorf("sku %s is invalid or discontinued", it.SkuId)
		}
		rawSplitItems[i] = &RawSplitItem{
			SkuID:    it.SkuId,
			Quantity: it.Quantity,
			SKU:      sku,
		}
	}

	// Step 2.1.2: 执行跨商家租户拆单与优惠分摊
	// 来源: Step 2.1.1 校验通过的 rawSplitItems
	// 用于: Step 2.1.3 生成各商家的子订单拓扑并分摊平台满减折扣
	groups := uc.splitter.GroupItemsByTenant(rawSplitItems)
	var platformDiscount int64 = 0
	allocatedDiscounts, realizedPrices := uc.splitter.AllocateProportionalDiscounts(groups, platformDiscount)

	// Step 2.1.3: 生成主订单编号并执行多商户库存预占
	// 来源: 系统时间与随机 UUID 前缀
	// 用于: Step 2.1.4 组装主子订单拓扑并预占底层库存
	parentOrderNo := fmt.Sprintf("PORD%s%s", time.Now().Format("20060102150405"), strings.ToUpper(uuid.New().String()[:8]))
	reservations, err := uc.splitter.ReserveStockAllOrNothing(ctx, parentOrderNo, groups)
	if err != nil {
		return nil, err
	}

	// Step 2.1.4: 组装主订单与商家子订单聚合实体
	// 来源: parentOrderNo 与 Step 2.1.2/2.1.3 的分单、折后金额及预占流水
	// 用于: Step 2.1.5 在数据库单一事务中原子持久化落库
	parentOrder, childOrders := uc.splitter.AssembleParentAndChildOrders(
		parentOrderNo,
		userID,
		req.GetReceiverName(),
		req.GetReceiverPhone(),
		req.GetReceiverAddress(),
		groups,
		allocatedDiscounts,
		realizedPrices,
		reservations,
	)

	// Step 2.1.5: 持久化主订单与子订单集合
	// 来源: Step 2.1.4 组装的 parentOrder 与 childOrders
	// 用于: 在单一事务中原子写入 parent_orders, child_orders 及 order_items 表
	createdParent, createdChildren, err := uc.repo.CreateParentOrder(ctx, parentOrder, childOrders)
	if err != nil {
		return nil, err
	}

	// Step 2.1.6: 写入延时关单队列并清理已购购物车条目
	// 来源: createdParent.Id 与 createdParent.ExpireAt
	// 用于: 驱动超时自动关单以及维护买家购物车的即时库存状态
	_ = uc.repo.AddOrderExpiration(ctx, createdParent.Id, createdParent.ExpireAt)
	_ = uc.repo.RemoveCartItems(ctx, userID, skuIDs)

	// Step 2.1.7: 组装并返回提单响应
	// 来源: createdParent 与 createdChildren
	// 用于: 返回 gRPC CreateOrderResponse
	return uc.buildCreateOrderResponse(createdParent, createdChildren), nil
}

// Step 3.2: 组装提单响应 (主单 + 首个子单视图 + 全部子单)
// 来源: 已落库的主订单与子单集合 (新建或幂等回显)
// 用于: 统一新建链路与回显链路的响应结构, 避免两处逻辑漂移
func (uc *OrderUsecase) buildCreateOrderResponse(parent *orderv1.ParentOrder, children []*orderv1.ChildOrder) *orderv1.CreateOrderResponse {
	var primaryOrder *orderv1.Order
	if len(children) > 0 {
		ch := children[0]
		primaryOrder = &orderv1.Order{
			Id:              ch.Id,
			TenantId:        ch.TenantId,
			OrderNo:         ch.OrderNo,
			UserId:          ch.UserId,
			TotalAmount:     ch.TotalAmount,
			PayAmount:       ch.PayAmount,
			Status:          ch.Status,
			ReceiverName:    parent.ReceiverName,
			ReceiverPhone:   parent.ReceiverPhone,
			ReceiverAddress: parent.ReceiverAddress,
			ExpireAt:        parent.ExpireAt,
			CreatedAt:       parent.CreatedAt,
			Items:           ch.Items,
			ParentId:        parent.Id,
		}
	}

	return &orderv1.CreateOrderResponse{
		Order:       primaryOrder,
		ParentOrder: parent,
		ChildOrders: children,
	}
}



// Step 4.1: 支付订单推进
func (uc *OrderUsecase) PayOrder(ctx context.Context, targetID, payChannel string) (*orderv1.PayOrderResponse, error) {
	now := time.Now().Unix()
	if payChannel == "" {
		payChannel = "MOCK_PAYMENT"
	}

	if parent, err := uc.repo.GetParentOrder(ctx, targetID); err == nil {
		if parent.Status == "PAID" {
			return &orderv1.PayOrderResponse{Success: true, Status: "PAID"}, nil
		}
		if parent.Status != "PENDING_PAYMENT" {
			return nil, fmt.Errorf("order status %s cannot be paid", parent.Status)
		}

		var sqlDB *sql.DB
		if dbGetter, ok := uc.repo.(DBGetter); ok && dbGetter != nil {
			sqlDB = dbGetter.GetSQLDB()
		}

		localTx := func(tx *sql.Tx) error {
			ctxWithTx := WithTx(ctx, tx)
			success, err := uc.repo.UpdateParentOrderStatus(ctxWithTx, parent.Id, "PENDING_PAYMENT", "PAID", map[string]interface{}{
				"paid_at": time.Unix(now, 0),
			})
			if err != nil || !success {
				return errors.New("update parent status failed")
			}
			return nil
		}

		if uc.fulfiller != nil {
			if err := uc.fulfiller.ConfirmPayment(ctx, FromParent(parent), payChannel, sqlDB, localTx); err != nil {
				return nil, err
			}
		} else {
			if err := localTx(nil); err != nil {
				return nil, err
			}
		}

		_ = uc.repo.RemoveOrderExpiration(ctx, parent.Id)
		return &orderv1.PayOrderResponse{Success: true, Status: "PAID"}, nil
	}

	order, err := uc.repo.GetOrder(ctx, targetID)
	if err != nil {
		return nil, ErrOrderNotFound
	}
	if order.Status == "PAID" {
		return &orderv1.PayOrderResponse{Success: true, Status: "PAID"}, nil
	}
	if order.Status != "PENDING_PAYMENT" {
		return nil, fmt.Errorf("order status %s cannot be paid", order.Status)
	}

	var sqlDB *sql.DB
	if dbGetter, ok := uc.repo.(DBGetter); ok && dbGetter != nil {
		sqlDB = dbGetter.GetSQLDB()
	}

	localTx := func(tx *sql.Tx) error {
		ctxWithTx := WithTx(ctx, tx)
		success, err := uc.repo.UpdateOrderStatus(ctxWithTx, order.Id, "PENDING_PAYMENT", "PAID", map[string]interface{}{
			"paid_at": time.Unix(now, 0),
		})
		if err != nil || !success {
			return errors.New("update order status failed")
		}
		return nil
	}

	if uc.fulfiller != nil {
		if err := uc.fulfiller.ConfirmPayment(ctx, FromSingle(order), payChannel, sqlDB, localTx); err != nil {
			return nil, err
		}
	} else {
		if err := localTx(nil); err != nil {
			return nil, err
		}
	}

	_ = uc.repo.RemoveOrderExpiration(ctx, order.Id)
	return &orderv1.PayOrderResponse{Success: true, Status: "PAID"}, nil
}

// Step 4.2: DTM 二阶段消息 QueryPrepared 反查本地事务是否已提交
// 来源: Step 4.1 PayOrder 本地事务异常时 DTM 协调器通过 gRPC 元数据 (dtm-gid) 发起的反查请求
// 用于: 通过子事务屏障确认本地订单状态是否已原子提交，未提交则返回 ErrFailure 触发 DTM 回滚
func (uc *OrderUsecase) QueryPrepared(ctx context.Context) error {
	dbGetter, ok := uc.repo.(DBGetter)
	if !ok || dbGetter == nil || dbGetter.GetSQLDB() == nil {
		return fmt.Errorf("order repository does not expose sql.DB for DTM barrier")
	}

	barrier, err := dtmgrpc.BarrierFromGrpc(ctx)
	if err != nil {
		return err
	}
	return barrier.QueryPrepared(dbGetter.GetSQLDB())
}

// Step 5.1: 取消订单并释放库存
func (uc *OrderUsecase) CancelOrder(ctx context.Context, targetID, reason string) error {
	if reason == "" {
		reason = "buyer cancelled"
	}
	extraUpdates := map[string]interface{}{"cancel_reason": reason}

	if parent, err := uc.repo.GetParentOrder(ctx, targetID); err == nil {
		if parent.Status != "PENDING_PAYMENT" {
			return fmt.Errorf("cannot cancel order with status %s", parent.Status)
		}
		_, err := uc.repo.UpdateParentOrderStatus(ctx, parent.Id, "PENDING_PAYMENT", "CANCELLED", extraUpdates)
		if err != nil {
			return err
		}
		if uc.fulfiller != nil {
			_ = uc.fulfiller.ReleaseHold(ctx, FromParent(parent), reason)
		}
		_ = uc.repo.RemoveOrderExpiration(ctx, parent.Id)
		return nil
	}

	order, err := uc.repo.GetOrder(ctx, targetID)
	if err != nil {
		return ErrOrderNotFound
	}
	if order.Status != "PENDING_PAYMENT" {
		return fmt.Errorf("cannot cancel order with status %s", order.Status)
	}
	_, err = uc.repo.UpdateOrderStatus(ctx, order.Id, "PENDING_PAYMENT", "CANCELLED", extraUpdates)
	if err != nil {
		return err
	}
	if uc.fulfiller != nil {
		_ = uc.fulfiller.ReleaseHold(ctx, FromSingle(order), reason)
	}
	_ = uc.repo.RemoveOrderExpiration(ctx, order.Id)
	return nil
}

// Step 6.1: 商家发货
func (uc *OrderUsecase) AdminShipOrder(ctx context.Context, targetID, carrier, trackingNo string) error {
	now := time.Now()
	if child, err := uc.repo.GetChildOrder(ctx, targetID); err == nil {
		if child.Status != "PAID" {
			return fmt.Errorf("only PAID orders can be shipped (current: %s)", child.Status)
		}
		_, err := uc.repo.UpdateChildOrderStatus(ctx, child.Id, "PAID", "SHIPPED", map[string]interface{}{
			"carrier":     carrier,
			"tracking_no": trackingNo,
			"shipped_at":  now,
		})
		return err
	}

	order, err := uc.repo.GetOrder(ctx, targetID)
	if err != nil {
		return ErrOrderNotFound
	}
	if order.Status != "PAID" {
		return fmt.Errorf("only PAID orders can be shipped (current: %s)", order.Status)
	}
	_, err = uc.repo.UpdateOrderStatus(ctx, order.Id, "PAID", "SHIPPED", map[string]interface{}{
		"shipped_at": now,
	})
	return err
}

// Step 7.1: 确认收货并触发分账清算
func (uc *OrderUsecase) ConfirmReceipt(ctx context.Context, targetID string) (*orderv1.ConfirmReceiptResponse, error) {
	now := time.Now()
	if child, err := uc.repo.GetChildOrder(ctx, targetID); err == nil {
		if child.Status != "SHIPPED" {
			return nil, fmt.Errorf("only SHIPPED orders can be confirmed (current: %s)", child.Status)
		}
		_, err := uc.repo.UpdateChildOrderStatus(ctx, child.Id, "SHIPPED", "COMPLETED", map[string]interface{}{
			"completed_at": now,
		})
		if err != nil {
			return nil, err
		}

		var settled, comm int64
		if uc.fulfiller != nil {
			res, _ := uc.fulfiller.SettleEscrow(ctx, OrderTarget{
				Parent: &orderv1.ParentOrder{Id: child.ParentId, OrderNo: child.OrderNo, PayAmount: child.PayAmount},
				Single: &orderv1.Order{Id: child.Id, OrderNo: child.OrderNo, TenantId: child.TenantId, PayAmount: child.PayAmount},
			})
			if res != nil {
				settled = res.SettledAmount
				comm = res.CommissionAmount
			}
		}

		return &orderv1.ConfirmReceiptResponse{
			Success:          true,
			OrderId:          child.Id,
			Status:           "COMPLETED",
			SettledAmount:    settled,
			CommissionAmount: comm,
		}, nil
	}

	order, err := uc.repo.GetOrder(ctx, targetID)
	if err != nil {
		return nil, ErrOrderNotFound
	}
	if order.Status != "SHIPPED" {
		return nil, fmt.Errorf("only SHIPPED orders can be confirmed (current: %s)", order.Status)
	}
	_, err = uc.repo.UpdateOrderStatus(ctx, order.Id, "SHIPPED", "COMPLETED", map[string]interface{}{
		"completed_at": now,
	})
	if err != nil {
		return nil, err
	}

	var settled, comm int64
	if uc.fulfiller != nil {
		res, _ := uc.fulfiller.SettleEscrow(ctx, FromSingle(order))
		if res != nil {
			settled = res.SettledAmount
			comm = res.CommissionAmount
		}
	}

	return &orderv1.ConfirmReceiptResponse{
		Success:          true,
		OrderId:          order.Id,
		Status:           "COMPLETED",
		SettledAmount:    settled,
		CommissionAmount: comm,
	}, nil
}

// Step 8.1: 订单查询操作
func (uc *OrderUsecase) GetOrder(ctx context.Context, id string) (*orderv1.Order, error) {
	return uc.repo.GetOrder(ctx, id)
}

func (uc *OrderUsecase) ListOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.Order, int64, error) {
	return uc.repo.ListOrders(ctx, userID, status, page, pageSize)
}

func (uc *OrderUsecase) GetParentOrder(ctx context.Context, id string) (*orderv1.ParentOrder, error) {
	return uc.repo.GetParentOrder(ctx, id)
}

func (uc *OrderUsecase) GetChildOrder(ctx context.Context, id string) (*orderv1.ChildOrder, error) {
	return uc.repo.GetChildOrder(ctx, id)
}

func (uc *OrderUsecase) ListParentOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.ParentOrder, int64, error) {
	return uc.repo.ListParentOrders(ctx, userID, status, page, pageSize)
}

func (uc *OrderUsecase) ListChildOrders(ctx context.Context, tenantID, status string, page, pageSize int) ([]*orderv1.ChildOrder, int64, error) {
	return uc.repo.ListChildOrders(ctx, tenantID, status, page, pageSize)
}

// Step 9.1: 申请售后
// 来源: 买家针对子订单发起的售后请求 orderv1.ApplyAfterSalesRequest
// 用于: 校验订单状态、计算退款金额并创建 APPLIED 状态的售后单
func (uc *OrderUsecase) ApplyAfterSales(ctx context.Context, req *orderv1.ApplyAfterSalesRequest, userID string) (*orderv1.AfterSalesOrder, error) {
	childOrderID := strings.TrimSpace(req.GetChildOrderId())
	if childOrderID == "" {
		return nil, errors.New("child_order_id cannot be empty")
	}

	afterSalesType := strings.ToUpper(strings.TrimSpace(req.GetType()))
	if afterSalesType != "REFUND_ONLY" && afterSalesType != "RETURN_AND_REFUND" {
		return nil, errors.New("type must be REFUND_ONLY or RETURN_AND_REFUND")
	}

	var childOrder *orderv1.ChildOrder
	var parentOrderID, tenantID, childOrderNo string
	var orderItems []*orderv1.OrderItem
	var currentStatus string

	if child, err := uc.repo.GetChildOrder(ctx, childOrderID); err == nil {
		childOrder = child
		parentOrderID = child.ParentId
		tenantID = child.TenantId
		childOrderNo = child.OrderNo
		orderItems = child.Items
		currentStatus = child.Status
	} else if legOrder, err := uc.repo.GetOrder(ctx, childOrderID); err == nil {
		tenantID = legOrder.TenantId
		childOrderNo = legOrder.OrderNo
		orderItems = legOrder.Items
		currentStatus = legOrder.Status
	} else {
		return nil, ErrOrderNotFound
	}

	if currentStatus != "PAID" && currentStatus != "SHIPPED" && currentStatus != "COMPLETED" {
		return nil, fmt.Errorf("order status %s is not eligible for after-sales", currentStatus)
	}
	if currentStatus == "PAID" && afterSalesType == "RETURN_AND_REFUND" {
		return nil, errors.New("unshipped orders can only apply for REFUND_ONLY")
	}

	var totalRefundAmount int64
	var asItems []*orderv1.AfterSalesItem

	if len(req.GetItems()) > 0 {
		itemMap := make(map[string]*orderv1.OrderItem)
		for _, it := range orderItems {
			itemMap[it.SkuId] = it
		}

		for _, p := range req.GetItems() {
			oi, exists := itemMap[p.SkuId]
			if !exists {
				return nil, fmt.Errorf("sku %s does not belong to this order", p.SkuId)
			}
			if p.Quantity <= 0 || p.Quantity > oi.Quantity {
				return nil, fmt.Errorf("invalid refund quantity %d for sku %s (purchased %d)", p.Quantity, p.SkuId, oi.Quantity)
			}
			realizedPrice := oi.RealizedPrice
			if realizedPrice <= 0 {
				realizedPrice = oi.Price
			}
			itemRefund := realizedPrice * p.Quantity
			totalRefundAmount += itemRefund

			asItems = append(asItems, &orderv1.AfterSalesItem{
				SkuId:         p.SkuId,
				Quantity:      p.Quantity,
				RealizedPrice: realizedPrice,
				RefundAmount:  itemRefund,
			})
		}
	} else {
		if childOrder != nil {
			totalRefundAmount = childOrder.PayAmount
		} else {
			legOrder, err := uc.repo.GetOrder(ctx, childOrderID)
			if err != nil {
				return nil, ErrOrderNotFound
			}
			totalRefundAmount = legOrder.PayAmount
		}
		for _, oi := range orderItems {
			realizedPrice := oi.RealizedPrice
			if realizedPrice <= 0 {
				realizedPrice = oi.Price
			}
			asItems = append(asItems, &orderv1.AfterSalesItem{
				SkuId:         oi.SkuId,
				Quantity:      oi.Quantity,
				RealizedPrice: realizedPrice,
				RefundAmount:  realizedPrice * oi.Quantity,
			})
		}
	}

	now := time.Now().Unix()
	asID := "as-" + uuid.NewString()
	asNo := fmt.Sprintf("AS-%s-%d", childOrderNo, now%1000000)

	asOrder := &orderv1.AfterSalesOrder{
		Id:            asID,
		AfterSalesNo:  asNo,
		ChildOrderId:  childOrderID,
		ChildOrderNo:  childOrderNo,
		ParentOrderId: parentOrderID,
		TenantId:      tenantID,
		UserId:        userID,
		Type:          afterSalesType,
		Status:        "APPLIED",
		RefundAmount:  totalRefundAmount,
		Reason:        req.GetReason(),
		CreatedAt:     now,
		UpdatedAt:     now,
		Items:         asItems,
	}

	return uc.repo.CreateAfterSales(ctx, asOrder)
}

// Step 9.2: 商家审核售后申请
// 来源: 商户审核传入的售后 ID、是否通过及拒绝原因
// 用于: 流转售后单状态，若为仅退款通过则触发冲正退款
func (uc *OrderUsecase) AuditAfterSales(ctx context.Context, id string, approved bool, rejectReason string) (*orderv1.AuditAfterSalesResponse, error) {
	as, err := uc.repo.GetAfterSales(ctx, id)
	if err != nil {
		return nil, ErrAfterSalesNotFound
	}
	if as.Status != "APPLIED" {
		return nil, fmt.Errorf("cannot audit after-sales in status %s", as.Status)
	}

	if !approved {
		success, err := uc.repo.UpdateAfterSalesStatus(ctx, as.Id, "APPLIED", "MERCHANT_REJECTED", map[string]interface{}{
			"reject_reason": rejectReason,
		})
		if err != nil || !success {
			return nil, errors.New("failed to reject after-sales order")
		}
		return &orderv1.AuditAfterSalesResponse{
			Success: true,
			Status:  "MERCHANT_REJECTED",
		}, nil
	}

	if as.Type == "REFUND_ONLY" {
		success, err := uc.repo.UpdateAfterSalesStatus(ctx, as.Id, "APPLIED", "REFUNDED", nil)
		if err != nil || !success {
			return nil, errors.New("failed to finalize refund-only after-sales")
		}

		isSettled := false
		var prevStatus string
		if ch, err := uc.repo.GetChildOrder(ctx, as.ChildOrderId); err == nil {
			prevStatus = ch.Status
			isSettled = (ch.Status == "COMPLETED")
			_, _ = uc.repo.UpdateChildOrderStatus(ctx, ch.Id, ch.Status, "REFUNDED", nil)
		} else if ord, err := uc.repo.GetOrder(ctx, as.ChildOrderId); err == nil {
			prevStatus = ord.Status
			isSettled = (ord.Status == "COMPLETED")
			_, _ = uc.repo.UpdateOrderStatus(ctx, ord.Id, ord.Status, "REFUNDED", nil)
		}

		if uc.fulfiller != nil {
			var releaseDeltas []*inventoryv1.StockItemDelta
			if prevStatus == "PAID" && len(as.Items) > 0 {
				for _, it := range as.Items {
					releaseDeltas = append(releaseDeltas, &inventoryv1.StockItemDelta{
						SkuId:    it.SkuId,
						Quantity: it.Quantity,
					})
				}
			}
			_ = uc.fulfiller.RefundEscrow(ctx, RefundTarget{
				ChildOrderID: as.ChildOrderId,
				ChildOrderNo: as.ChildOrderNo,
				RefundID:     as.Id,
				TenantID:     as.TenantId,
				Amount:       as.RefundAmount,
				Reason:       as.Reason,
				IsSettled:    isSettled,
				ReleaseItems: releaseDeltas,
			})
		}

		return &orderv1.AuditAfterSalesResponse{
			Success:      true,
			Status:       "REFUNDED",
			RefundAmount: as.RefundAmount,
		}, nil
	}

	success, err := uc.repo.UpdateAfterSalesStatus(ctx, as.Id, "APPLIED", "MERCHANT_APPROVED", nil)
	if err != nil || !success {
		return nil, errors.New("failed to approve after-sales order")
	}

	return &orderv1.AuditAfterSalesResponse{
		Success: true,
		Status:  "MERCHANT_APPROVED",
	}, nil
}

// Step 9.3: 买家退货录入物流单号
// 来源: 买家寄回退货商品后录入物流承运商与运单号
// 用于: 将售后单推进至 BUYER_RETURNED 状态
func (uc *OrderUsecase) BuyerReturnGoods(ctx context.Context, id, carrier, trackingNo string) (*orderv1.BuyerReturnGoodsResponse, error) {
	as, err := uc.repo.GetAfterSales(ctx, id)
	if err != nil {
		return nil, ErrAfterSalesNotFound
	}
	if as.Status != "MERCHANT_APPROVED" {
		return nil, fmt.Errorf("cannot return goods in status %s", as.Status)
	}

	success, err := uc.repo.UpdateAfterSalesStatus(ctx, as.Id, "MERCHANT_APPROVED", "BUYER_RETURNED", map[string]interface{}{
		"return_carrier":     carrier,
		"return_tracking_no": trackingNo,
	})
	if err != nil || !success {
		return nil, errors.New("failed to record return logistics")
	}

	return &orderv1.BuyerReturnGoodsResponse{
		Success: true,
		Status:  "BUYER_RETURNED",
	}, nil
}

// Step 9.4: 商家确认收到退货并触发退款清算
// 来源: 商家验货通过确认收到退货
// 用于: 将售后单转为 REFUNDED 并调用 Fulfiller 执行复式冲正与库存释放
func (uc *OrderUsecase) MerchantConfirmReturn(ctx context.Context, id string) (*orderv1.MerchantConfirmReturnResponse, error) {
	as, err := uc.repo.GetAfterSales(ctx, id)
	if err != nil {
		return nil, ErrAfterSalesNotFound
	}
	if as.Status != "BUYER_RETURNED" {
		return nil, fmt.Errorf("cannot confirm return in status %s", as.Status)
	}

	success, err := uc.repo.UpdateAfterSalesStatus(ctx, as.Id, "BUYER_RETURNED", "REFUNDED", nil)
	if err != nil || !success {
		return nil, errors.New("failed to complete after-sales return")
	}

	isSettled := false
	if ch, err := uc.repo.GetChildOrder(ctx, as.ChildOrderId); err == nil {
		isSettled = (ch.Status == "COMPLETED")
		_, _ = uc.repo.UpdateChildOrderStatus(ctx, ch.Id, ch.Status, "REFUNDED", nil)
	} else if ord, err := uc.repo.GetOrder(ctx, as.ChildOrderId); err == nil {
		isSettled = (ord.Status == "COMPLETED")
		_, _ = uc.repo.UpdateOrderStatus(ctx, ord.Id, ord.Status, "REFUNDED", nil)
	}

	if uc.fulfiller != nil {
		var releaseDeltas []*inventoryv1.StockItemDelta
		for _, it := range as.Items {
			releaseDeltas = append(releaseDeltas, &inventoryv1.StockItemDelta{
				SkuId:    it.SkuId,
				Quantity: it.Quantity,
			})
		}
		_ = uc.fulfiller.RefundEscrow(ctx, RefundTarget{
			ChildOrderID: as.ChildOrderId,
			ChildOrderNo: as.ChildOrderNo,
			RefundID:     as.Id,
			TenantID:     as.TenantId,
			Amount:       as.RefundAmount,
			Reason:       as.Reason,
			IsSettled:    isSettled,
			ReleaseItems: releaseDeltas,
		})
	}

	return &orderv1.MerchantConfirmReturnResponse{
		Success:      true,
		Status:       "REFUNDED",
		RefundAmount: as.RefundAmount,
	}, nil
}

// Step 9.5: 买家撤销售后申请
// 来源: 买家主动撤销售后单
// 用于: 将 APPLIED 状态的售后单流转为 CANCELLED
func (uc *OrderUsecase) CancelAfterSales(ctx context.Context, id string) error {
	as, err := uc.repo.GetAfterSales(ctx, id)
	if err != nil {
		return ErrAfterSalesNotFound
	}
	if as.Status != "APPLIED" {
		return fmt.Errorf("cannot cancel after-sales in status %s", as.Status)
	}
	success, err := uc.repo.UpdateAfterSalesStatus(ctx, as.Id, "APPLIED", "CANCELLED", nil)
	if err != nil || !success {
		return errors.New("failed to cancel after-sales")
	}
	return nil
}

// Step 9.6: 查询单个售后单详情
// 来源: 售后单 ID
// 用于: 返回售后单实体
func (uc *OrderUsecase) GetAfterSales(ctx context.Context, id string) (*orderv1.AfterSalesOrder, error) {
	return uc.repo.GetAfterSales(ctx, id)
}

// Step 9.7: 买家查询自身售后单列表
// 来源: userID、status 及分页参数
// 用于: 返回买家售后单分页列表
func (uc *OrderUsecase) ListAfterSales(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.AfterSalesOrder, int64, error) {
	return uc.repo.ListAfterSales(ctx, userID, status, page, pageSize)
}

// Step 9.8: 商家查询租户售后单列表
// 来源: tenantID、status 及分页参数
// 用于: 返回商家端售后单分页列表
func (uc *OrderUsecase) AdminListAfterSales(ctx context.Context, tenantID, status string, page, pageSize int) ([]*orderv1.AfterSalesOrder, int64, error) {
	return uc.repo.AdminListAfterSales(ctx, tenantID, status, page, pageSize)
}
