package biz

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"

	inventoryv1 "order/gen/inventory/v1"
	orderv1 "order/gen/order/v1"
	productv1 "order/gen/product/v1"
	walletv1 "order/gen/wallet/v1"
	"order/internal/pkg/tenant"
)

type OrderUsecase struct {
	repo            OrderRepo
	productClient   productv1.ProductServiceClient
	inventoryClient inventoryv1.InventoryServiceClient
	walletClient    walletv1.WalletServiceClient
	splitter        *OrderSplitter
	fulfiller       Fulfiller
	leaseTimeout    time.Duration
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
		leaseTimeout:    5 * time.Second,
		log:             log.NewHelper(logger),
	}
}

// Step 0.2: 动态配置提单租约锁超时窗口
// 来源: 单元测试或压测混沌注入
// 用于: 支持自愈租约时间窗口自定义，默认 5s
func (uc *OrderUsecase) SetLeaseTimeout(timeout time.Duration) {
	uc.leaseTimeout = timeout
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

// Step 2.0: 签发具备幂等语义的结算令牌 (内嵌签发毫秒时间戳)
// 来源: 买家点击"去结算"进入结算页
// 用于: 生成全局唯一且可反解签发时刻的幂等令牌, 支撑主订单号的跨节点确定性推导
func GenerateSettlementToken() string {
	return fmt.Sprintf("st-%d-%s", time.Now().UnixMilli(), uuid.NewString())
}

// Step 2.0.1: 解析结算令牌内嵌的签发毫秒时间戳
// 来源: GenerateSettlementToken 生成的 st-{unixMilli}-{uuid} 格式
// 用于: 主订单号保留可读的时间戳前缀, 同时保持跨节点、跨重试的确定性
func settlementTokenTimestamp(token string) (int64, bool) {
	const prefix = "st-"
	if !strings.HasPrefix(token, prefix) {
		return 0, false
	}
	rest := token[len(prefix):]
	idx := strings.Index(rest, "-")
	if idx <= 0 {
		return 0, false
	}
	ms, err := strconv.ParseInt(rest[:idx], 10, 64)
	if err != nil {
		return 0, false
	}
	return ms, true
}

// Step 2.1: 准备结算并签发结算令牌与跨店拆单预览
func (uc *OrderUsecase) PrepareSettlement(ctx context.Context, userID string, skuIDs []string) (*orderv1.PrepareSettlementResponse, error) {
	rawItems, err := uc.repo.GetCartItems(ctx, userID)
	if err != nil {
		return nil, err
	}

	selectedSKUs := make(map[string]bool)
	for _, id := range skuIDs {
		selectedSKUs[id] = true
	}

	var targetItems []*CartItemData
	for _, it := range rawItems {
		if len(skuIDs) > 0 {
			if selectedSKUs[it.SkuID] {
				targetItems = append(targetItems, it)
			}
		} else if it.Selected {
			targetItems = append(targetItems, it)
		}
	}

	if len(targetItems) == 0 {
		return nil, errors.New("no items selected for settlement")
	}

	fetchSkuIDs := make([]string, len(targetItems))
	for i, it := range targetItems {
		fetchSkuIDs[i] = it.SkuID
	}

	skuMap := make(map[string]*productv1.SKU)
	if uc.productClient != nil {
		pResp, err := uc.productClient.BatchGetSKUs(ctx, &productv1.BatchGetSKUsRequest{SkuIds: fetchSkuIDs})
		if err == nil && pResp != nil {
			for _, sku := range pResp.Skus {
				skuMap[sku.Id] = sku
			}
		}
	}

	cartItems := make([]*orderv1.CartItem, len(targetItems))
	rawSplitItems := make([]*RawSplitItem, len(targetItems))
	var totalAmount int64

	for i, raw := range targetItems {
		sku, ok := skuMap[raw.SkuID]
		title := raw.SkuID
		var image, attrsJSON string
		var price int64
		tid := "tenant-default"
		available := false

		if ok {
			title = sku.Title
			image = sku.Image
			attrsJSON = sku.AttrsJson
			price = sku.Price
			if sku.TenantId != "" {
				tid = sku.TenantId
			}
			available = (sku.Status == 1)
		} else {
			sku = &productv1.SKU{
				Id:       raw.SkuID,
				TenantId: tid,
				Title:    title,
				Price:    0,
			}
		}

		subtotal := price * raw.Quantity
		totalAmount += subtotal

		cartItems[i] = &orderv1.CartItem{
			SkuId:     raw.SkuID,
			Quantity:  raw.Quantity,
			Selected:  true,
			Title:     title,
			Image:     image,
			AttrsJson: attrsJSON,
			Price:     price,
			Subtotal:  subtotal,
			Available: available,
			TenantId:  tid,
			StoreName: fmt.Sprintf("商户店铺 (%s)", tid),
		}

		rawSplitItems[i] = &RawSplitItem{
			SkuID:    raw.SkuID,
			Quantity: raw.Quantity,
			SKU:      sku,
		}
	}

	groups := uc.splitter.GroupItemsByTenant(rawSplitItems)
	var totalDiscount int64 = 0
	allocatedDiscounts, _ := uc.splitter.AllocateProportionalDiscounts(groups, totalDiscount)

	storeGroups := make([]*orderv1.StoreSettlementGroup, len(groups))
	for i, g := range groups {
		var storeItems []*orderv1.CartItem
		var storeTotal int64
		var storeDiscount int64

		for _, it := range g.Items {
			subtotal := it.SKU.Price * it.Quantity
			storeTotal += subtotal
			storeDiscount += allocatedDiscounts[it.SkuID]

			for _, ci := range cartItems {
				if ci.SkuId == it.SkuID {
					storeItems = append(storeItems, ci)
					break
				}
			}
		}

		storeGroups[i] = &orderv1.StoreSettlementGroup{
			TenantId:       g.TenantID,
			StoreName:      fmt.Sprintf("商户店铺 (%s)", g.TenantID),
			Items:          storeItems,
			TotalAmount:    storeTotal,
			DiscountAmount: storeDiscount,
			PayAmount:      storeTotal - storeDiscount,
		}
	}

	token := GenerateSettlementToken()
	_ = uc.repo.SaveSettlementToken(ctx, userID, token, SettlementTokenInitTTL)

	return &orderv1.PrepareSettlementResponse{
		SettlementToken: token,
		Items:           cartItems,
		TotalAmount:     totalAmount,
		StoreGroups:     storeGroups,
		TotalDiscount:   totalDiscount,
		PayAmount:       totalAmount - totalDiscount,
	}, nil
}

// Step 3.0: 由结算令牌确定性推导主订单编号
// 来源: Step 2.0 签发的 settlement_token 与当前租户 ID
// 用于: 同一令牌在任何节点、任何次重试中推导出的订单号完全一致,
//
//	使 DTM Saga 全局事务 ID、库存幂等键与数据库唯一索引在租约自愈重入时仍命中同一条业务记录
func DeriveParentOrderNo(tenantID, token string) string {
	sum := sha256.Sum256([]byte(tenantID + "|" + token))
	suffix := hex.EncodeToString(sum[:])[:6]
	if ms, ok := settlementTokenTimestamp(token); ok {
		return fmt.Sprintf("PORD%s%s", time.UnixMilli(ms).Format("20060102150405"), suffix)
	}
	return fmt.Sprintf("PORD%s", suffix)
}

// Step 3.1: 创建订单与跨商家拆单
func (uc *OrderUsecase) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest, userID string) (*orderv1.CreateOrderResponse, error) {
	token := strings.TrimSpace(req.GetSettlementToken())
	if token == "" {
		return nil, errors.New("settlement_token cannot be empty")
	}

	leaseTimeout := uc.leaseTimeout
	if leaseTimeout <= 0 {
		leaseTimeout = 5 * time.Second
	}
	tokenRes, err := uc.repo.AcquireSettlementToken(ctx, userID, token, leaseTimeout)
	if err != nil {
		return nil, err
	}

	switch tokenRes.Action {
	case "SUCCESS_REPLAY":
		// 命中已成功终态: 优先直接读取 Redis 中固化的订单快照纯内存回显, 绝不二次落库、绝不穿透 MySQL
		if parent := unmarshalOrderSnapshot(tokenRes.Snapshot); parent != nil {
			return uc.buildCreateOrderResponse(parent, parent.ChildOrders), nil
		}
		// 快照缺失 (如灰度期旧令牌或快照写入失败) 时降级为主键回查, 仍绝不重新建单
		parent, gErr := uc.repo.GetParentOrder(ctx, tokenRes.ParentOrderID)
		if gErr != nil || parent == nil {
			// 回显链路断裂时必须显式失败, 绝不能回落去重新建单, 否则将制造重复订单
			return nil, fmt.Errorf("cannot replay settlement result for parent order %s: %v", tokenRes.ParentOrderID, gErr)
		}
		return uc.buildCreateOrderResponse(parent, parent.ChildOrders), nil
	case "PROCEED":
		// 抢占成功, 持有租约, 继续向下执行建单链路
	default:
		return nil, fmt.Errorf("cannot create order: %s", tokenRes.Action)
	}

	// Step 3.1.1: 启动租约续期心跳 (CAS 续租)
	// 来源: Step 3.1 AcquireSettlementToken 抢占成功后返回的 lockTime
	// 用于: DTM Saga 跨店库存预占等长耗时链路持续持有租约, 杜绝租约中途过期被他人接管
	stopRenew := uc.startLeaseRenewal(ctx, token, tokenRes.LockTime)
	defer stopRenew()

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

	groups := uc.splitter.GroupItemsByTenant(rawSplitItems)
	var platformDiscount int64 = 0
	allocatedDiscounts, realizedPrices := uc.splitter.AllocateProportionalDiscounts(groups, platformDiscount)

	// Step 3.1.2: 由结算令牌确定性推导主订单编号 (替代随机编号)
	// 来源: Step 3.0 DeriveParentOrderNo(租户, 结算令牌)
	// 用于: DTM Saga 全局事务 ID 与库存幂等键随主订单号一并确定化, 杜绝自愈重入重复预占库存
	parentOrderNo := DeriveParentOrderNo(uc.resolveTenantID(ctx), token)
	reservations, err := uc.splitter.ReserveStockAllOrNothing(ctx, parentOrderNo, groups)
	if err != nil {
		_ = uc.repo.RecordSettlementFail(ctx, token, true, err.Error())
		return nil, err
	}

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

	createdParent, createdChildren, err := uc.repo.CreateParentOrder(ctx, parentOrder, childOrders, token)
	if err != nil {
		// Step 3.1.3: 结算令牌唯一索引冲突兜底
		// 来源: MySQL UNIQUE (tenant_id, settlement_token) 拦截的重复建单
		// 用于: 复用确定性订单号回显既有订单, 绝不生成第二笔订单
		if errors.Is(err, ErrDuplicateSettlementToken) {
			existing, gErr := uc.repo.GetParentOrderByNo(ctx, parentOrderNo)
			if gErr == nil && existing != nil {
				_ = uc.repo.RecordSettlementSuccess(ctx, token, existing.Id, existing.OrderNo, marshalOrderSnapshot(existing), SettlementTokenSuccessTTL)
				return uc.buildCreateOrderResponse(existing, existing.ChildOrders), nil
			}
			return nil, err
		}
		_ = uc.repo.RecordSettlementFail(ctx, token, false, err.Error())
		return nil, err
	}

	_ = uc.repo.AddOrderExpiration(ctx, createdParent.Id, createdParent.ExpireAt)
	_ = uc.repo.RemoveCartItems(ctx, userID, skuIDs)
	// 固化订单快照到 Redis Hash, 使后续弱网重试可以纯内存秒级回显
	_ = uc.repo.RecordSettlementSuccess(ctx, token, createdParent.Id, createdParent.OrderNo, marshalOrderSnapshot(createdParent), SettlementTokenSuccessTTL)

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

// Step 3.2.1: 序列化主订单快照 (含子单与明细) 供纯内存回显
// 来源: 已落库的主订单聚合 (主单 + 子单 + 明细)
// 用于: 弱网超时重试命中 SUCCESS 终态时直接提取快照秒级回显, 完全不穿透 MySQL
func marshalOrderSnapshot(parent *orderv1.ParentOrder) string {
	if parent == nil {
		return ""
	}
	raw, err := json.Marshal(parent)
	if err != nil {
		return ""
	}
	return string(raw)
}

// Step 3.2.2: 反序列化 Redis 中固化的主订单快照
// 来源: AcquireSettlementToken 在 SUCCESS_REPLAY 分支回传的 order_snapshot 字段
// 用于: 纯内存重建提单响应; 快照缺失或损坏时返回 nil, 由调用方降级为主键回查
func unmarshalOrderSnapshot(snapshot string) *orderv1.ParentOrder {
	if snapshot == "" {
		return nil
	}
	var parent orderv1.ParentOrder
	if err := json.Unmarshal([]byte(snapshot), &parent); err != nil {
		return nil
	}
	return &parent
}

// Step 3.3: 启动租约续期心跳 (CAS 续租)
// 来源: Step 3.1 AcquireSettlementToken 抢占成功后返回的 lockTime
// 用于: 长耗时建单链路持续持有租约, 杜绝租约中途过期被他人接管后重复建单与重复预占库存
// 返回值: 停止续租的取消函数 (由调用方 defer 触发)
func (uc *OrderUsecase) startLeaseRenewal(ctx context.Context, token string, lockTime int64) func() {
	interval := uc.leaseTimeout / 3
	if interval <= 0 {
		interval = time.Second
	}
	renewCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		current := lockTime
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				newLock, ok, rErr := uc.repo.RenewSettlementToken(renewCtx, token, current, SettlementTokenKeyTTL)
				if rErr != nil {
					// Redis 异常无法续租: 停止心跳, 由数据库唯一索引兜底阻断重复建单
					uc.log.WithContext(renewCtx).Warnf("settlement token lease renew failed: %v", rErr)
					return
				}
				if !ok {
					// lock_time 已被他人接管改写, 本节点已失去租约, 立即停止续租
					uc.log.WithContext(renewCtx).Warnf("settlement token lease lost, token=%s", token)
					return
				}
				current = newLock
			}
		}
	}()
	return cancel
}

// Step 3.4: 解析当前请求的租户标识 (与仓储层 resolveTenant 保持一致的兜底语义)
// 来源: 租户拦截器注入 ctx 的 x-tenant-id
// 用于: 订单号确定性推导需与 parent_orders.tenant_id 取值口径完全一致
func (uc *OrderUsecase) resolveTenantID(ctx context.Context) string {
	tid := tenant.TenantID(ctx)
	if tid == "" {
		return "tenant-default"
	}
	return tid
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
