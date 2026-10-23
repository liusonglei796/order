package data

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"order/internal/biz"
	orderv1 "order/gen/order/v1"
	"order/internal/pkg/paging"
	"order/internal/pkg/tenant"
)

var (
	ErrOrderNotFound      = biz.ErrOrderNotFound
	ErrInvalidOrderStatus = biz.ErrInvalidOrderStatus
	ErrAfterSalesNotFound = biz.ErrAfterSalesNotFound
)

func toUnixSeconds(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case nil:
		return 0, false
	case time.Time:
		return t.Unix(), true
	case *time.Time:
		if t == nil {
			return 0, false
		}
		return t.Unix(), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	default:
		return 0, false
	}
}

// MemoryRepository provides a concurrent in-memory store for orders and cart.
type MemoryRepository struct {
	mu           sync.RWMutex
	carts        map[string]map[string]*CartItemData // key: "tenant_id:user_id" -> map[sku_id]*CartItemData
	orders       map[string]*orderv1.Order           // key: "tenant_id:order_id" -> Order
	orderNoIndex map[string]string                   // key: "tenant_id:order_no" -> order_id
	orderKeys    []string                            // chronological order of keys

	// Marketplace Parent-Child Order Topology
	parentOrders  map[string]*orderv1.ParentOrder // key: parent_id -> ParentOrder
	parentNoIndex map[string]string               // key: "tenant_id:order_no" -> parent_id
	parentKeys    []string
	childOrders   map[string]*orderv1.ChildOrder // key: "tenant_id:child_id" -> ChildOrder
	childNoIndex  map[string]string              // key: "tenant_id:order_no" -> child_id
	childKeys     []string

	// Granular SKU & Child Order After-Sales
	afterSales        map[string]*orderv1.AfterSalesOrder // key: id -> AfterSalesOrder
	afterSalesNoIndex map[string]string                   // key: "tenant_id:after_sales_no" -> id
	afterSalesKeys    []string                            // chronological order of keys

	// Redis ZSet Expiration Queue Simulation (ADR 0009)
	expirations map[string]int64 // orderID -> expireAt
}

// NewMemoryRepository initializes an in-memory repository with seed data.
func NewMemoryRepository() *MemoryRepository {
	repo := &MemoryRepository{
		carts:             make(map[string]map[string]*CartItemData),
		orders:            make(map[string]*orderv1.Order),
		orderNoIndex:      make(map[string]string),
		orderKeys:         make([]string, 0),
		parentOrders:      make(map[string]*orderv1.ParentOrder),
		parentNoIndex:     make(map[string]string),
		parentKeys:        make([]string, 0),
		childOrders:       make(map[string]*orderv1.ChildOrder),
		childNoIndex:      make(map[string]string),
		childKeys:         make([]string, 0),
		afterSales:        make(map[string]*orderv1.AfterSalesOrder),
		afterSalesNoIndex: make(map[string]string),
		afterSalesKeys:    make([]string, 0),
		expirations:       make(map[string]int64),
	}
	repo.seedInitialData()
	return repo
}

func (m *MemoryRepository) resolveTenant(ctx context.Context) string {
	tid := tenant.TenantID(ctx)
	if tid == "" {
		return "tenant-default"
	}
	return tid
}

func (m *MemoryRepository) seedInitialData() {
	defTenant := "tenant-default"
	custUser := "user-cust-1"
	now := time.Now().Unix()

	// Seed cart with 1 iPhone 15 Pro
	cartKey := fmt.Sprintf("%s:%s", defTenant, custUser)
	m.carts[cartKey] = map[string]*CartItemData{
		"sku-iphone-15-pro-black-256g": {
			SkuID:    "sku-iphone-15-pro-black-256g",
			Quantity: 1,
			Selected: true,
		},
	}

	// Seed 1 existing PAID order
	orderID := "ord-demo-001"
	orderNo := "ORD202609250001"
	demoOrder := &orderv1.Order{
		Id:              orderID,
		TenantId:        defTenant,
		OrderNo:         orderNo,
		UserId:          custUser,
		TotalAmount:     799900,
		PayAmount:       799900,
		Status:          "PAID",
		ReceiverName:    "张三",
		ReceiverPhone:   "13800138000",
		ReceiverAddress: "北京市海淀区中关村南大街 1 号",
		ExpireAt:        now + 900,
		PaidAt:          now - 300,
		CreatedAt:       now - 600,
		Items: []*orderv1.OrderItem{
			{
				Id:        "item-demo-001",
				OrderId:   orderID,
				SkuId:     "sku-iphone-15-pro-black-256g",
				Title:     "Apple iPhone 15 Pro 256GB 黑色钛金属",
				Image:     "https://images.unsplash.com/photo-1592750475338-74b7b21085ab",
				AttrsJson: `{"颜色":"黑色钛金属","存储容量":"256GB"}`,
				Price:     799900,
				Quantity:  1,
				Subtotal:  799900,
			},
		},
	}

	key := fmt.Sprintf("%s:%s", defTenant, orderID)
	m.orders[key] = demoOrder
	m.orderNoIndex[fmt.Sprintf("%s:%s", defTenant, orderNo)] = orderID
	m.orderKeys = append(m.orderKeys, key)

	// Seed ParentOrder and ChildOrder for Marketplace Parent-Child Order Topology
	parentID := "pord-demo-001"
	parentOrderNo := "PORD202609250001"
	childID := "cord-demo-001"
	childOrderNo := "CORD202609250001"

	demoChildOrder := &orderv1.ChildOrder{
		Id:             childID,
		ParentId:       parentID,
		TenantId:       defTenant,
		OrderNo:        childOrderNo,
		UserId:         custUser,
		TotalAmount:    799900,
		DiscountAmount: 0,
		PayAmount:      799900,
		Status:         "PAID",
		ShippedAt:      now - 300,
		CreatedAt:      now - 600,
		Items: []*orderv1.OrderItem{
			{
				Id:                "item-demo-001",
				OrderId:           childID,
				ParentOrderId:     parentID,
				TenantId:          defTenant,
				SkuId:             "sku-iphone-15-pro-black-256g",
				Title:             "Apple iPhone 15 Pro 256GB 黑色钛金属",
				Image:             "https://images.unsplash.com/photo-1592750475338-74b7b21085ab",
				AttrsJson:         `{"颜色":"黑色钛金属","存储容量":"256GB"}`,
				Price:             799900,
				Quantity:          1,
				Subtotal:          799900,
				OriginalPrice:     799900,
				AllocatedDiscount: 0,
				RealizedPrice:     799900,
			},
		},
	}

	demoParentOrder := &orderv1.ParentOrder{
		Id:              parentID,
		TenantId:        defTenant,
		OrderNo:         parentOrderNo,
		UserId:          custUser,
		TotalAmount:     799900,
		DiscountAmount:  0,
		PayAmount:       799900,
		Status:          "PAID",
		ReceiverName:    "张三",
		ReceiverPhone:   "13800138000",
		ReceiverAddress: "北京市海淀区中关村南大街 1 号",
		ExpireAt:        now + 900,
		PaidAt:          now - 300,
		CreatedAt:       now - 600,
		ChildOrders:     []*orderv1.ChildOrder{demoChildOrder},
	}

	m.parentOrders[parentID] = demoParentOrder
	m.parentNoIndex[fmt.Sprintf("%s:%s", defTenant, parentOrderNo)] = parentID
	m.parentKeys = append(m.parentKeys, parentID)

	childKey := fmt.Sprintf("%s:%s", defTenant, childID)
	m.childOrders[childKey] = demoChildOrder
	m.childNoIndex[fmt.Sprintf("%s:%s", defTenant, childOrderNo)] = childID
	m.childKeys = append(m.childKeys, childKey)
}

// --- Cart Operations ---

func (m *MemoryRepository) GetCartItems(ctx context.Context, userID string) ([]*CartItemData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tid := m.resolveTenant(ctx)
	key := fmt.Sprintf("%s:%s", tid, userID)

	userCart, ok := m.carts[key]
	if !ok {
		return []*CartItemData{}, nil
	}

	items := make([]*CartItemData, 0, len(userCart))
	for _, item := range userCart {
		items = append(items, &CartItemData{
			SkuID:    item.SkuID,
			Quantity: item.Quantity,
			Selected: item.Selected,
		})
	}
	return items, nil
}

func (m *MemoryRepository) SetCartItem(ctx context.Context, userID string, item *CartItemData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid := m.resolveTenant(ctx)
	key := fmt.Sprintf("%s:%s", tid, userID)

	if _, ok := m.carts[key]; !ok {
		m.carts[key] = make(map[string]*CartItemData)
	}

	m.carts[key][item.SkuID] = &CartItemData{
		SkuID:    item.SkuID,
		Quantity: item.Quantity,
		Selected: item.Selected,
	}
	return nil
}

func (m *MemoryRepository) RemoveCartItem(ctx context.Context, userID, skuID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid := m.resolveTenant(ctx)
	key := fmt.Sprintf("%s:%s", tid, userID)

	if userCart, ok := m.carts[key]; ok {
		delete(userCart, skuID)
	}
	return nil
}

func (m *MemoryRepository) RemoveCartItems(ctx context.Context, userID string, skuIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid := m.resolveTenant(ctx)
	key := fmt.Sprintf("%s:%s", tid, userID)

	if userCart, ok := m.carts[key]; ok {
		for _, skuID := range skuIDs {
			delete(userCart, skuID)
		}
	}
	return nil
}

func (m *MemoryRepository) ClearCart(ctx context.Context, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid := m.resolveTenant(ctx)
	key := fmt.Sprintf("%s:%s", tid, userID)
	delete(m.carts, key)
	return nil
}







// --- Order Operations ---

func (m *MemoryRepository) CreateOrder(ctx context.Context, order *orderv1.Order) (*orderv1.Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid := m.resolveTenant(ctx)
	if order.TenantId != "" {
		tid = order.TenantId
	}

	orderID := order.Id
	if orderID == "" {
		orderID = "ord-" + uuid.NewString()
	}

	now := time.Now().Unix()
	expireAt := order.ExpireAt
	if expireAt <= 0 {
		expireAt = now + 900 // 15 mins
	}

	orderCopy := proto.Clone(order).(*orderv1.Order)
	orderCopy.Id = orderID
	orderCopy.TenantId = tid
	orderCopy.ExpireAt = expireAt
	orderCopy.CreatedAt = now

	for _, item := range orderCopy.Items {
		if item.Id == "" {
			item.Id = "item-" + uuid.NewString()
		}
		item.OrderId = orderID
	}

	key := fmt.Sprintf("%s:%s", tid, orderID)
	m.orders[key] = orderCopy
	m.orderNoIndex[fmt.Sprintf("%s:%s", tid, orderCopy.OrderNo)] = orderID
	m.orderKeys = append(m.orderKeys, key)

	return proto.Clone(orderCopy).(*orderv1.Order), nil
}

func (m *MemoryRepository) GetOrder(ctx context.Context, orderID string) (*orderv1.Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tid := m.resolveTenant(ctx)
	if tid != "" && tid != "platform" {
		key := fmt.Sprintf("%s:%s", tid, orderID)
		if order, ok := m.orders[key]; ok {
			return proto.Clone(order).(*orderv1.Order), nil
		}
	}

	for _, order := range m.orders {
		if order.Id == orderID {
			return proto.Clone(order).(*orderv1.Order), nil
		}
	}
	return nil, ErrOrderNotFound
}

func (m *MemoryRepository) GetOrderByNo(ctx context.Context, orderNo string) (*orderv1.Order, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tid := m.resolveTenant(ctx)
	if tid != "" && tid != "platform" {
		if orderID, ok := m.orderNoIndex[fmt.Sprintf("%s:%s", tid, orderNo)]; ok {
			key := fmt.Sprintf("%s:%s", tid, orderID)
			if order, ok := m.orders[key]; ok {
				return proto.Clone(order).(*orderv1.Order), nil
			}
		}
	}

	for _, order := range m.orders {
		if order.OrderNo == orderNo {
			return proto.Clone(order).(*orderv1.Order), nil
		}
	}
	return nil, ErrOrderNotFound
}

func (m *MemoryRepository) ListOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.Order, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tid := m.resolveTenant(ctx)
	page, pageSize = paging.Normalize(page, pageSize)

	var filtered []*orderv1.Order
	// Iterate in reverse (newest first)
	for i := len(m.orderKeys) - 1; i >= 0; i-- {
		key := m.orderKeys[i]
		o := m.orders[key]
		if tid != "" && tid != "platform" && o.TenantId != tid {
			continue
		}
		if userID != "" && o.UserId != userID {
			continue
		}
		if status != "" && o.Status != status {
			continue
		}
		filtered = append(filtered, o)
	}

	total := int64(len(filtered))
	start := paging.Offset(page, pageSize)
	if start >= int(total) {
		return []*orderv1.Order{}, total, nil
	}
	end := start + pageSize
	if end > int(total) {
		end = int(total)
	}

	pageSlice := filtered[start:end]
	res := make([]*orderv1.Order, len(pageSlice))
	for i, o := range pageSlice {
		res[i] = proto.Clone(o).(*orderv1.Order)
	}
	return res, total, nil
}

func (m *MemoryRepository) UpdateOrderStatus(ctx context.Context, orderID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid := m.resolveTenant(ctx)
	var order *orderv1.Order
	if tid != "" && tid != "platform" {
		key := fmt.Sprintf("%s:%s", tid, orderID)
		if o, ok := m.orders[key]; ok {
			order = o
		}
	}
	if order == nil {
		for _, o := range m.orders {
			if o.Id == orderID {
				order = o
				break
			}
		}
	}

	if order == nil {
		return false, nil
	}
	if oldStatus != "" && order.Status != oldStatus {
		return false, nil
	}

	order.Status = newStatus
	if paidAt, ok := toUnixSeconds(extraUpdates["paid_at"]); ok {
		order.PaidAt = paidAt
	}
	if shippedAt, ok := toUnixSeconds(extraUpdates["shipped_at"]); ok {
		order.ShippedAt = shippedAt
	}

	// Also synchronize corresponding child order
	childKey := fmt.Sprintf("%s:%s", order.TenantId, orderID)
	if ch, found := m.childOrders[childKey]; found {
		ch.Status = newStatus
		if order.ShippedAt > 0 {
			ch.ShippedAt = order.ShippedAt
		}
	}

	return true, nil
}

// Step 3.1: 持久化主订单及其聚合的一组商家子订单
// 来源: OrderSplitter 拆单引擎计算输出的 *orderv1.ParentOrder 与 []*orderv1.ChildOrder
// 用于: 将主单、子单和明细数据写入内存索引，保障买家付款与商家履约的拓扑对应
func (m *MemoryRepository) CreateParentOrder(ctx context.Context, parent *orderv1.ParentOrder, children []*orderv1.ChildOrder) (*orderv1.ParentOrder, []*orderv1.ChildOrder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().Unix()
	parentID := parent.Id
	if parentID == "" {
		parentID = "pord-" + uuid.NewString()
	}

	expireAt := parent.ExpireAt
	if expireAt <= 0 {
		expireAt = now + 900 // 15 mins default
	}

	parentCopy := proto.Clone(parent).(*orderv1.ParentOrder)
	parentCopy.Id = parentID
	if parentCopy.TenantId == "" {
		parentCopy.TenantId = m.resolveTenant(ctx)
	}
	parentCopy.ExpireAt = expireAt
	parentCopy.CreatedAt = now

	savedChildren := make([]*orderv1.ChildOrder, len(children))
	for i, ch := range children {
		childID := ch.Id
		if childID == "" {
			childID = "cord-" + uuid.NewString()
		}

		childCopy := proto.Clone(ch).(*orderv1.ChildOrder)
		childCopy.Id = childID
		childCopy.ParentId = parentID
		childCopy.CreatedAt = now

		for _, item := range childCopy.Items {
			if item.Id == "" {
				item.Id = "item-" + uuid.NewString()
			}
			item.OrderId = childID
			item.ParentOrderId = parentID
			item.TenantId = childCopy.TenantId
		}

		childKey := fmt.Sprintf("%s:%s", childCopy.TenantId, childID)
		m.childOrders[childKey] = childCopy
		m.childNoIndex[fmt.Sprintf("%s:%s", childCopy.TenantId, childCopy.OrderNo)] = childID
		m.childKeys = append(m.childKeys, childKey)
		savedChildren[i] = childCopy

		// Synchronize backward-compatible Order representation
		legacyOrder := &orderv1.Order{
			Id:              childCopy.Id,
			TenantId:        childCopy.TenantId,
			OrderNo:         childCopy.OrderNo,
			UserId:          childCopy.UserId,
			TotalAmount:     childCopy.TotalAmount,
			PayAmount:       childCopy.PayAmount,
			Status:          childCopy.Status,
			ReceiverName:    parentCopy.ReceiverName,
			ReceiverPhone:   parentCopy.ReceiverPhone,
			ReceiverAddress: parentCopy.ReceiverAddress,
			ExpireAt:        expireAt,
			CreatedAt:       now,
			Items:           childCopy.Items,
			ParentId:        parentID,
		}
		legKey := fmt.Sprintf("%s:%s", childCopy.TenantId, childID)
		m.orders[legKey] = legacyOrder
		m.orderNoIndex[fmt.Sprintf("%s:%s", childCopy.TenantId, childCopy.OrderNo)] = childID
		m.orderKeys = append(m.orderKeys, legKey)
	}

	parentCopy.ChildOrders = savedChildren
	// Step 3.1.1: 记录主单归属租户
	// 来源: TenantServerInterceptor 注入的 x-tenant-id
	// 用于: Step 3.1.2 检索时做跨租户隔离
	ptid := m.resolveTenant(ctx)
	parentCopy.TenantId = ptid
	m.parentOrders[parentID] = parentCopy
	m.parentNoIndex[fmt.Sprintf("%s:%s", ptid, parentCopy.OrderNo)] = parentID
	m.parentKeys = append(m.parentKeys, parentID)

	return proto.Clone(parentCopy).(*orderv1.ParentOrder), savedChildren, nil
}

// Step 3.2: 根据主订单 ID 查询主单详情及关联子单
// 来源: 买家在订单详情页展示或支付网关发起的 parentID 查询
// 用于: 返回完整聚合的主订单实体供收银台和详情页渲染
func (m *MemoryRepository) GetParentOrder(ctx context.Context, parentID string) (*orderv1.ParentOrder, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Step 3.2.1: 校验主单归属租户与当前请求租户一致
	// 来源: 主单上的 TenantId 与 ctx 中的租户
	// 用于: Step 3.2.2 阻止跨租户读取主单详情
	p, ok := m.parentOrders[parentID]
	if !ok {
		return nil, ErrOrderNotFound
	}
	tid := m.resolveTenant(ctx)
	if tid != "" && tid != "platform" && p.TenantId != "" && p.TenantId != "platform" && p.TenantId != tid {
		return nil, ErrOrderNotFound
	}
	return proto.Clone(p).(*orderv1.ParentOrder), nil
}

// Step 3.3: 根据主订单号反查主订单
// 来源: 外部支付回调通知携带的主订单编号 orderNo
// 用于: 定位待核销的主订单实体
func (m *MemoryRepository) GetParentOrderByNo(ctx context.Context, orderNo string) (*orderv1.ParentOrder, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Step 3.3.1: 索引键已含租户前缀，跨租户查同一单号不会命中
	// 来源: Step 3.1 写入的 "tenant_id:order_no" 索引
	// 用于: Step 3.3.2 定位待核销的主订单实体
	tid := m.resolveTenant(ctx)
	if tid != "" && tid != "platform" {
		parentID, ok := m.parentNoIndex[fmt.Sprintf("%s:%s", tid, orderNo)]
		if !ok {
			return nil, ErrOrderNotFound
		}
		p, ok := m.parentOrders[parentID]
		if !ok {
			return nil, ErrOrderNotFound
		}
		return proto.Clone(p).(*orderv1.ParentOrder), nil
	}

	for _, p := range m.parentOrders {
		if p.OrderNo == orderNo {
			return proto.Clone(p).(*orderv1.ParentOrder), nil
		}
	}
	return nil, ErrOrderNotFound
}

// Step 3.4: 分页检索买家名下所有主订单
// 来源: 买家个人中心我的订单列表请求 (userID, status, page, pageSize)
// 用于: 倒序返回买家聚合支付维度的主订单分页数据
func (m *MemoryRepository) ListParentOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.ParentOrder, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tid := m.resolveTenant(ctx)
	page, pageSize = paging.Normalize(page, pageSize)

	var filtered []*orderv1.ParentOrder
	for i := len(m.parentKeys) - 1; i >= 0; i-- {
		pid := m.parentKeys[i]
		p := m.parentOrders[pid]
		if tid != "" && tid != "platform" && p.TenantId != "" && p.TenantId != "platform" && p.TenantId != tid {
			continue
		}
		if userID != "" && p.UserId != userID {
			continue
		}
		if status != "" && p.Status != status {
			continue
		}
		filtered = append(filtered, p)
	}

	total := int64(len(filtered))
	start := paging.Offset(page, pageSize)
	if start >= int(total) {
		return []*orderv1.ParentOrder{}, total, nil
	}
	end := start + pageSize
	if end > int(total) {
		end = int(total)
	}

	pageSlice := filtered[start:end]
	res := make([]*orderv1.ParentOrder, len(pageSlice))
	for i, p := range pageSlice {
		res[i] = proto.Clone(p).(*orderv1.ParentOrder)
	}
	return res, total, nil
}

// Step 3.5: 更新主订单状态及联动各子订单
// 来源: 支付成功或超时关单触发的 (parentID, oldStatus, newStatus)
// 用于: CAS 驱动主订单及其名下全部子订单生命周期状态流转
func (m *MemoryRepository) UpdateParentOrderStatus(ctx context.Context, parentID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Step 3.5.1: 校验归属租户，阻止跨租户改单状态
	// 来源: 主单上的 TenantId 与 ctx 中的租户
	// 用于: Step 3.5.2 CAS 状态流转
	p, ok := m.parentOrders[parentID]
	if !ok {
		return false, nil
	}
	tid := m.resolveTenant(ctx)
	if tid != "" && tid != "platform" && p.TenantId != tid {
		return false, nil
	}
	if oldStatus != "" && p.Status != oldStatus {
		return false, nil
	}

	p.Status = newStatus
	if paidAt, ok := toUnixSeconds(extraUpdates["paid_at"]); ok {
		p.PaidAt = paidAt
	}

	// Cascade status to child orders if applicable (e.g. PAID or CANCELLED)
	for _, ch := range p.ChildOrders {
		childKey := fmt.Sprintf("%s:%s", ch.TenantId, ch.Id)
		if realChild, found := m.childOrders[childKey]; found {
			realChild.Status = newStatus
		}
		legKey := fmt.Sprintf("%s:%s", ch.TenantId, ch.Id)
		if legOrder, found := m.orders[legKey]; found {
			legOrder.Status = newStatus
			if p.PaidAt > 0 {
				legOrder.PaidAt = p.PaidAt
			}
		}
	}

	return true, nil
}

// Step 3.6: 根据子订单 ID 查询商家子订单
// 来源: 商家端管理后台或履约发货发起的 childID 查询
// 用于: 返回单个商家的独立履约子订单
func (m *MemoryRepository) GetChildOrder(ctx context.Context, childID string) (*orderv1.ChildOrder, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tid := m.resolveTenant(ctx)
	childKey := fmt.Sprintf("%s:%s", tid, childID)
	if ch, ok := m.childOrders[childKey]; ok {
		return proto.Clone(ch).(*orderv1.ChildOrder), nil
	}

	// Fallback scan across tenants by childID
	for _, ch := range m.childOrders {
		if ch.Id == childID {
			return proto.Clone(ch).(*orderv1.ChildOrder), nil
		}
	}
	return nil, ErrOrderNotFound
}

// Step 3.7: 根据子订单编号查询商家子订单
// 来源: 物流追踪或售后逆向流程根据子订单编号检索
// 用于: 准确定位商家子订单
func (m *MemoryRepository) GetChildOrderByNo(ctx context.Context, orderNo string) (*orderv1.ChildOrder, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tid := m.resolveTenant(ctx)
	childID, ok := m.childNoIndex[fmt.Sprintf("%s:%s", tid, orderNo)]
	if !ok {
		// Fallback scan across tenants
		for _, ch := range m.childOrders {
			if ch.OrderNo == orderNo {
				return proto.Clone(ch).(*orderv1.ChildOrder), nil
			}
		}
		return nil, ErrOrderNotFound
	}
	return m.GetChildOrder(ctx, childID)
}

// Step 3.8: 分页检索商家租户名下所有子订单
// 来源: 商家管理后台 AdminListOrders
// 用于: 仅展示该商家租户所属的子订单数据，实现严格的多租户隔离
func (m *MemoryRepository) ListChildOrders(ctx context.Context, tenantID, status string, page, pageSize int) ([]*orderv1.ChildOrder, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	page, pageSize = paging.Normalize(page, pageSize)

	var filtered []*orderv1.ChildOrder
	for i := len(m.childKeys) - 1; i >= 0; i-- {
		key := m.childKeys[i]
		ch := m.childOrders[key]
		if tenantID != "" && tenantID != "platform" && ch.TenantId != tenantID {
			continue
		}
		if status != "" && ch.Status != status {
			continue
		}
		filtered = append(filtered, ch)
	}

	total := int64(len(filtered))
	start := paging.Offset(page, pageSize)
	if start >= int(total) {
		return []*orderv1.ChildOrder{}, total, nil
	}
	end := start + pageSize
	if end > int(total) {
		end = int(total)
	}

	pageSlice := filtered[start:end]
	res := make([]*orderv1.ChildOrder, len(pageSlice))
	for i, ch := range pageSlice {
		res[i] = proto.Clone(ch).(*orderv1.ChildOrder)
	}
	return res, total, nil
}

// Step 3.9: 商家单独更新子订单履约状态 (发货、确认收货)
// 来源: 商家执行 AdminShipOrder 或买家单店确认收货
// 用于: 推进特定商家的子订单流转，并在全量履约完成后自动同步主订单
func (m *MemoryRepository) UpdateChildOrderStatus(ctx context.Context, childID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tid := m.resolveTenant(ctx)
	childKey := fmt.Sprintf("%s:%s", tid, childID)
	ch, ok := m.childOrders[childKey]
	if !ok {
		// Fallback lookup
		for _, c := range m.childOrders {
			if c.Id == childID {
				ch = c
				ok = true
				break
			}
		}
		if !ok {
			return false, nil
		}
	}

	if oldStatus != "" && ch.Status != oldStatus {
		return false, nil
	}

	ch.Status = newStatus
	if carrier, ok := extraUpdates["carrier"].(string); ok {
		ch.Carrier = carrier
	}
	if trackingNo, ok := extraUpdates["tracking_no"].(string); ok {
		ch.TrackingNo = trackingNo
	}
	if shippedAt, ok := toUnixSeconds(extraUpdates["shipped_at"]); ok {
		ch.ShippedAt = shippedAt
	}
	if completedAt, ok := toUnixSeconds(extraUpdates["completed_at"]); ok {
		ch.CompletedAt = completedAt
	}

	// Synchronize legacy order
	legKey := fmt.Sprintf("%s:%s", ch.TenantId, ch.Id)
	if legOrder, found := m.orders[legKey]; found {
		legOrder.Status = newStatus
		if ch.ShippedAt > 0 {
			legOrder.ShippedAt = ch.ShippedAt
		}
	}

	return true, nil
}

// Step 3.1: 创建售后单 (CreateAfterSales)
// 来源: 买家在订单中心提交的售后退款申请
// 用于: 将售后单、退款项及退款金额写入并发内存存储，维护售后单号索引
func (m *MemoryRepository) CreateAfterSales(ctx context.Context, as *orderv1.AfterSalesOrder) (*orderv1.AfterSalesOrder, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().Unix()
	asID := as.Id
	if asID == "" {
		asID = "as-" + uuid.NewString()
	}
	asNo := as.AfterSalesNo
	if asNo == "" {
		asNo = fmt.Sprintf("AS-%s-%d", as.ChildOrderId, now%1000000)
	}

	asCopy := proto.Clone(as).(*orderv1.AfterSalesOrder)
	asCopy.Id = asID
	asCopy.AfterSalesNo = asNo
	// Step 3.10.1: 售后单归属商家租户，而非发起申请的买家租户
	// 来源: 服务层由子订单派生的 as.TenantId（客户端无法伪造，已被 order.go 覆盖）
	// 用于: Step 3.10.2 落库归属与单号索引键；缺省时才回落到 ctx
	tid := as.TenantId
	if tid == "" {
		tid = m.resolveTenant(ctx)
	}
	asCopy.TenantId = tid
	if asCopy.CreatedAt == 0 {
		asCopy.CreatedAt = now
	}
	asCopy.UpdatedAt = now

	m.afterSales[asID] = asCopy
	m.afterSalesNoIndex[asNo] = asID
	m.afterSalesKeys = append(m.afterSalesKeys, asID)

	return proto.Clone(asCopy).(*orderv1.AfterSalesOrder), nil
}

// Step 3.2: 根据售后单 ID 查询详情 (GetAfterSales)
// 来源: 买家或商家查看售后单详情请求
// 用于: 从内存映射中读取售后单实体
func (m *MemoryRepository) GetAfterSales(ctx context.Context, id string) (*orderv1.AfterSalesOrder, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 注意: 售后单是设计上就跨租户的聚合 —— 买家(商城租户)发起、商家(店铺租户)处理。
	// 因此此处不做 tenant_id 过滤，归属校验由服务层按用户所有权 + 商家租户完成
	// (见 order.go: as.UserId != userID -> PermissionDenied, isMerchant := tenantID == as.TenantId)。
	as, ok := m.afterSales[id]
	if !ok {
		return nil, ErrAfterSalesNotFound
	}
	return proto.Clone(as).(*orderv1.AfterSalesOrder), nil
}

// Step 3.3: 根据售后单号查询详情 (GetAfterSalesByNo)
// 来源: 物流回调或防重校验
// 用于: 通过 afterSalesNoIndex 索引读取售后单实体
func (m *MemoryRepository) GetAfterSalesByNo(ctx context.Context, afterSalesNo string) (*orderv1.AfterSalesOrder, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	id, ok := m.afterSalesNoIndex[afterSalesNo]
	if !ok {
		return nil, ErrAfterSalesNotFound
	}
	as, ok := m.afterSales[id]
	if !ok {
		return nil, ErrAfterSalesNotFound
	}
	return proto.Clone(as).(*orderv1.AfterSalesOrder), nil
}

// Step 3.4: 原子流转售后单状态 (UpdateAfterSalesStatus)
// 来源: 商家审批、买家填单、退款完结或买家撤销驱动的状态变更
// 用于: CAS 校验旧状态并更新为新状态，更新附加字段 (如拒绝原因、退货运单号等)
func (m *MemoryRepository) UpdateAfterSalesStatus(ctx context.Context, id, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	as, ok := m.afterSales[id]
	if !ok {
		return false, nil
	}

	if oldStatus != "" && as.Status != oldStatus {
		return false, nil
	}

	as.Status = newStatus
	as.UpdatedAt = time.Now().Unix()

	if rejectReason, ok := extraUpdates["reject_reason"].(string); ok {
		as.RejectReason = rejectReason
	}
	if carrier, ok := extraUpdates["return_carrier"].(string); ok {
		as.ReturnCarrier = carrier
	}
	if trackingNo, ok := extraUpdates["return_tracking_no"].(string); ok {
		as.ReturnTrackingNo = trackingNo
	}

	return true, nil
}

// Step 3.5: 分页查询买家名下售后单列表 (ListAfterSales)
// 来源: 买家售后记录查询
// 用于: 按买家 ID 与可选状态过滤并分页倒序返回售后单
func (m *MemoryRepository) ListAfterSales(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.AfterSalesOrder, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	page, pageSize = paging.Normalize(page, pageSize)

	var filtered []*orderv1.AfterSalesOrder
	for i := len(m.afterSalesKeys) - 1; i >= 0; i-- {
		id := m.afterSalesKeys[i]
		as := m.afterSales[id]
		if userID != "" && as.UserId != userID {
			continue
		}
		if status != "" && as.Status != status {
			continue
		}
		filtered = append(filtered, as)
	}

	total := int64(len(filtered))
	start := paging.Offset(page, pageSize)
	if start >= int(total) {
		return []*orderv1.AfterSalesOrder{}, total, nil
	}
	end := start + pageSize
	if end > int(total) {
		end = int(total)
	}

	res := make([]*orderv1.AfterSalesOrder, end-start)
	for i, as := range filtered[start:end] {
		res[i] = proto.Clone(as).(*orderv1.AfterSalesOrder)
	}
	return res, total, nil
}

// Step 3.6: 商家端分页查询售后单列表 (AdminListAfterSales)
// 来源: 商家售后处理工作台
// 用于: 按商户租户 ID 与可选状态过滤并分页倒序返回待处理售后单
func (m *MemoryRepository) AdminListAfterSales(ctx context.Context, tenantID, status string, page, pageSize int) ([]*orderv1.AfterSalesOrder, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	page, pageSize = paging.Normalize(page, pageSize)

	var filtered []*orderv1.AfterSalesOrder
	for i := len(m.afterSalesKeys) - 1; i >= 0; i-- {
		id := m.afterSalesKeys[i]
		as := m.afterSales[id]
		if tenantID != "" && as.TenantId != tenantID {
			continue
		}
		if status != "" && as.Status != status {
			continue
		}
		filtered = append(filtered, as)
	}

	total := int64(len(filtered))
	start := paging.Offset(page, pageSize)
	if start >= int(total) {
		return []*orderv1.AfterSalesOrder{}, total, nil
	}
	end := start + pageSize
	if end > int(total) {
		end = int(total)
	}

	res := make([]*orderv1.AfterSalesOrder, end-start)
	for i, as := range filtered[start:end] {
		res[i] = proto.Clone(as).(*orderv1.AfterSalesOrder)
	}
	return res, total, nil
}

// --- Order Expiration Queue Simulation (ADR 0009) ---

// Step 4.1: 将待支付订单添加至内存延迟队列 (模拟 Redis ZAdd)
// 来源: CreateOrder / CreateParentOrder 成功创建的订单 ID 与过期时间戳 expireAt
// 用于: 超时关单协程定时拉取到期订单
func (m *MemoryRepository) AddOrderExpiration(ctx context.Context, orderID string, expireAt int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expirations[orderID] = expireAt
	return nil
}

// Step 4.2: 从内存延迟队列中轮询拉取当前已过期的订单 ID 列表 (模拟 Redis ZRangeByScore)
// 来源: 超时关单后台协程传入的当前时间戳 maxTimestamp 与批次数量 limit
// 用于: 按时间戳升序排序并截取前 limit 笔到期订单
func (m *MemoryRepository) PollExpiredOrders(ctx context.Context, maxTimestamp int64, limit int64) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	type item struct {
		id       string
		expireAt int64
	}
	var expired []item
	for id, exp := range m.expirations {
		if exp <= maxTimestamp {
			expired = append(expired, item{id: id, expireAt: exp})
		}
	}

	sort.Slice(expired, func(i, j int) bool {
		return expired[i].expireAt < expired[j].expireAt
	})

	if limit > 0 && int64(len(expired)) > limit {
		expired = expired[:limit]
	}

	res := make([]string, len(expired))
	for i, it := range expired {
		res[i] = it.id
	}
	return res, nil
}

// Step 4.3: 从内存延迟队列中移除指定订单 (模拟 Redis ZRem)
// 来源: PayOrder 支付成功、CancelOrder 主动取消或超时关单处理完成传入的 orderID
// 用于: 防止已支付或已取消的订单被重复扫描与处理
func (m *MemoryRepository) RemoveOrderExpiration(ctx context.Context, orderID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.expirations, orderID)
	return nil
}
