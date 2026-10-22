package biz

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"order/internal/conf"
	inventoryv1 "order/gen/inventory/v1"
	orderv1 "order/gen/order/v1"
	productv1 "order/gen/product/v1"
)

// RawSplitItem 封装原始 SKU 参数及解析后的元数据
type RawSplitItem struct {
	SkuID    string
	Quantity int64
	SKU      *productv1.SKU
}

// TenantItemGroup 聚合特定商户的结账商品条目
type TenantItemGroup struct {
	TenantID string
	Items    []*RawSplitItem
	Subtotal int64
}

// SuccessfulReservation 记录单店库存预占结果供异常补偿
type SuccessfulReservation struct {
	TenantID string
	OrderNo  string
	Items    []*inventoryv1.StockItemDelta
}

// OrderSplitter 编排跨商户拆单、平台优惠分摊及多租户原子库存预占
type OrderSplitter struct {
	inventoryClient inventoryv1.InventoryServiceClient
	dtmServer       string
	inventoryAddr   string
}

// Step 0.1: 构造 OrderSplitter 拆单引擎实例
// 来源: wire 注入的 inventoryClient 与 Order 配置
// 用于: 执行跨商家拆单、营销分摊及多租户原子库存预占
func NewOrderSplitter(inventoryClient inventoryv1.InventoryServiceClient, c *conf.Order) *OrderSplitter {
	dtmServer := ""
	inventoryAddr := ""
	if c != nil {
		dtmServer = c.DtmServer
		inventoryAddr = c.InventoryGrpcAddr
	}
	return &OrderSplitter{
		inventoryClient: inventoryClient,
		dtmServer:       dtmServer,
		inventoryAddr:   inventoryAddr,
	}
}

// Step 1.1: 结算条目按商家租户分组 (Tenant Grouping)
// 来源: 买家在结算页或下单请求传入的 []*RawSplitItem (包含已解析的 SKU 元数据)
// 用于: 将混合了多个商家的商品列表按 sku.TenantId 聚合为独立履约批次
func (s *OrderSplitter) GroupItemsByTenant(items []*RawSplitItem) []*TenantItemGroup {
	groupMap := make(map[string]*TenantItemGroup)
	var orderedGroups []*TenantItemGroup

	for _, it := range items {
		tid := it.SKU.TenantId
		if tid == "" {
			tid = "tenant-default"
		}

		grp, exists := groupMap[tid]
		if !exists {
			grp = &TenantItemGroup{
				TenantID: tid,
				Items:    make([]*RawSplitItem, 0),
			}
			groupMap[tid] = grp
			orderedGroups = append(orderedGroups, grp)
		}

		itemSubtotal := it.SKU.Price * it.Quantity
		grp.Subtotal += itemSubtotal
		grp.Items = append(grp.Items, it)
	}

	return orderedGroups
}

// Step 1.2: 跨商家平台满减比例分摊计算 (Proportional Discount Allocation)
// 来源: Step 1.1 分组好的 []*TenantItemGroup 与全平台满减优惠总额 totalDiscount (分)
// 用于: 精确计算各 SKU 分摊的优惠金额与实付单价快照，解决部分退款金额争议，消除分钱浮点误差
func (s *OrderSplitter) AllocateProportionalDiscounts(
	groups []*TenantItemGroup,
	totalDiscount int64,
) (map[string]int64, map[string]int64) {
	allocatedDiscounts := make(map[string]int64)
	realizedPrices := make(map[string]int64)

	var grossTotal int64
	var totalItemCount int
	for _, g := range groups {
		grossTotal += g.Subtotal
		totalItemCount += len(g.Items)
	}

	if grossTotal <= 0 || totalDiscount <= 0 {
		for _, g := range groups {
			for _, it := range g.Items {
				allocatedDiscounts[it.SkuID] = 0
				realizedPrices[it.SkuID] = it.SKU.Price
			}
		}
		return allocatedDiscounts, realizedPrices
	}

	if totalDiscount > grossTotal {
		totalDiscount = grossTotal
	}

	var accumulatedDiscount int64
	var itemIndex int

	for _, g := range groups {
		for _, it := range g.Items {
			itemIndex++
			subtotal := it.SKU.Price * it.Quantity

			var itemDiscount int64
			if itemIndex == totalItemCount {
				// 最后一个条目吸收整除余数，确保总分摊绝对等于 totalDiscount
				itemDiscount = totalDiscount - accumulatedDiscount
			} else {
				itemDiscount = (totalDiscount * subtotal) / grossTotal
				accumulatedDiscount += itemDiscount
			}

			if itemDiscount > subtotal {
				itemDiscount = subtotal
			}

			allocatedDiscounts[it.SkuID] = itemDiscount
			realizedPrice := (subtotal - itemDiscount) / it.Quantity
			realizedPrices[it.SkuID] = realizedPrice
		}
	}

	return allocatedDiscounts, realizedPrices
}

// Step 1.3: 跨商家库存全量原子预扣与补偿回滚 (All-or-Nothing Stock Reservation)
// 来源: Step 1.1 产生的分组列表及主订单号 parentOrderNo
// 用于: 纯粹通过 DTM Saga 执行跨微服务分布式库存预占编排
func (s *OrderSplitter) ReserveStockAllOrNothing(
	ctx context.Context,
	parentOrderNo string,
	groups []*TenantItemGroup,
) ([]SuccessfulReservation, error) {
	if s.inventoryClient == nil {
		return nil, nil
	}

	if s.dtmServer == "" || s.inventoryAddr == "" {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"DTM coordinator not configured for saga reservation",
		)
	}

	gid := fmt.Sprintf("order-saga-%s", parentOrderNo)
	saga := dtmgrpc.NewSagaGrpc(s.dtmServer, gid)
	saga.WaitResult = true
	saga.RequestTimeout = 2
	saga.RetryLimit = 15

	if len(groups) > 0 && groups[0].TenantID != "" {
		saga.BranchHeaders = map[string]string{"x-tenant-id": groups[0].TenantID}
	}

	action := s.inventoryAddr + inventoryv1.InventoryService_ReserveStock_FullMethodName
	compensate := s.inventoryAddr + inventoryv1.InventoryService_ReleaseStock_FullMethodName

	var successfulReservations []SuccessfulReservation

	for i, g := range groups {
		childOrderNo := fmt.Sprintf("%s-C%02d", parentOrderNo, i+1)
		deltas := make([]*inventoryv1.StockItemDelta, len(g.Items))
		for j, it := range g.Items {
			deltas[j] = &inventoryv1.StockItemDelta{
				SkuId:    it.SkuID,
				Quantity: it.Quantity,
			}
		}

		req := &inventoryv1.ReserveStockRequest{
			OrderNo: childOrderNo,
			Items:   deltas,
		}

		saga.Add(action, compensate, req)
		if g.TenantID != "" && len(saga.Steps) > 0 {
			saga.Steps[len(saga.Steps)-1]["x-tenant-id"] = g.TenantID
		}

		successfulReservations = append(successfulReservations, SuccessfulReservation{
			TenantID: g.TenantID,
			OrderNo:  childOrderNo,
			Items:    deltas,
		})
	}

	if err := saga.Submit(); err != nil {
		return nil, status.Errorf(
			codes.ResourceExhausted,
			"checkout inventory reservation rejected by DTM Saga: %v",
			err,
		)
	}

	return successfulReservations, nil
}

// Step 1.4: 聚合装配 ParentOrder 与 ChildOrders 实体
// 来源: Step 1.1 分组、Step 1.2 实付分摊、Step 1.3 预占成功的子单编号及收货人信息
// 用于: 生成符合淘宝模型的主子订单拓扑数据结构，准备写入数据库存储
func (s *OrderSplitter) AssembleParentAndChildOrders(
	parentOrderNo string,
	userID string,
	receiverName string,
	receiverPhone string,
	receiverAddress string,
	groups []*TenantItemGroup,
	allocatedDiscounts map[string]int64,
	realizedPrices map[string]int64,
	reservations []SuccessfulReservation,
) (*orderv1.ParentOrder, []*orderv1.ChildOrder) {
	now := time.Now().Unix()
	parentID := "pord-" + uuid.NewString()

	resMap := make(map[string]string)
	for _, r := range reservations {
		resMap[r.TenantID] = r.OrderNo
	}

	var parentTotalAmount int64
	var parentDiscountAmount int64
	var parentPayAmount int64

	childOrders := make([]*orderv1.ChildOrder, len(groups))

	for i, g := range groups {
		childID := "cord-" + uuid.NewString()
		childOrderNo := resMap[g.TenantID]
		if childOrderNo == "" {
			childOrderNo = fmt.Sprintf("%s-C%02d", parentOrderNo, i+1)
		}

		var childTotal int64
		var childDiscount int64

		orderItems := make([]*orderv1.OrderItem, len(g.Items))
		for j, it := range g.Items {
			subtotal := it.SKU.Price * it.Quantity
			discount := allocatedDiscounts[it.SkuID]
			realized := realizedPrices[it.SkuID]

			childTotal += subtotal
			childDiscount += discount

			orderItems[j] = &orderv1.OrderItem{
				Id:                "item-" + uuid.NewString(),
				OrderId:           childID,
				ParentOrderId:     parentID,
				TenantId:          g.TenantID,
				SkuId:             it.SkuID,
				Title:             it.SKU.Title,
				Image:             it.SKU.Image,
				AttrsJson:         it.SKU.AttrsJson,
				Price:             it.SKU.Price,
				Quantity:          it.Quantity,
				Subtotal:          subtotal,
				OriginalPrice:     it.SKU.Price,
				AllocatedDiscount: discount,
				RealizedPrice:     realized,
			}
		}

		childPay := childTotal - childDiscount
		parentTotalAmount += childTotal
		parentDiscountAmount += childDiscount
		parentPayAmount += childPay

		childOrders[i] = &orderv1.ChildOrder{
			Id:             childID,
			ParentId:       parentID,
			TenantId:       g.TenantID,
			OrderNo:        childOrderNo,
			UserId:         userID,
			TotalAmount:    childTotal,
			DiscountAmount: childDiscount,
			PayAmount:      childPay,
			Status:         "PENDING_PAYMENT",
			CreatedAt:      now,
			Items:          orderItems,
		}
	}

	parentOrder := &orderv1.ParentOrder{
		Id:              parentID,
		OrderNo:         parentOrderNo,
		UserId:          userID,
		TotalAmount:     parentTotalAmount,
		DiscountAmount:  parentDiscountAmount,
		PayAmount:       parentPayAmount,
		Status:          "PENDING_PAYMENT",
		ReceiverName:    receiverName,
		ReceiverPhone:   receiverPhone,
		ReceiverAddress: receiverAddress,
		ExpireAt:        now + 900,
		CreatedAt:       now,
		ChildOrders:     childOrders,
	}

	return parentOrder, childOrders
}

// GenerateMarketplaceOrderNo 生成平台级主订单编号
//
// Deprecated: 主订单编号必须由结算令牌确定性推导 (biz.DeriveParentOrderNo)，
// 随机编号会让租约自愈重入产生新的 DTM 全局事务 ID 与库存幂等键，
// 从而重复预占库存。提单链路请改用 DeriveParentOrderNo。
func GenerateMarketplaceOrderNo() string {
	return fmt.Sprintf("PORD%s%04d", time.Now().Format("20060102150405"), rand.Intn(10000))
}
