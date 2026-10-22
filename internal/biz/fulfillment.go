package biz

import (
	"context"
	"database/sql"

	inventoryv1 "order/gen/inventory/v1"
	orderv1 "order/gen/order/v1"
)

// LocalTxFunc 定义由调用方提供的本地数据库事务闭包
type LocalTxFunc func(tx *sql.Tx) error

// TenantStockDeltas 封装归属单一商户租户的库存实扣/释放批次
type TenantStockDeltas struct {
	TenantID string
	OrderNo  string
	Items    []*inventoryv1.StockItemDelta
}

// OrderTarget 抹平多商家主订单 (ParentOrder) 与单店订单 (Single Order) 的数据结构差异
type OrderTarget struct {
	Parent *orderv1.ParentOrder
	Single *orderv1.Order
}

// Step 1.1: 从主订单构造 OrderTarget 联合载体
func FromParent(p *orderv1.ParentOrder) OrderTarget {
	return OrderTarget{Parent: p}
}

// Step 1.2: 从单店订单构造 OrderTarget 联合载体
func FromSingle(s *orderv1.Order) OrderTarget {
	return OrderTarget{Single: s}
}

// Step 1.3: 提取顶层业务订单编号
func (t OrderTarget) OrderNo() string {
	if t.Parent != nil {
		return t.Parent.OrderNo
	}
	if t.Single != nil {
		return t.Single.OrderNo
	}
	return ""
}

// Step 1.4: 提取订单主键标识
func (t OrderTarget) OrderID() string {
	if t.Parent != nil {
		return t.Parent.Id
	}
	if t.Single != nil {
		return t.Single.Id
	}
	return ""
}

// Step 1.5: 提取订单下单买家 UserID
func (t OrderTarget) UserID() string {
	if t.Parent != nil {
		return t.Parent.UserId
	}
	if t.Single != nil {
		return t.Single.UserId
	}
	return ""
}

// Step 1.6: 提取订单应付总金额 (分)
func (t OrderTarget) PayAmount() int64 {
	if t.Parent != nil {
		return t.Parent.PayAmount
	}
	if t.Single != nil {
		return t.Single.PayAmount
	}
	return 0
}

// Step 1.7: 提取担保金入账的目标租户标识
func (t OrderTarget) EscrowTenantID() string {
	if t.Parent != nil {
		return "platform"
	}
	if t.Single != nil && t.Single.TenantId != "" {
		return t.Single.TenantId
	}
	return "tenant-default"
}

// Step 1.8: 按商户租户分组萃取所有商品的库存变动条目
func (t OrderTarget) TenantDeltas() []TenantStockDeltas {
	if t.Parent != nil {
		result := make([]TenantStockDeltas, 0, len(t.Parent.ChildOrders))
		for _, ch := range t.Parent.ChildOrders {
			deltas := make([]*inventoryv1.StockItemDelta, len(ch.Items))
			for i, it := range ch.Items {
				deltas[i] = &inventoryv1.StockItemDelta{
					SkuId:    it.SkuId,
					Quantity: it.Quantity,
				}
			}
			result = append(result, TenantStockDeltas{
				TenantID: ch.TenantId,
				OrderNo:  ch.OrderNo,
				Items:    deltas,
			})
		}
		return result
	}

	if t.Single != nil {
		deltas := make([]*inventoryv1.StockItemDelta, len(t.Single.Items))
		for i, it := range t.Single.Items {
			deltas[i] = &inventoryv1.StockItemDelta{
				SkuId:    it.SkuId,
				Quantity: it.Quantity,
			}
		}
		tid := t.Single.TenantId
		if tid == "" {
			tid = "tenant-default"
		}
		return []TenantStockDeltas{
			{
				TenantID: tid,
				OrderNo:  t.Single.OrderNo,
				Items:    deltas,
			},
		}
	}

	return nil
}

// SettleResult 封装确认收货分账结算出参
type SettleResult struct {
	SettledAmount    int64
	CommissionAmount int64
}

// RefundTarget 封装售后单冲正退款履约入参
type RefundTarget struct {
	ChildOrderID string
	ChildOrderNo string
	RefundID     string
	TenantID     string
	Amount       int64
	Reason       string
	IsSettled    bool
	ReleaseItems []*inventoryv1.StockItemDelta
}

// Fulfiller 定义履约中台统一外部接缝
type Fulfiller interface {
	ConfirmPayment(ctx context.Context, target OrderTarget, payChannel string, db *sql.DB, localTxFunc LocalTxFunc) error
	ReleaseHold(ctx context.Context, target OrderTarget, reason string) error
	SettleEscrow(ctx context.Context, target OrderTarget) (*SettleResult, error)
	RefundEscrow(ctx context.Context, target RefundTarget) error
}
