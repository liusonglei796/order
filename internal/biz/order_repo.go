package biz

import (
	"context"
	"database/sql"
	"errors"

	orderv1 "order/gen/order/v1"
)

var (
	ErrOrderNotFound      = errors.New("order not found")
	ErrInvalidOrderStatus = errors.New("invalid order status transition")
	ErrAfterSalesNotFound = errors.New("after-sales order not found")
)

type CartItemData struct {
	SkuID    string `json:"sku_id"`
	Quantity int64  `json:"quantity"`
	Selected bool   `json:"selected"`
}

type DBGetter interface {
	GetSQLDB() *sql.DB
}

type OrderRepo interface {
	// Cart
	GetCartItems(ctx context.Context, userID string) ([]*CartItemData, error)
	SetCartItem(ctx context.Context, userID string, item *CartItemData) error
	RemoveCartItem(ctx context.Context, userID, skuID string) error
	RemoveCartItems(ctx context.Context, userID string, skuIDs []string) error
	ClearCart(ctx context.Context, userID string) error

	// Order & OrderItems
	CreateOrder(ctx context.Context, order *orderv1.Order) (*orderv1.Order, error)
	GetOrder(ctx context.Context, orderID string) (*orderv1.Order, error)
	GetOrderByNo(ctx context.Context, orderNo string) (*orderv1.Order, error)
	ListOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.Order, int64, error)
	UpdateOrderStatus(ctx context.Context, orderID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error)

	// Parent & Child Orders
	CreateParentOrder(ctx context.Context, parent *orderv1.ParentOrder, children []*orderv1.ChildOrder) (*orderv1.ParentOrder, []*orderv1.ChildOrder, error)
	GetParentOrder(ctx context.Context, parentID string) (*orderv1.ParentOrder, error)
	GetParentOrderByNo(ctx context.Context, orderNo string) (*orderv1.ParentOrder, error)
	ListParentOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.ParentOrder, int64, error)
	UpdateParentOrderStatus(ctx context.Context, parentID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error)

	// Child Orders
	GetChildOrder(ctx context.Context, childID string) (*orderv1.ChildOrder, error)
	GetChildOrderByNo(ctx context.Context, orderNo string) (*orderv1.ChildOrder, error)
	ListChildOrders(ctx context.Context, tenantID, status string, page, pageSize int) ([]*orderv1.ChildOrder, int64, error)
	UpdateChildOrderStatus(ctx context.Context, childID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error)

	// After-Sales & Refunds
	CreateAfterSales(ctx context.Context, as *orderv1.AfterSalesOrder) (*orderv1.AfterSalesOrder, error)
	GetAfterSales(ctx context.Context, id string) (*orderv1.AfterSalesOrder, error)
	GetAfterSalesByNo(ctx context.Context, afterSalesNo string) (*orderv1.AfterSalesOrder, error)
	UpdateAfterSalesStatus(ctx context.Context, id, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error)
	ListAfterSales(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.AfterSalesOrder, int64, error)
	AdminListAfterSales(ctx context.Context, tenantID, status string, page, pageSize int) ([]*orderv1.AfterSalesOrder, int64, error)

	// Order Expiration Queue (Redis Sorted Set)
	AddOrderExpiration(ctx context.Context, orderID string, expireAt int64) error
	PollExpiredOrders(ctx context.Context, maxTimestamp int64, limit int64) ([]string, error)
	RemoveOrderExpiration(ctx context.Context, orderID string) error
}

type txContextKey struct{}

// Step 0.1: 将事务 *sql.Tx 注入上下文
// 来源: DTM 子事务屏障 barrier.CallWithDB 传入的底层 *sql.Tx
// 用于: 仓储层在事务中复用数据库连接
func WithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txContextKey{}, tx)
}

// Step 0.2: 从上下文中提取事务 *sql.Tx
// 来源: Step 0.1 注入的 context.Context
// 用于: 数据访问层判断是否在事务中执行
func TxFromContext(ctx context.Context) *sql.Tx {
	if tx, ok := ctx.Value(txContextKey{}).(*sql.Tx); ok {
		return tx
	}
	return nil
}
