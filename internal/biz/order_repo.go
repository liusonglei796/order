package biz

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	orderv1 "order/gen/order/v1"
)

var (
	ErrOrderNotFound           = errors.New("order not found")
	ErrInvalidOrderStatus      = errors.New("invalid order status transition")
	ErrSettlementTokenExpired  = errors.New("settlement token expired or invalid")
	ErrSettlementTokenConsumed = errors.New("settlement token has already been consumed")
	ErrAfterSalesNotFound      = errors.New("after-sales order not found")
	// ErrDuplicateSettlementToken 表示同一结算令牌已落库成单 (数据库唯一索引兜底触发)
	ErrDuplicateSettlementToken = errors.New("settlement token already bound to an existing parent order")
)

const (
	TokenStatusInit          = "INIT"
	TokenStatusProcessing    = "PROCESSING"
	TokenStatusSuccess       = "SUCCESS"
	TokenStatusRetryableFail = "RETRYABLE_FAIL"
	TokenStatusFatalFail     = "FATAL_FAIL"
)

// 结算令牌各状态在 Redis 中的存活窗口
// 来源: 结算令牌全生命周期状态机设计 (INIT 签发长 TTL / SUCCESS 固化长 TTL / FAIL 短 TTL)
// 用于: 统一治理 Redis Hash 过期策略, 消除各层散落的魔法数字
const (
	// SettlementTokenInitTTL 初始态 TTL: 覆盖用户核对地址与优惠券的人工决策时长
	SettlementTokenInitTTL = 30 * time.Minute
	// SettlementTokenSuccessTTL 成功终态 TTL: 固化订单快照, 支撑长周期弱网重试秒级回显
	SettlementTokenSuccessTTL = 24 * time.Hour
	// SettlementTokenFatalFailTTL 硬失败 TTL: 业务前置不满足, 短期留存用于前端提示溯源
	SettlementTokenFatalFailTTL = 5 * time.Minute
	// SettlementTokenRetryableFailTTL 可重试软失败 TTL: 等待客户端重试接管
	SettlementTokenRetryableFailTTL = 15 * time.Minute
	// SettlementTokenKeyTTL 抢占令牌时刷新到 Redis 的物理存活上限 (防止长租约期间 key 提前消失)
	SettlementTokenKeyTTL = 30 * time.Minute
)

// IsDuplicateSettlementToken 判断数据库错误是否为 settlement_token 唯一索引冲突
// 来源: MySQL 1062 Duplicate entry / SQLite UNIQUE constraint failed 错误文本
// 用于: 租约自愈重入时识别"订单其实已落库", 转为回显既有订单而非重复建单
func IsDuplicateSettlementToken(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrDuplicateSettlementToken) {
		return true
	}
	msg := err.Error()
	isDuplicate := strings.Contains(msg, "Duplicate entry") ||
		strings.Contains(msg, "1062") ||
		strings.Contains(msg, "UNIQUE constraint failed")
	if !isDuplicate {
		return false
	}
	// 索引名命中即判定为结算令牌冲突; 若驱动未回显索引名则保守判定为任意唯一键冲突
	return strings.Contains(msg, "uniq_parent_settlement_token") || !strings.Contains(msg, "uniq_")
}

type TokenAcquireResult struct {
	Action        string
	ParentOrderID string
	ParentOrderNo string
	Reason        string
	// LockTime 抢占成功时写入 Redis 的租约起算点 (毫秒时间戳)
	// 用于: 租约续期时的 CAS 比对, 避免已失去锁的节点覆盖新持有者的租约
	LockTime int64
	// Snapshot 成功终态下固化在 Redis 中的主订单 JSON 快照 (含子单与明细)
	// 用于: 弱网重试命中时纯内存直接回显, 完全不穿透 MySQL
	Snapshot string
}

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

	// Settlement Tokens
	SaveSettlementToken(ctx context.Context, userID, token string, ttl time.Duration) error
	ConsumeSettlementToken(ctx context.Context, userID, token string) (bool, error)
	AcquireSettlementToken(ctx context.Context, userID, token string, leaseTimeout time.Duration) (*TokenAcquireResult, error)
	// RenewSettlementToken 以 CAS 方式续期持有中的租约
	// 来源: 提单业务执行期间的心跳协程, 携带抢占时拿到的 lockTime
	// 用于: 长耗时建单链路持续持有租约, 杜绝租约中途过期被他人接管导致重复建单
	RenewSettlementToken(ctx context.Context, token string, expectLockTime int64, ttl time.Duration) (newLockTime int64, ok bool, err error)
	RecordSettlementSuccess(ctx context.Context, token, parentOrderID, parentOrderNo, snapshot string, ttl time.Duration) error
	RecordSettlementFail(ctx context.Context, token string, isFatal bool, reason string) error

	// Order & OrderItems
	CreateOrder(ctx context.Context, order *orderv1.Order) (*orderv1.Order, error)
	GetOrder(ctx context.Context, orderID string) (*orderv1.Order, error)
	GetOrderByNo(ctx context.Context, orderNo string) (*orderv1.Order, error)
	ListOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.Order, int64, error)
	UpdateOrderStatus(ctx context.Context, orderID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error)

	// Parent & Child Orders
	// settlementToken 作为幂等键随建单事务落库, 由数据库唯一索引构成第二道兜底防线
	CreateParentOrder(ctx context.Context, parent *orderv1.ParentOrder, children []*orderv1.ChildOrder, settlementToken string) (*orderv1.ParentOrder, []*orderv1.ChildOrder, error)
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
