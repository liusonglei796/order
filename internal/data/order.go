package data

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dtm-labs/client/dtmcli/dtmimp"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"order/internal/biz"
	orderv1 "order/gen/order/v1"
	"order/internal/pkg/paging"
	"order/internal/pkg/tenant"
)

type CartItemData = biz.CartItemData
type TokenAcquireResult = biz.TokenAcquireResult
type OrderRepo = biz.OrderRepo
type DBGetter = biz.DBGetter

var TxFromContext = biz.TxFromContext

const (
	TokenStatusInit          = biz.TokenStatusInit
	TokenStatusProcessing    = biz.TokenStatusProcessing
	TokenStatusSuccess       = biz.TokenStatusSuccess
	TokenStatusRetryableFail = biz.TokenStatusRetryableFail
	TokenStatusFatalFail     = biz.TokenStatusFatalFail

	OrderExpirationZSetKey = "order:expiration:zset"
)

// RedisClientConfig aliases options for connecting to Redis.
type RedisClientConfig struct {
	Addr     string
	Password string
	DB       int
}

// OrderModel maps to 'orders' table.
type OrderModel struct {
	ID              string     `gorm:"primaryKey;type:varchar(64)" json:"id"`
	TenantID        string     `gorm:"uniqueIndex:uniq_tenant_order_no;index:idx_tenant_user;type:varchar(64);not null;default:'tenant-default'" json:"tenant_id"`
	OrderNo         string     `gorm:"uniqueIndex:uniq_tenant_order_no;type:varchar(64);not null" json:"order_no"`
	UserID          string     `gorm:"index:idx_tenant_user;type:varchar(64);not null" json:"user_id"`
	TotalAmount     int64      `gorm:"type:bigint;not null" json:"total_amount"`
	PayAmount       int64      `gorm:"type:bigint;not null" json:"pay_amount"`
	Status          string     `gorm:"index:idx_tenant_status;type:varchar(32);not null;default:'PENDING_PAYMENT'" json:"status"`
	ReceiverName    string     `gorm:"type:varchar(64)" json:"receiver_name"`
	ReceiverPhone   string     `gorm:"type:varchar(32)" json:"receiver_phone"`
	ReceiverAddress string     `gorm:"type:varchar(255)" json:"receiver_address"`
	ExpireAt        time.Time  `gorm:"index:idx_expire_at" json:"expire_at"`
	PaidAt          *time.Time `json:"paid_at"`
	ShippedAt       *time.Time `json:"shipped_at"`
	CreatedAt       time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt       time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (OrderModel) TableName() string {
	return "orders"
}

// ParentOrderModel maps to 'parent_orders' table.
type ParentOrderModel struct {
	ID              string     `gorm:"primaryKey;type:varchar(64)" json:"id"`
	TenantID        string     `gorm:"uniqueIndex:uniq_parent_order_no;uniqueIndex:uniq_parent_settlement_token;index:idx_parent_tenant;type:varchar(64);not null;default:'tenant-default'" json:"tenant_id"`
	OrderNo         string     `gorm:"uniqueIndex:uniq_parent_order_no;type:varchar(64);not null" json:"order_no"`
	SettlementToken *string    `gorm:"uniqueIndex:uniq_parent_settlement_token;column:settlement_token;type:varchar(64)" json:"settlement_token"`
	UserID          string     `gorm:"index:idx_parent_user;type:varchar(64);not null" json:"user_id"`
	TotalAmount     int64      `gorm:"type:bigint;not null" json:"total_amount"`
	DiscountAmount  int64      `gorm:"type:bigint;not null;default:0" json:"discount_amount"`
	PayAmount       int64      `gorm:"type:bigint;not null" json:"pay_amount"`
	Status          string     `gorm:"index:idx_parent_status;type:varchar(32);not null;default:'PENDING_PAYMENT'" json:"status"`
	ReceiverName    string     `gorm:"type:varchar(64)" json:"receiver_name"`
	ReceiverPhone   string     `gorm:"type:varchar(32)" json:"receiver_phone"`
	ReceiverAddress string     `gorm:"type:varchar(255)" json:"receiver_address"`
	ExpireAt        time.Time  `gorm:"index:idx_parent_expire_at" json:"expire_at"`
	PaidAt          *time.Time `json:"paid_at"`
	CreatedAt       time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt       time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (ParentOrderModel) TableName() string {
	return "parent_orders"
}

// ChildOrderModel maps to 'child_orders' table.
type ChildOrderModel struct {
	ID             string     `gorm:"primaryKey;type:varchar(64)" json:"id"`
	ParentID       string     `gorm:"index:idx_parent_id;type:varchar(64);not null" json:"parent_id"`
	TenantID       string     `gorm:"uniqueIndex:uniq_tenant_child_order_no;type:varchar(64);not null" json:"tenant_id"`
	OrderNo        string     `gorm:"uniqueIndex:uniq_tenant_child_order_no;type:varchar(64);not null" json:"order_no"`
	UserID         string     `gorm:"index:idx_child_user;type:varchar(64);not null" json:"user_id"`
	TotalAmount    int64      `gorm:"type:bigint;not null" json:"total_amount"`
	DiscountAmount int64      `gorm:"type:bigint;not null;default:0" json:"discount_amount"`
	PayAmount      int64      `gorm:"type:bigint;not null" json:"pay_amount"`
	Status         string     `gorm:"index:idx_child_status;type:varchar(32);not null;default:'PENDING_PAYMENT'" json:"status"`
	Carrier        string     `gorm:"type:varchar(64)" json:"carrier"`
	TrackingNo     string     `gorm:"type:varchar(64)" json:"tracking_no"`
	ShippedAt      *time.Time `json:"shipped_at"`
	CompletedAt    *time.Time `json:"completed_at"`
	CreatedAt      time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt      time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

func (ChildOrderModel) TableName() string {
	return "child_orders"
}

// OrderItemModel maps to 'order_items' table.
type OrderItemModel struct {
	ID                string `gorm:"primaryKey;type:varchar(64)" json:"id"`
	TenantID          string `gorm:"index:idx_tenant_order_id;type:varchar(64);not null" json:"tenant_id"`
	OrderID           string `gorm:"index:idx_tenant_order_id;type:varchar(64);not null" json:"order_id"`
	ParentOrderID     string `gorm:"index:idx_parent_order_id;type:varchar(64)" json:"parent_order_id"`
	SKUID             string `gorm:"type:varchar(64);not null" json:"sku_id"`
	Title             string `gorm:"type:varchar(255);not null" json:"title"`
	Image             string `gorm:"type:varchar(512)" json:"image"`
	AttrsJSON         string `gorm:"type:text" json:"attrs_json"`
	Price             int64  `gorm:"type:bigint;not null" json:"price"`
	Quantity          int64  `gorm:"type:bigint;not null" json:"quantity"`
	Subtotal          int64  `gorm:"type:bigint;not null" json:"subtotal"`
	OriginalPrice     int64  `gorm:"type:bigint;not null;default:0" json:"original_price"`
	AllocatedDiscount int64  `gorm:"type:bigint;not null;default:0" json:"allocated_discount"`
	RealizedPrice     int64  `gorm:"type:bigint;not null;default:0" json:"realized_price"`
}

func (OrderItemModel) TableName() string {
	return "order_items"
}

func (m *OrderModel) ToProto(items []*orderv1.OrderItem) *orderv1.Order {
	if m == nil {
		return nil
	}
	var paidAt int64
	if m.PaidAt != nil {
		paidAt = m.PaidAt.Unix()
	}
	var shippedAt int64
	if m.ShippedAt != nil {
		shippedAt = m.ShippedAt.Unix()
	}
	return &orderv1.Order{
		Id:              m.ID,
		TenantId:        m.TenantID,
		OrderNo:         m.OrderNo,
		UserId:          m.UserID,
		TotalAmount:     m.TotalAmount,
		PayAmount:       m.PayAmount,
		Status:          m.Status,
		ReceiverName:    m.ReceiverName,
		ReceiverPhone:   m.ReceiverPhone,
		ReceiverAddress: m.ReceiverAddress,
		ExpireAt:        m.ExpireAt.Unix(),
		PaidAt:          paidAt,
		ShippedAt:       shippedAt,
		CreatedAt:       m.CreatedAt.Unix(),
		Items:           items,
	}
}

func (m *ParentOrderModel) ToProto(children []*orderv1.ChildOrder) *orderv1.ParentOrder {
	if m == nil {
		return nil
	}
	var paidAt int64
	if m.PaidAt != nil {
		paidAt = m.PaidAt.Unix()
	}
	return &orderv1.ParentOrder{
		Id:              m.ID,
		TenantId:        m.TenantID,
		OrderNo:         m.OrderNo,
		UserId:          m.UserID,
		TotalAmount:     m.TotalAmount,
		DiscountAmount:  m.DiscountAmount,
		PayAmount:       m.PayAmount,
		Status:          m.Status,
		ReceiverName:    m.ReceiverName,
		ReceiverPhone:   m.ReceiverPhone,
		ReceiverAddress: m.ReceiverAddress,
		ExpireAt:        m.ExpireAt.Unix(),
		PaidAt:          paidAt,
		CreatedAt:       m.CreatedAt.Unix(),
		ChildOrders:     children,
	}
}

func (m *ChildOrderModel) ToProto(items []*orderv1.OrderItem) *orderv1.ChildOrder {
	if m == nil {
		return nil
	}
	var shippedAt, completedAt int64
	if m.ShippedAt != nil {
		shippedAt = m.ShippedAt.Unix()
	}
	if m.CompletedAt != nil {
		completedAt = m.CompletedAt.Unix()
	}
	return &orderv1.ChildOrder{
		Id:             m.ID,
		ParentId:       m.ParentID,
		TenantId:       m.TenantID,
		OrderNo:        m.OrderNo,
		UserId:         m.UserID,
		TotalAmount:    m.TotalAmount,
		DiscountAmount: m.DiscountAmount,
		PayAmount:      m.PayAmount,
		Status:         m.Status,
		Carrier:        m.Carrier,
		TrackingNo:     m.TrackingNo,
		ShippedAt:      shippedAt,
		CompletedAt:    completedAt,
		CreatedAt:      m.CreatedAt.Unix(),
		Items:          items,
	}
}

func (m *OrderItemModel) ToProto() *orderv1.OrderItem {
	if m == nil {
		return nil
	}
	return &orderv1.OrderItem{
		Id:                m.ID,
		OrderId:           m.OrderID,
		ParentOrderId:     m.ParentOrderID,
		TenantId:          m.TenantID,
		SkuId:             m.SKUID,
		Title:             m.Title,
		Image:             m.Image,
		AttrsJson:         m.AttrsJSON,
		Price:             m.Price,
		Quantity:          m.Quantity,
		Subtotal:          m.Subtotal,
		OriginalPrice:     m.OriginalPrice,
		AllocatedDiscount: m.AllocatedDiscount,
		RealizedPrice:     m.RealizedPrice,
	}
}

// AfterSalesModel maps to 'after_sales_orders' table.
type AfterSalesModel struct {
	ID string `gorm:"primaryKey;type:varchar(64)" json:"id"`
	// TenantID 紧跟 ID 声明：GORM 按结构体字段顺序排索引列，
	// 保证 uniq_after_sales_no / idx_as_tenant_status 都以 tenant_id 打头
	TenantID         string    `gorm:"uniqueIndex:uniq_after_sales_no;index:idx_as_tenant_status;type:varchar(64);not null" json:"tenant_id"`
	AfterSalesNo     string    `gorm:"uniqueIndex:uniq_after_sales_no;type:varchar(64);not null" json:"after_sales_no"`
	ChildOrderID     string    `gorm:"index:idx_child_order;type:varchar(64);not null" json:"child_order_id"`
	ChildOrderNo     string    `gorm:"type:varchar(64);not null" json:"child_order_no"`
	ParentOrderID    string    `gorm:"type:varchar(64)" json:"parent_order_id"`
	UserID           string    `gorm:"index:idx_as_user_status;type:varchar(64);not null" json:"user_id"`
	Type             string    `gorm:"type:varchar(32);not null" json:"type"`
	Status           string    `gorm:"index:idx_as_tenant_status;type:varchar(32);not null;default:'APPLIED'" json:"status"`
	RefundAmount     int64     `gorm:"type:bigint;not null" json:"refund_amount"`
	Reason           string    `gorm:"type:varchar(255)" json:"reason"`
	RejectReason     string    `gorm:"type:varchar(255)" json:"reject_reason"`
	ReturnCarrier    string    `gorm:"type:varchar(64)" json:"return_carrier"`
	ReturnTrackingNo string    `gorm:"type:varchar(64)" json:"return_tracking_no"`
	CreatedAt        time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt        time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

func (AfterSalesModel) TableName() string {
	return "after_sales_orders"
}

func (m *AfterSalesModel) ToProto(items []*orderv1.AfterSalesItem) *orderv1.AfterSalesOrder {
	if m == nil {
		return nil
	}
	return &orderv1.AfterSalesOrder{
		Id:               m.ID,
		AfterSalesNo:     m.AfterSalesNo,
		ChildOrderId:     m.ChildOrderID,
		ChildOrderNo:     m.ChildOrderNo,
		ParentOrderId:    m.ParentOrderID,
		TenantId:         m.TenantID,
		UserId:           m.UserID,
		Type:             m.Type,
		Status:           m.Status,
		RefundAmount:     m.RefundAmount,
		Reason:           m.Reason,
		RejectReason:     m.RejectReason,
		ReturnCarrier:    m.ReturnCarrier,
		ReturnTrackingNo: m.ReturnTrackingNo,
		CreatedAt:        m.CreatedAt.Unix(),
		UpdatedAt:        m.UpdatedAt.Unix(),
		Items:            items,
	}
}

// AfterSalesItemModel maps to 'after_sales_items' table.
type AfterSalesItemModel struct {
	ID            string `gorm:"primaryKey;type:varchar(64)" json:"id"`
	TenantID      string `gorm:"index:idx_as_id;type:varchar(64);not null;default:'tenant-default'" json:"tenant_id"`
	AfterSalesID  string `gorm:"index:idx_as_id;type:varchar(64);not null" json:"after_sales_id"`
	SKUID         string `gorm:"type:varchar(64);not null" json:"sku_id"`
	Quantity      int64  `gorm:"type:bigint;not null" json:"quantity"`
	RealizedPrice int64  `gorm:"type:bigint;not null" json:"realized_price"`
	RefundAmount  int64  `gorm:"type:bigint;not null" json:"refund_amount"`
}

func (AfterSalesItemModel) TableName() string {
	return "after_sales_items"
}

func (m *AfterSalesItemModel) ToProto() *orderv1.AfterSalesItem {
	if m == nil {
		return nil
	}
	return &orderv1.AfterSalesItem{
		SkuId:         m.SKUID,
		Quantity:      m.Quantity,
		RealizedPrice: m.RealizedPrice,
		RefundAmount:  m.RefundAmount,
	}
}

// MySQLRepository implements Repository for the Order service.
type MySQLRepository struct {
	db          *gorm.DB
	redisClient redis.UniversalClient
}

// Step 0.1: 构造 Order 仓储层实例
// 来源: Wire 依赖注入的 Data 容器
// 用于: 提供订单、拆单子单、售后单及购物车等完整持久化能力
func NewOrderRepo(data *Data) (biz.OrderRepo, error) {
	if data.db == nil {
		otelzap.L().Info("No order database configured, using in-memory order repository")
		return NewMemoryRepository(), nil
	}

	if err := data.db.AutoMigrate(&OrderModel{}, &OrderItemModel{}, &ParentOrderModel{}, &ChildOrderModel{}, &AfterSalesModel{}, &AfterSalesItemModel{}); err != nil {
		otelzap.L().Warn("MySQL AutoMigrate failed for order, falling back to memory repository", zap.Error(err))
		return NewMemoryRepository(), nil
	}

	// Step 0.2: 确保 DTM 子事务屏障表存在于订单数据库 (ADR 0019)
	_ = data.db.Exec(`
CREATE TABLE IF NOT EXISTS dtm_barrier (
  id          BIGINT(22)   NOT NULL AUTO_INCREMENT,
  trans_type  VARCHAR(45)  DEFAULT '',
  gid         VARCHAR(128) NOT NULL,
  branch_id   VARCHAR(128) NOT NULL,
  op          VARCHAR(45)  NOT NULL,
  barrier_id  VARCHAR(45)  DEFAULT '',
  reason      VARCHAR(45)  DEFAULT '',
  create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_barrier (gid, branch_id, op, barrier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`).Error

	// Step 0.3: 对齐 DTM 子事务屏障表名与本库迁移脚本
	// 来源: migrations/20260929_dtm_barrier.sql 创建的无 Schema 表 dtm_barrier
	// 用于: 让 DoAndSubmitDB 与 QueryPrepared 读写同一张屏障表
	dtmimp.BarrierTableName = "dtm_barrier"

	otelzap.L().Info("Connected to MySQL and initialized order schema successfully")
	return &MySQLRepository{
		db:          data.db,
		redisClient: data.redis,
	}, nil
}

// Step 0.3: 返回底层 *sql.DB 用于 DTM 子事务屏障与事务消息
// 来源: GORM 实例 db.DB()
// 用于: 传递给 DTM 子事务屏障 barrier.CallWithDB 开启物理数据库事务
func (r *MySQLRepository) GetSQLDB() *sql.DB {
	if r.db == nil {
		return nil
	}
	sqlDB, err := r.db.DB()
	if err != nil {
		return nil
	}
	return sqlDB
}

// Step 0.4: 获取绑定了上下文或 DTM 事务连接的 *gorm.DB
// 来源: ctx 中解析的 TxFromContext(ctx) 或基础实例 r.db
// 用于: 支持 DTM DoAndSubmitDB 与子事务屏障复用外部事务连接 (ADR 0019)
func (r *MySQLRepository) getDB(ctx context.Context) *gorm.DB {
	db := r.db.WithContext(ctx)
	if sqlTx := TxFromContext(ctx); sqlTx != nil {
		session := db.Session(&gorm.Session{})
		session.Statement.ConnPool = sqlTx
		return session
	}
	return db
}

func (r *MySQLRepository) resolveTenant(ctx context.Context) string {
	tid := tenant.TenantID(ctx)
	if tid == "" {
		return "tenant-default"
	}
	return tid
}

// --- Cart Operations (Redis Hash) ---

func (r *MySQLRepository) cartKey(tid, userID string) string {
	return fmt.Sprintf("cart:%s:%s", tid, userID)
}

func (r *MySQLRepository) GetCartItems(ctx context.Context, userID string) ([]*CartItemData, error) {
	tid := r.resolveTenant(ctx)
	key := r.cartKey(tid, userID)

	vals, err := r.redisClient.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	items := make([]*CartItemData, 0, len(vals))
	for _, valStr := range vals {
		var item CartItemData
		if err := json.Unmarshal([]byte(valStr), &item); err == nil {
			items = append(items, &item)
		}
	}
	return items, nil
}

func (r *MySQLRepository) SetCartItem(ctx context.Context, userID string, item *CartItemData) error {
	tid := r.resolveTenant(ctx)
	key := r.cartKey(tid, userID)

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return r.redisClient.HSet(ctx, key, item.SkuID, string(data)).Err()
}

func (r *MySQLRepository) RemoveCartItem(ctx context.Context, userID, skuID string) error {
	tid := r.resolveTenant(ctx)
	return r.redisClient.HDel(ctx, r.cartKey(tid, userID), skuID).Err()
}

func (r *MySQLRepository) RemoveCartItems(ctx context.Context, userID string, skuIDs []string) error {
	if len(skuIDs) == 0 {
		return nil
	}
	tid := r.resolveTenant(ctx)
	return r.redisClient.HDel(ctx, r.cartKey(tid, userID), skuIDs...).Err()
}

func (r *MySQLRepository) ClearCart(ctx context.Context, userID string) error {
	tid := r.resolveTenant(ctx)
	return r.redisClient.Del(ctx, r.cartKey(tid, userID)).Err()
}

// --- Settlement Token (状态机治理与多阶段 TTL 防重模型) ---

func (r *MySQLRepository) settlementKey(tid, token string) string {
	return fmt.Sprintf("settlement:%s:%s", tid, token)
}

// Step 2.1: 保存初始待消费结算令牌 (INIT 状态, TTL 15~30m)
// 来源: OrderService.PrepareSettlement 阶段签发的结算令牌
// 用于: 记录买家归属并进入待消费初始态，覆盖用户核对地址与优惠券的人工决策时长
func (r *MySQLRepository) SaveSettlementToken(ctx context.Context, userID, token string, ttl time.Duration) error {
	tid := r.resolveTenant(ctx)
	key := r.settlementKey(tid, token)
	pipe := r.redisClient.Pipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		"status":     TokenStatusInit,
		"user_id":    userID,
		"created_at": time.Now().UnixMilli(),
	})
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Step 2.2: 原子推进结算令牌状态至 PROCESSING，支持并发拦截、宕机租约自愈与已成功幂等回显
// 来源: OrderService.CreateOrder 提单阶段传入的 token 与买家 userID
// 用于: 毫秒级阻断突发并发连击；在发现前序超时时自愈接管；在命中 SUCCESS 时零打扰回显历史订单
func (r *MySQLRepository) AcquireSettlementToken(ctx context.Context, userID, token string, leaseTimeout time.Duration) (*TokenAcquireResult, error) {
	tid := r.resolveTenant(ctx)
	key := r.settlementKey(tid, token)
	nowMs := time.Now().UnixMilli()
	leaseTimeoutMs := leaseTimeout.Milliseconds()
	if leaseTimeoutMs <= 0 {
		leaseTimeoutMs = 5000 // 默认 5s 租约，覆盖下游业务建单熔断时间
	}

	luaScript := `
		local exists = redis.call('EXISTS', KEYS[1])
		if exists == 0 then
			return {'EXPIRED', '', '', '', '', ''}
		end

		local owner = redis.call('HGET', KEYS[1], 'user_id')
		if owner ~= ARGV[1] then
			return {'FORBIDDEN', '', '', '', '', ''}
		end

		local status = redis.call('HGET', KEYS[1], 'status')
		if status == 'SUCCESS' then
			local pId = redis.call('HGET', KEYS[1], 'parent_order_id') or ''
			local pNo = redis.call('HGET', KEYS[1], 'parent_order_no') or ''
			local snap = redis.call('HGET', KEYS[1], 'order_snapshot') or ''
			return {'SUCCESS_REPLAY', pId, pNo, '', '', snap}
		end

		if status == 'FATAL_FAIL' then
			local reason = redis.call('HGET', KEYS[1], 'reason') or 'business check failed'
			return {'FATAL_FAIL', '', '', reason, '', ''}
		end

		if status == 'INIT' or status == 'RETRYABLE_FAIL' then
			redis.call('HMSET', KEYS[1], 'status', 'PROCESSING', 'lock_time', ARGV[2])
			redis.call('EXPIRE', KEYS[1], 1800)
			return {'PROCEED', '', '', '', ARGV[2], ''}
		end

		if status == 'PROCESSING' then
			local lockTime = tonumber(redis.call('HGET', KEYS[1], 'lock_time') or '0')
			local elapsed = tonumber(ARGV[2]) - lockTime
			if elapsed > tonumber(ARGV[3]) then
				-- 租约超时自愈：前序请求节点可能已宕机，允许接管重试
				redis.call('HMSET', KEYS[1], 'status', 'PROCESSING', 'lock_time', ARGV[2])
				-- 接管后必须同步刷新物理 TTL，否则 key 会沿用首次抢占时刻的过期倒计时
				redis.call('EXPIRE', KEYS[1], 1800)
				return {'PROCEED', '', '', '', ARGV[2], ''}
			else
				-- 还在处理窗口期内，阻断连击
				return {'STILL_PROCESSING', '', '', '', tostring(lockTime), ''}
			end
		end

		return {'EXPIRED', '', '', '', '', ''}
	`

	rawRes, err := r.redisClient.Eval(ctx, luaScript, []string{key}, userID, nowMs, leaseTimeoutMs).Slice()
	if err != nil {
		return nil, err
	}

	res := &TokenAcquireResult{
		Action: fmt.Sprint(rawRes[0]),
	}
	if len(rawRes) > 1 {
		res.ParentOrderID = fmt.Sprint(rawRes[1])
	}
	if len(rawRes) > 2 {
		res.ParentOrderNo = fmt.Sprint(rawRes[2])
	}
	if len(rawRes) > 3 {
		res.Reason = fmt.Sprint(rawRes[3])
	}
	if len(rawRes) > 4 {
		if ms, convErr := strconv.ParseInt(fmt.Sprint(rawRes[4]), 10, 64); convErr == nil {
			res.LockTime = ms
		}
	}
	if len(rawRes) > 5 {
		res.Snapshot = fmt.Sprint(rawRes[5])
	}
	return res, nil
}

// Step 2.2.1: 以 CAS 方式续期持有中的租约，防止长耗时建单期间租约中途过期
// 来源: 提单业务执行期间的续租心跳, 携带 AcquireSettlementToken 返回的 lockTime
// 用于: 杜绝 DTM Saga 慢链路耗时超过租约窗口后被他人接管并重复建单
func (r *MySQLRepository) RenewSettlementToken(ctx context.Context, token string, expectLockTime int64, ttl time.Duration) (int64, bool, error) {
	tid := r.resolveTenant(ctx)
	key := r.settlementKey(tid, token)
	nowMs := time.Now().UnixMilli()

	// CAS 续租: 仅当 status 仍为 PROCESSING 且 lock_time 与持有者一致时才会续期
	// 若 lock_time 已被他人接管改写，本次续租返回 LOST，持锁者立即感知自己已失去租约
	renewScript := `
		local status = redis.call('HGET', KEYS[1], 'status')
		if status ~= 'PROCESSING' then
			return {'LOST', ''}
		end

		local cur = tonumber(redis.call('HGET', KEYS[1], 'lock_time') or '0')
		if cur ~= tonumber(ARGV[2]) then
			return {'LOST', ''}
		end

		redis.call('HMSET', KEYS[1], 'lock_time', ARGV[1])
		redis.call('EXPIRE', KEYS[1], 1800)
		return {'RENEWED', ARGV[1]}
	`

	rawRes, err := r.redisClient.Eval(ctx, renewScript, []string{key}, nowMs, expectLockTime).Slice()
	if err != nil {
		return 0, false, err
	}
	if len(rawRes) == 0 || fmt.Sprint(rawRes[0]) != "RENEWED" {
		return 0, false, nil
	}
	if len(rawRes) > 1 {
		if ms, convErr := strconv.ParseInt(fmt.Sprint(rawRes[1]), 10, 64); convErr == nil {
			return ms, true, nil
		}
	}
	return nowMs, true, nil
}

// Step 2.3: 记录结算令牌建单成功 (SUCCESS 状态, 绑定订单 ID 与订单快照, TTL 24h)
// 来源: 订单与子单成功持久化落库
// 用于: 支持网络重试时直接无缝回显已创建的完整订单, 免打 MySQL 库
func (r *MySQLRepository) RecordSettlementSuccess(ctx context.Context, token, parentOrderID, parentOrderNo, snapshot string, ttl time.Duration) error {
	tid := r.resolveTenant(ctx)
	key := r.settlementKey(tid, token)
	pipe := r.redisClient.Pipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		"status":          TokenStatusSuccess,
		"parent_order_id": parentOrderID,
		"parent_order_no": parentOrderNo,
		"order_snapshot":  snapshot,
	})
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Step 2.4: 记录结算令牌建单失败 (可重试软故障 vs 硬阻断失败)
// 来源: 库存不足、限购不满足或底层网络死锁异常
// 用于: 精准分流重试权限，硬失败 5m 秒拒，软故障允许再次建单
func (r *MySQLRepository) RecordSettlementFail(ctx context.Context, token string, isFatal bool, reason string) error {
	tid := r.resolveTenant(ctx)
	key := r.settlementKey(tid, token)
	status := TokenStatusRetryableFail
	ttl := biz.SettlementTokenRetryableFailTTL
	if isFatal {
		status = TokenStatusFatalFail
		ttl = biz.SettlementTokenFatalFailTTL
	}
	pipe := r.redisClient.Pipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		"status": status,
		"reason": reason,
	})
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// Step 2.5: 兼容旧版原子单次消费接口
// 来源: 历史调用或极简测试
// 用于: 委托给 AcquireSettlementToken 状态机执行
func (r *MySQLRepository) ConsumeSettlementToken(ctx context.Context, userID, token string) (bool, error) {
	res, err := r.AcquireSettlementToken(ctx, userID, token, 5*time.Second)
	if err != nil {
		return false, err
	}
	return res.Action == "PROCEED" || res.Action == "SUCCESS_REPLAY", nil
}

// --- Order Operations ---

func (r *MySQLRepository) CreateOrder(ctx context.Context, order *orderv1.Order) (*orderv1.Order, error) {
	tid := r.resolveTenant(ctx)
	if order.TenantId != "" {
		tid = order.TenantId
	}

	orderID := order.Id
	if orderID == "" {
		orderID = "ord-" + uuid.NewString()
	}

	expireAt := time.Unix(order.ExpireAt, 0)
	if order.ExpireAt <= 0 {
		expireAt = time.Now().Add(15 * time.Minute)
	}

	m := &OrderModel{
		ID:              orderID,
		TenantID:        tid,
		OrderNo:         order.OrderNo,
		UserID:          order.UserId,
		TotalAmount:     order.TotalAmount,
		PayAmount:       order.PayAmount,
		Status:          order.Status,
		ReceiverName:    order.ReceiverName,
		ReceiverPhone:   order.ReceiverPhone,
		ReceiverAddress: order.ReceiverAddress,
		ExpireAt:        expireAt,
	}

	itemModels := make([]OrderItemModel, len(order.Items))
	for i, item := range order.Items {
		itemID := item.Id
		if itemID == "" {
			itemID = "item-" + uuid.NewString()
		}
		itemModels[i] = OrderItemModel{
			ID:        itemID,
			TenantID:  tid,
			OrderID:   orderID,
			SKUID:     item.SkuId,
			Title:     item.Title,
			Image:     item.Image,
			AttrsJSON: item.AttrsJson,
			Price:     item.Price,
			Quantity:  item.Quantity,
			Subtotal:  item.Subtotal,
		}
	}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(m).Error; err != nil {
			return err
		}
		if len(itemModels) > 0 {
			if err := tx.Create(&itemModels).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	resItems := make([]*orderv1.OrderItem, len(itemModels))
	for i := range itemModels {
		resItems[i] = itemModels[i].ToProto()
	}
	return m.ToProto(resItems), nil
}

func (r *MySQLRepository) GetOrder(ctx context.Context, orderID string) (*orderv1.Order, error) {
	tid := r.resolveTenant(ctx)
	query := r.db.WithContext(ctx).Where("id = ?", orderID)
	if tid != "" && tid != "platform" {
		query = query.Where("tenant_id = ?", tid)
	}

	var m OrderModel
	if err := query.First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}

	itemQuery := r.db.WithContext(ctx).Where("order_id = ?", orderID)
	if tid != "" && tid != "platform" {
		itemQuery = itemQuery.Where("tenant_id = ?", tid)
	}

	var items []OrderItemModel
	if err := itemQuery.Find(&items).Error; err != nil {
		return nil, err
	}
	resItems := make([]*orderv1.OrderItem, len(items))
	for i := range items {
		resItems[i] = items[i].ToProto()
	}
	return m.ToProto(resItems), nil
}

func (r *MySQLRepository) GetOrderByNo(ctx context.Context, orderNo string) (*orderv1.Order, error) {
	tid := r.resolveTenant(ctx)
	query := r.db.WithContext(ctx).Where("order_no = ?", orderNo)
	if tid != "" && tid != "platform" {
		query = query.Where("tenant_id = ?", tid)
	}

	var m OrderModel
	if err := query.First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetOrder(ctx, m.ID)
}

func (r *MySQLRepository) ListOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.Order, int64, error) {
	tid := r.resolveTenant(ctx)
	page, pageSize = paging.Normalize(page, pageSize)

	query := r.db.WithContext(ctx).Model(&OrderModel{})
	if tid != "" && tid != "platform" {
		query = query.Where("tenant_id = ?", tid)
	}
	if userID != "" {
		query = query.Where("user_id = ?", userID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var orders []OrderModel
	if err := query.Order("created_at desc").
		Offset(paging.Offset(page, pageSize)).Limit(pageSize).
		Find(&orders).Error; err != nil {
		return nil, 0, err
	}

	orderIDs := make([]string, 0, len(orders))
	for i := range orders {
		orderIDs = append(orderIDs, orders[i].ID)
	}

	itemMap, err := r.loadItemsByOrderIDs(ctx, orderIDs)
	if err != nil {
		return nil, 0, err
	}

	res := make([]*orderv1.Order, len(orders))
	for i := range orders {
		res[i] = orders[i].ToProto(itemMap[orders[i].ID])
	}
	return res, total, nil
}

func (r *MySQLRepository) UpdateOrderStatus(ctx context.Context, orderID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error) {
	tid := r.resolveTenant(ctx)
	updates := map[string]interface{}{
		"status": newStatus,
	}
	for k, v := range extraUpdates {
		updates[k] = v
	}

	query := r.getDB(ctx).Model(&OrderModel{}).Where("id = ?", orderID)
	if tid != "" && tid != "platform" {
		query = query.Where("tenant_id = ?", tid)
	}
	if oldStatus != "" {
		query = query.Where("status = ?", oldStatus)
	}

	res := query.Updates(updates)
	if res.Error != nil {
		return false, res.Error
	}
	if res.RowsAffected > 0 {
		// Step 3.4: 在同一本地事务连接内级联更新子订单，确保与主订单状态原子提交
		// 来源: 上方 CAS 胜出的主订单更新结果
		// 用于: DTM 二阶段消息 QueryPrepared 能观察到完整且一致的订单状态
		// 注意: 子订单可能归属不同商户租户，按唯一主键级联，不能再叠加买家侧租户过滤
		childQuery := r.getDB(ctx).Model(&ChildOrderModel{}).Where("id = ?", orderID)
		if err := childQuery.Updates(updates).Error; err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// Step 3.1: 在本地数据库事务中持久化主订单及其聚合的子订单与明细项
// 来源: OrderSplitter 拆单引擎计算输出的 *orderv1.ParentOrder 与 []*orderv1.ChildOrder
// 用于: 在单一事务中原子写入 parent_orders, child_orders 及 order_items 表
func (r *MySQLRepository) CreateParentOrder(ctx context.Context, parent *orderv1.ParentOrder, children []*orderv1.ChildOrder, settlementToken string) (*orderv1.ParentOrder, []*orderv1.ChildOrder, error) {
	parentID := parent.Id
	if parentID == "" {
		parentID = "pord-" + uuid.NewString()
	}

	now := time.Now()
	expireAt := now.Add(15 * time.Minute)
	if parent.ExpireAt > 0 {
		expireAt = time.Unix(parent.ExpireAt, 0)
	}

	parentModel := &ParentOrderModel{
		ID:              parentID,
		TenantID:        r.resolveTenant(ctx),
		OrderNo:         parent.OrderNo,
		UserID:          parent.UserId,
		TotalAmount:     parent.TotalAmount,
		DiscountAmount:  parent.DiscountAmount,
		PayAmount:       parent.PayAmount,
		Status:          parent.Status,
		ReceiverName:    parent.ReceiverName,
		ReceiverPhone:   parent.ReceiverPhone,
		ReceiverAddress: parent.ReceiverAddress,
		ExpireAt:        expireAt,
		CreatedAt:       now,
	}
	// 结算令牌随建单事务落库，作为 UNIQUE (tenant_id, settlement_token) 的幂等键
	// 用于: Redis 集群脑裂丢数据时由 MySQL 唯一索引兜底，阻断同一令牌重复建单
	if settlementToken != "" {
		tokenCopy := settlementToken
		parentModel.SettlementToken = &tokenCopy
	}

	childModels := make([]*ChildOrderModel, len(children))
	var allItemModels []*OrderItemModel

	for i, ch := range children {
		childID := ch.Id
		if childID == "" {
			childID = "cord-" + uuid.NewString()
		}

		childModel := &ChildOrderModel{
			ID:             childID,
			ParentID:       parentID,
			TenantID:       ch.TenantId,
			OrderNo:        ch.OrderNo,
			UserID:         ch.UserId,
			TotalAmount:    ch.TotalAmount,
			DiscountAmount: ch.DiscountAmount,
			PayAmount:      ch.PayAmount,
			Status:         ch.Status,
			CreatedAt:      now,
		}
		childModels[i] = childModel

		for _, it := range ch.Items {
			itemID := it.Id
			if itemID == "" {
				itemID = "item-" + uuid.NewString()
			}
			allItemModels = append(allItemModels, &OrderItemModel{
				ID:                itemID,
				TenantID:          ch.TenantId,
				OrderID:           childID,
				ParentOrderID:     parentID,
				SKUID:             it.SkuId,
				Title:             it.Title,
				Image:             it.Image,
				AttrsJSON:         it.AttrsJson,
				Price:             it.Price,
				Quantity:          it.Quantity,
				Subtotal:          it.Subtotal,
				OriginalPrice:     it.OriginalPrice,
				AllocatedDiscount: it.AllocatedDiscount,
				RealizedPrice:     it.RealizedPrice,
			})
		}
	}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(parentModel).Error; err != nil {
			return err
		}
		if len(childModels) > 0 {
			if err := tx.Create(&childModels).Error; err != nil {
				return err
			}
		}
		if len(allItemModels) > 0 {
			if err := tx.Create(&allItemModels).Error; err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		// 结算令牌唯一索引冲突: 同一令牌已落库成单 (租约自愈重入的极端并发场景)
		// 用于: 向上层抛出可识别的哨兵错误, 由用例层转为回显既有订单而非重复建单
		if biz.IsDuplicateSettlementToken(err) {
			return nil, nil, biz.ErrDuplicateSettlementToken
		}
		return nil, nil, err
	}

	savedChildren := make([]*orderv1.ChildOrder, len(children))
	for i, chModel := range childModels {
		var items []*orderv1.OrderItem
		for _, im := range allItemModels {
			if im.OrderID == chModel.ID {
				items = append(items, im.ToProto())
			}
		}
		savedChildren[i] = chModel.ToProto(items)
	}

	return parentModel.ToProto(savedChildren), savedChildren, nil
}

// Step 3.1.1: 批量预载子单与商品明细，消除聚合读取的 N+1 查询
// 来源: 一次聚合查询得到的 parentIDs 集合
// 用于: 以 2 条固定 SQL 取回全部子单及其明细，替代"每条主单再查子单、子单再查明细"的嵌套循环
func (r *MySQLRepository) loadChildGraph(ctx context.Context, parentIDs []string) (map[string][]*orderv1.ChildOrder, error) {
	result := make(map[string][]*orderv1.ChildOrder, len(parentIDs))
	if len(parentIDs) == 0 {
		return result, nil
	}

	var childModels []ChildOrderModel
	if err := r.db.WithContext(ctx).
		Where("parent_id IN ?", parentIDs).
		Order("created_at asc").
		Find(&childModels).Error; err != nil {
		return nil, err
	}
	if len(childModels) == 0 {
		return result, nil
	}

	childIDs := make([]string, 0, len(childModels))
	byParent := make(map[string][]*ChildOrderModel, len(childModels))
	for i := range childModels {
		cm := &childModels[i]
		childIDs = append(childIDs, cm.ID)
		byParent[cm.ParentID] = append(byParent[cm.ParentID], cm)
	}

	itemMap, err := r.loadItemsByOrderIDs(ctx, childIDs)
	if err != nil {
		return nil, err
	}

	for parentID, models := range byParent {
		children := make([]*orderv1.ChildOrder, 0, len(models))
		for _, cm := range models {
			children = append(children, cm.ToProto(itemMap[cm.ID]))
		}
		result[parentID] = children
	}
	return result, nil
}

// Step 3.1.2: 批量预载订单明细
// 来源: 一次聚合查询得到的 orderIDs 集合
// 用于: 单条 IN 查询取回全部明细，替代逐单查询
func (r *MySQLRepository) loadItemsByOrderIDs(ctx context.Context, orderIDs []string) (map[string][]*orderv1.OrderItem, error) {
	result := make(map[string][]*orderv1.OrderItem, len(orderIDs))
	if len(orderIDs) == 0 {
		return result, nil
	}

	var items []OrderItemModel
	if err := r.db.WithContext(ctx).
		Where("order_id IN ?", orderIDs).
		Find(&items).Error; err != nil {
		return nil, err
	}
	for i := range items {
		it := items[i].ToProto()
		result[it.GetOrderId()] = append(result[it.GetOrderId()], it)
	}
	return result, nil
}

// Step 3.1.3: 批量预载售后明细
// 来源: 一次列表查询得到的 afterSalesIDs 集合
// 用于: 单条 IN 查询取回全部售后明细，替代逐单查询
func (r *MySQLRepository) loadAfterSalesItems(ctx context.Context, afterSalesIDs []string) (map[string][]*orderv1.AfterSalesItem, error) {
	result := make(map[string][]*orderv1.AfterSalesItem, len(afterSalesIDs))
	if len(afterSalesIDs) == 0 {
		return result, nil
	}

	var items []*AfterSalesItemModel
	if err := r.db.WithContext(ctx).
		Where("after_sales_id IN ?", afterSalesIDs).
		Find(&items).Error; err != nil {
		return nil, err
	}
	for _, it := range items {
		result[it.AfterSalesID] = append(result[it.AfterSalesID], it.ToProto())
	}
	return result, nil
}

// Step 3.2: 根据主订单 ID 关联查询主单及全部商家子订单
// 来源: 买家在订单详情页展示或支付请求传入的 parentID
// 用于: 组装 ParentOrder 及所有 ChildOrder 与商品明细返回
func (r *MySQLRepository) GetParentOrder(ctx context.Context, parentID string) (*orderv1.ParentOrder, error) {
	tid := r.resolveTenant(ctx)
	query := r.db.WithContext(ctx).Where("id = ?", parentID)
	if tid != "" && tid != "platform" {
		query = query.Where("tenant_id = ?", tid)
	}

	var pm ParentOrderModel
	if err := query.First(&pm).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}

	graph, err := r.loadChildGraph(ctx, []string{pm.ID})
	if err != nil {
		return nil, err
	}
	return pm.ToProto(graph[pm.ID]), nil
}

// Step 3.3: 根据主订单编号查询主单
// 来源: 支付渠道回调通知
// 用于: 通过唯一 order_no 索引定位主订单
func (r *MySQLRepository) GetParentOrderByNo(ctx context.Context, orderNo string) (*orderv1.ParentOrder, error) {
	tid := r.resolveTenant(ctx)
	query := r.db.WithContext(ctx).Where("order_no = ?", orderNo)
	if tid != "" && tid != "platform" {
		query = query.Where("tenant_id = ?", tid)
	}

	var pm ParentOrderModel
	if err := query.First(&pm).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetParentOrder(ctx, pm.ID)
}

// Step 3.4: 分页检索买家名下所有主订单
// 来源: 买家个人中心查询请求
// 用于: 倒序返回主单列表
func (r *MySQLRepository) ListParentOrders(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.ParentOrder, int64, error) {
	tid := r.resolveTenant(ctx)
	page, pageSize = paging.Normalize(page, pageSize)

	query := r.db.WithContext(ctx).Model(&ParentOrderModel{})
	if tid != "" && tid != "platform" {
		query = query.Where("tenant_id = ?", tid)
	}
	if userID != "" {
		query = query.Where("user_id = ?", userID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var pms []ParentOrderModel
	if err := query.Order("created_at desc").
		Offset(paging.Offset(page, pageSize)).Limit(pageSize).
		Find(&pms).Error; err != nil {
		return nil, 0, err
	}

	parentIDs := make([]string, 0, len(pms))
	for i := range pms {
		parentIDs = append(parentIDs, pms[i].ID)
	}
	graph, err := r.loadChildGraph(ctx, parentIDs)
	if err != nil {
		return nil, 0, err
	}

	res := make([]*orderv1.ParentOrder, len(pms))
	for i := range pms {
		res[i] = pms[i].ToProto(graph[pms[i].ID])
	}
	return res, total, nil
}

// Step 3.5: 更新主订单状态并级联更新全部子订单
// 来源: 聚合支付完成通知或全单超时关单
// 用于: CAS 更新主单及子单状态为 PAID 或 CANCELLED
func (r *MySQLRepository) UpdateParentOrderStatus(ctx context.Context, parentID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error) {
	tid := r.resolveTenant(ctx)
	updates := map[string]interface{}{
		"status": newStatus,
	}
	for k, v := range extraUpdates {
		updates[k] = v
	}

	query := r.getDB(ctx).Model(&ParentOrderModel{}).Where("id = ?", parentID)
	if tid != "" && tid != "platform" {
		query = query.Where("tenant_id = ?", tid)
	}
	if oldStatus != "" {
		query = query.Where("status = ?", oldStatus)
	}

	res := query.Updates(updates)
	if res.Error != nil {
		return false, res.Error
	}
	if res.RowsAffected == 0 {
		return false, nil
	}

	// Cascade status to all child orders within the same local transaction.
	// Child orders may belong to different merchant tenants, so cascade by parent_id only.
	childUpdates := map[string]interface{}{
		"status": newStatus,
	}
	childQuery := r.getDB(ctx).Model(&ChildOrderModel{}).Where("parent_id = ?", parentID)
	if err := childQuery.Updates(childUpdates).Error; err != nil {
		return false, err
	}

	return true, nil
}

// Step 3.6: 根据子订单 ID 查询商家子订单
// 来源: 商家管理后台查询或发货操作
// 用于: 校验租户并返回子订单实体
func (r *MySQLRepository) GetChildOrder(ctx context.Context, childID string) (*orderv1.ChildOrder, error) {
	tid := r.resolveTenant(ctx)
	query := r.db.WithContext(ctx).Where("id = ?", childID)
	if tid != "" && tid != "platform" && tid != "tenant-default" {
		query = query.Where("tenant_id = ?", tid)
	}

	var cm ChildOrderModel
	if err := query.First(&cm).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}

	itemMap, err := r.loadItemsByOrderIDs(ctx, []string{cm.ID})
	if err != nil {
		return nil, err
	}
	return cm.ToProto(itemMap[cm.ID]), nil
}

// Step 3.7: 根据子订单编号查询商家子订单
// 来源: 商家物流或逆向退款根据订单号查询
// 用于: 查询定位 ChildOrder
func (r *MySQLRepository) GetChildOrderByNo(ctx context.Context, orderNo string) (*orderv1.ChildOrder, error) {
	tid := r.resolveTenant(ctx)
	var cm ChildOrderModel
	if err := r.db.WithContext(ctx).Where("tenant_id = ? AND order_no = ?", tid, orderNo).First(&cm).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}
	return r.GetChildOrder(ctx, cm.ID)
}

// Step 3.8: 分页检索商家名下所有子订单
// 来源: 商家端 AdminListOrders
// 用于: 严格按 tenant_id 隔离返回商家的子订单列表
func (r *MySQLRepository) ListChildOrders(ctx context.Context, tenantID, status string, page, pageSize int) ([]*orderv1.ChildOrder, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)

	query := r.db.WithContext(ctx).Model(&ChildOrderModel{})
	if tenantID != "" && tenantID != "platform" {
		query = query.Where("tenant_id = ?", tenantID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var cms []ChildOrderModel
	if err := query.Order("created_at desc").
		Offset(paging.Offset(page, pageSize)).Limit(pageSize).
		Find(&cms).Error; err != nil {
		return nil, 0, err
	}

	childIDs := make([]string, 0, len(cms))
	for i := range cms {
		childIDs = append(childIDs, cms[i].ID)
	}
	itemMap, err := r.loadItemsByOrderIDs(ctx, childIDs)
	if err != nil {
		return nil, 0, err
	}

	res := make([]*orderv1.ChildOrder, len(cms))
	for i := range cms {
		res[i] = cms[i].ToProto(itemMap[cms[i].ID])
	}
	return res, total, nil
}

// Step 3.9: 商家单独更新子订单履约状态
// 来源: 商家端发货或退货
// 用于: 更新子订单 status/carrier/tracking_no
func (r *MySQLRepository) UpdateChildOrderStatus(ctx context.Context, childID, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error) {
	tid := r.resolveTenant(ctx)
	updates := map[string]interface{}{
		"status": newStatus,
	}
	for k, v := range extraUpdates {
		updates[k] = v
	}

	query := r.db.WithContext(ctx).Model(&ChildOrderModel{}).Where("id = ?", childID)
	if tid != "" && tid != "platform" && tid != "tenant-default" {
		query = query.Where("tenant_id = ?", tid)
	}
	if oldStatus != "" {
		query = query.Where("status = ?", oldStatus)
	}

	res := query.Updates(updates)
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

// Step 3.10: 售后单落库持久化 (CreateAfterSales)
// 来源: 买家提交售后申请
// 用于: 事务写入 after_sales_orders 与 after_sales_items
func (r *MySQLRepository) CreateAfterSales(ctx context.Context, as *orderv1.AfterSalesOrder) (*orderv1.AfterSalesOrder, error) {
	now := time.Now()
	asID := as.Id
	if asID == "" {
		asID = "as-" + uuid.NewString()
	}
	asNo := as.AfterSalesNo
	if asNo == "" {
		asNo = fmt.Sprintf("AS-%s-%d", as.ChildOrderId, now.Unix()%1000000)
	}

	// Step 3.10.1: 售后单归属商家租户，以服务层由子订单派生的值为准
	// 来源: order.go 中 child.TenantId 派生出的 as.TenantId（客户端无法伪造）
	// 用于: Step 3.10.2 落库；缺省时才回落到 ctx 中的租户
	tid := as.TenantId
	if tid == "" {
		tid = r.resolveTenant(ctx)
	}

	model := &AfterSalesModel{
		ID:               asID,
		AfterSalesNo:     asNo,
		ChildOrderID:     as.ChildOrderId,
		ChildOrderNo:     as.ChildOrderNo,
		ParentOrderID:    as.ParentOrderId,
		TenantID:         tid,
		UserID:           as.UserId,
		Type:             as.Type,
		Status:           as.Status,
		RefundAmount:     as.RefundAmount,
		Reason:           as.Reason,
		RejectReason:     as.RejectReason,
		ReturnCarrier:    as.ReturnCarrier,
		ReturnTrackingNo: as.ReturnTrackingNo,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	var itemModels []*AfterSalesItemModel
	for _, it := range as.Items {
		itemModels = append(itemModels, &AfterSalesItemModel{
			ID:            "asi-" + uuid.NewString(),
			TenantID:      tid,
			AfterSalesID:  asID,
			SKUID:         it.SkuId,
			Quantity:      it.Quantity,
			RealizedPrice: it.RealizedPrice,
		})
	}

	// Step 3.10.2: 开启本地事务落库售后单主表与明细项
	// 来源: Step 3.10.1 构造的 model 与 itemModels
	// 用于: 原子持久化售后单记录与退货退款明细，供后续商家审核与履约
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(model).Error; err != nil {
			return err
		}
		if len(itemModels) > 0 {
			if err := tx.Create(&itemModels).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return model.ToProto(as.Items), nil
}

// Step 3.11: 按主键 ID 查询售后详情 (GetAfterSales)
// 注意: 售后单是设计上就跨租户的聚合 —— 买家(商城租户)发起、商家(店铺租户)处理。
// 归属校验由服务层按用户所有权 + 商家租户完成 (见 order.go)，此处按 id 直取。
func (r *MySQLRepository) GetAfterSales(ctx context.Context, id string) (*orderv1.AfterSalesOrder, error) {
	var m AfterSalesModel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAfterSalesNotFound
		}
		return nil, err
	}
	itemMap, err := r.loadAfterSalesItems(ctx, []string{m.ID})
	if err != nil {
		return nil, err
	}
	return m.ToProto(itemMap[m.ID]), nil
}

// Step 3.12: 按售后单号查询详情 (GetAfterSalesByNo)
func (r *MySQLRepository) GetAfterSalesByNo(ctx context.Context, afterSalesNo string) (*orderv1.AfterSalesOrder, error) {
	var m AfterSalesModel
	if err := r.db.WithContext(ctx).Where("after_sales_no = ?", afterSalesNo).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrAfterSalesNotFound
		}
		return nil, err
	}
	return r.GetAfterSales(ctx, m.ID)
}

// Step 3.13: 原子流转售后状态 (UpdateAfterSalesStatus)
func (r *MySQLRepository) UpdateAfterSalesStatus(ctx context.Context, id, oldStatus, newStatus string, extraUpdates map[string]interface{}) (bool, error) {
	updates := map[string]interface{}{
		"status":     newStatus,
		"updated_at": time.Now(),
	}
	for k, v := range extraUpdates {
		updates[k] = v
	}

	query := r.db.WithContext(ctx).Model(&AfterSalesModel{}).Where("id = ?", id)
	if oldStatus != "" {
		query = query.Where("status = ?", oldStatus)
	}

	res := query.Updates(updates)
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

// Step 3.14: 买家查询售后记录 (ListAfterSales)
func (r *MySQLRepository) ListAfterSales(ctx context.Context, userID, status string, page, pageSize int) ([]*orderv1.AfterSalesOrder, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)

	var total int64
	query := r.db.WithContext(ctx).Model(&AfterSalesModel{})
	if userID != "" {
		query = query.Where("user_id = ?", userID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var models []*AfterSalesModel
	if err := query.Order("created_at DESC").
		Offset(paging.Offset(page, pageSize)).Limit(pageSize).
		Find(&models).Error; err != nil {
		return nil, 0, err
	}

	asIDs := make([]string, 0, len(models))
	for _, m := range models {
		asIDs = append(asIDs, m.ID)
	}
	itemMap, err := r.loadAfterSalesItems(ctx, asIDs)
	if err != nil {
		return nil, 0, err
	}

	result := make([]*orderv1.AfterSalesOrder, len(models))
	for i, m := range models {
		result[i] = m.ToProto(itemMap[m.ID])
	}
	return result, total, nil
}

// Step 3.15: 商家查询售后记录 (AdminListAfterSales)
func (r *MySQLRepository) AdminListAfterSales(ctx context.Context, tenantID, status string, page, pageSize int) ([]*orderv1.AfterSalesOrder, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)

	var total int64
	query := r.db.WithContext(ctx).Model(&AfterSalesModel{})
	if tenantID != "" && tenantID != "platform" {
		query = query.Where("tenant_id = ?", tenantID)
	}
	if status != "" {
		query = query.Where("status = ?", status)
	}

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var models []*AfterSalesModel
	if err := query.Order("created_at DESC").
		Offset(paging.Offset(page, pageSize)).Limit(pageSize).
		Find(&models).Error; err != nil {
		return nil, 0, err
	}

	asIDs := make([]string, 0, len(models))
	for _, m := range models {
		asIDs = append(asIDs, m.ID)
	}
	itemMap, err := r.loadAfterSalesItems(ctx, asIDs)
	if err != nil {
		return nil, 0, err
	}

	result := make([]*orderv1.AfterSalesOrder, len(models))
	for i, m := range models {
		result[i] = m.ToProto(itemMap[m.ID])
	}
	return result, total, nil
}

// --- Order Expiration Queue (Redis Sorted Set - ADR 0009) ---

// Step 4.1: 将待支付订单添加至 Redis ZSet 超时关单延迟队列 (ADR 0009)
// 来源: CreateOrder / CreateParentOrder 成功创建的订单 ID 与过期时间戳 expireAt
// 用于: 超时关单协程通过 ZRANGEBYSCORE 定时拉取到期订单
func (r *MySQLRepository) AddOrderExpiration(ctx context.Context, orderID string, expireAt int64) error {
	if r.redisClient == nil {
		return nil
	}
	return r.redisClient.ZAdd(ctx, OrderExpirationZSetKey, redis.Z{
		Score:  float64(expireAt),
		Member: orderID,
	}).Err()
}

// Step 4.2: 从 Redis ZSet 延迟队列中轮询拉取当前已过期的订单 ID 列表 (ADR 0009)
// 来源: 超时关单后台 Goroutine 传入的当前时间戳 maxTimestamp 与批次数量 limit
// 用于: 毫秒级按时间戳范围获取前 N 笔到期未支付订单，零 MySQL 查表压力
func (r *MySQLRepository) PollExpiredOrders(ctx context.Context, maxTimestamp int64, limit int64) ([]string, error) {
	if r.redisClient == nil {
		return nil, nil
	}
	return r.redisClient.ZRangeByScore(ctx, OrderExpirationZSetKey, &redis.ZRangeBy{
		Min:    "0",
		Max:    strconv.FormatInt(maxTimestamp, 10),
		Offset: 0,
		Count:  limit,
	}).Result()
}

// Step 4.3: 从 Redis ZSet 延迟队列中移除指定订单 (ADR 0009)
// 来源: PayOrder 支付成功、CancelOrder 主动取消或超时关单处理完成传入的 orderID
// 用于: 防止已支付或已取消的订单被重复扫描与处理
func (r *MySQLRepository) RemoveOrderExpiration(ctx context.Context, orderID string) error {
	if r.redisClient == nil {
		return nil
	}
	return r.redisClient.ZRem(ctx, OrderExpirationZSetKey, orderID).Err()
}
