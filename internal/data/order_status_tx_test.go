package data

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"order/internal/biz"
	"order/internal/pkg/tenant"
)

// Step 0.1: 构造独立的 SQLite 内存订单仓储
// 来源: 单元测试传入的唯一内存库名称
// 用于: 验证主/子订单状态级联更新是否受本地事务边界约束
func newOrderStatusTestRepo(t *testing.T, name string) (*MySQLRepository, *gorm.DB) {
	t.Helper()

	dsn := fmt.Sprintf("file:%s_%d?mode=memory&cache=shared", name, time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&ParentOrderModel{}, &ChildOrderModel{}))

	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	return &MySQLRepository{db: db}, db
}

// Step 1: 验证主订单状态与子订单状态在同一事务内提交或回滚
// 来源: PayOrder 通过 DTM 子事务屏障注入的本地 *sql.Tx
// 用于: 防止级联子单更新逃逸事务，造成 QueryPrepared 观察到半提交状态
func TestUpdateParentOrderStatus_CascadesInsideLocalTx(t *testing.T) {
	repo, db := newOrderStatusTestRepo(t, "memdb_order_status_tx")
	ctx := tenant.WithTenantID(context.Background(), "tenant-order")

	require.NoError(t, db.Create(&ParentOrderModel{
		ID:       "parent-tx-001",
		TenantID: "tenant-order",
		OrderNo:  "PORD-TX-001",
		UserID:   "user-tx",
		Status:   "PENDING_PAYMENT",
	}).Error)
	require.NoError(t, db.Create(&ChildOrderModel{
		ID:       "child-tx-001",
		ParentID: "parent-tx-001",
		TenantID: "merchant-other",
		OrderNo:  "CORD-TX-001",
		UserID:   "user-tx",
		Status:   "PENDING_PAYMENT",
	}).Error)

	// Step 1.1: 在显式事务内推进主订单状态
	// 来源: 模拟 DTM DoAndSubmitDB 的 busiCall
	// 用于: 触发主单 CAS 与子单级联更新
	sqlDB := repo.GetSQLDB()
	require.NotNil(t, sqlDB)
	tx, err := sqlDB.Begin()
	require.NoError(t, err)

	ctxTx := biz.WithTx(ctx, tx)
	ok, err := repo.UpdateParentOrderStatus(ctxTx, "parent-tx-001", "PENDING_PAYMENT", "PAID", nil)
	require.NoError(t, err)
	require.True(t, ok)

	// Step 1.2: 回滚本地事务
	// 来源: 模拟 busiCall 返回错误后 DTM 子事务屏障的回滚
	// 用于: 证明主单与子单状态同时回滚，无半提交残留
	require.NoError(t, tx.Rollback())

	var parent ParentOrderModel
	require.NoError(t, db.Where("id = ?", "parent-tx-001").First(&parent).Error)
	assert.Equal(t, "PENDING_PAYMENT", parent.Status)

	var child ChildOrderModel
	require.NoError(t, db.Where("id = ?", "child-tx-001").First(&child).Error)
	assert.Equal(t, "PENDING_PAYMENT", child.Status, "子订单级联更新必须跟随本地事务回滚")

	// Step 1.3: 重新在已提交事务内推进状态
	// 来源: 重试后的 DTM 本地事务
	// 用于: 验证跨商户租户的子订单也能按 parent_id 正确级联为 PAID
	tx, err = sqlDB.Begin()
	require.NoError(t, err)
	ok, err = repo.UpdateParentOrderStatus(biz.WithTx(ctx, tx), "parent-tx-001", "PENDING_PAYMENT", "PAID", nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Where("id = ?", "parent-tx-001").First(&parent).Error)
	assert.Equal(t, "PAID", parent.Status)
	require.NoError(t, db.Where("id = ?", "child-tx-001").First(&child).Error)
	assert.Equal(t, "PAID", child.Status, "跨商户子订单必须随主订单级联更新")
}
