package data

import (
	"context"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"

	"order/internal/biz"
	"order/internal/pkg/tenant"
)

const DefaultTimeoutCancelReason = "order payment timed out (15 mins)"

// OrderExpirationWorker 基于 Redis ZSet 实现的后台超时关单引擎，实现 kratos.Server 接口
type OrderExpirationWorker struct {
	repo      biz.OrderRepo
	fulfiller biz.Fulfiller
	log       *log.Helper
	interval  time.Duration
	batchSize int64
	stopCh    chan struct{}
	wg        sync.WaitGroup
	mu        sync.Mutex
	running   bool
}

// Step 0.1: 构造 OrderExpirationWorker 实例
func NewExpirationWorker(
	repo biz.OrderRepo,
	fulfiller biz.Fulfiller,
	logger log.Logger,
) *OrderExpirationWorker {
	return &OrderExpirationWorker{
		repo:      repo,
		fulfiller: fulfiller,
		log:       log.NewHelper(logger),
		interval:  1 * time.Second,
		batchSize: 100,
		stopCh:    make(chan struct{}),
	}
}

// Step 1.1: 启动后台超时关单轮询任务 (实现 kratos.Server 规范)
func (w *OrderExpirationWorker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = true
	w.stopCh = make(chan struct{})
	w.mu.Unlock()

	w.wg.Add(1)
	go w.runLoop(ctx)
	w.log.Infof("==> [Kratos Server] OrderExpirationWorker started (interval=%v)", w.interval)
	return nil
}

// Step 1.2: 优雅停止超时关单后台任务 (实现 kratos.Server 规范)
func (w *OrderExpirationWorker) Stop(ctx context.Context) error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	close(w.stopCh)
	w.mu.Unlock()

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.log.Info("==> [Kratos Server] OrderExpirationWorker stopped cleanly")
	case <-ctx.Done():
		w.log.Warn("==> [Kratos Server] OrderExpirationWorker stop timed out")
	}
	return nil
}

// Step 2.1: 轮询扫描循环
func (w *OrderExpirationWorker) runLoop(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.processExpiredOrders(ctx)
		}
	}
}

// Step 3.1: 批量处理到期未支付订单
func (w *OrderExpirationWorker) processExpiredOrders(ctx context.Context) {
	now := time.Now().Unix()
	expiredIDs, err := w.repo.PollExpiredOrders(ctx, now, w.batchSize)
	if err != nil || len(expiredIDs) == 0 {
		return
	}

	for _, id := range expiredIDs {
		w.cancelSingleExpiredOrder(ctx, id)
	}
}

// Step 4.1: 执行单笔超时关单并释放库存
func (w *OrderExpirationWorker) cancelSingleExpiredOrder(ctx context.Context, orderID string) {
	// 1. 尝试以主订单关单
	if parent, err := w.repo.GetParentOrder(ctx, orderID); err == nil && parent != nil {
		if parent.Status == "PENDING_PAYMENT" {
			success, err := w.repo.UpdateParentOrderStatus(ctx, parent.Id, "PENDING_PAYMENT", "CANCELLED", map[string]interface{}{
				"cancel_reason": DefaultTimeoutCancelReason,
			})
			if err == nil && success {
				if w.fulfiller != nil {
					_ = w.fulfiller.ReleaseHold(ctx, biz.FromParent(parent), DefaultTimeoutCancelReason)
				}
				w.log.Infof("parent order %s automatically cancelled due to timeout", parent.OrderNo)
			}
		}
		_ = w.repo.RemoveOrderExpiration(ctx, parent.Id)
		return
	}

	// 2. 回落为单店模式关单
	order, err := w.repo.GetOrder(ctx, orderID)
	if err == nil && order != nil {
		if order.Status == "PENDING_PAYMENT" {
			ctxWithTenant := tenant.WithTenantID(ctx, order.TenantId)
			success, err := w.repo.UpdateOrderStatus(ctxWithTenant, order.Id, "PENDING_PAYMENT", "CANCELLED", map[string]interface{}{
				"cancel_reason": DefaultTimeoutCancelReason,
			})
			if err == nil && success {
				if w.fulfiller != nil {
					_ = w.fulfiller.ReleaseHold(ctxWithTenant, biz.FromSingle(order), DefaultTimeoutCancelReason)
				}
				w.log.Infof("single order %s automatically cancelled due to timeout", order.OrderNo)
			}
		}
		_ = w.repo.RemoveOrderExpiration(ctx, order.Id)
	}
}
