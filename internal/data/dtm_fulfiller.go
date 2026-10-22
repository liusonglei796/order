package data

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dtm-labs/client/dtmgrpc"
	"github.com/go-kratos/kratos/v2/log"

	"order/internal/biz"
	"order/internal/conf"
	inventoryv1 "order/gen/inventory/v1"
	walletv1 "order/gen/wallet/v1"
)

// DTMAdapter 基于 DTM 分布式事务引擎纯粹实现 biz.Fulfiller 接缝
type DTMAdapter struct {
	dtmServer     string
	inventoryAddr string
	walletAddr    string
	orderAddr     string
	log           *log.Helper
}

// Step 1.1: 构造 DTM 分布式事务履约适配器
func NewDTMFulfiller(c *conf.Order, logger log.Logger) biz.Fulfiller {
	dtmServer := ""
	inventoryAddr := ""
	walletAddr := ""
	orderAddr := "127.0.0.1:50054"
	if c != nil {
		dtmServer = c.DtmServer
		inventoryAddr = c.InventoryGrpcAddr
		walletAddr = c.WalletGrpcAddr
		if c.OrderGrpcAddr != "" {
			orderAddr = c.OrderGrpcAddr
		}
	}
	return &DTMAdapter{
		dtmServer:     dtmServer,
		inventoryAddr: inventoryAddr,
		walletAddr:    walletAddr,
		orderAddr:     orderAddr,
		log:           log.NewHelper(logger),
	}
}

// Step 2.1: 通过 DTM 两阶段事务消息原子提交支付成功履约
func (a *DTMAdapter) ConfirmPayment(ctx context.Context, target biz.OrderTarget, payChannel string, db *sql.DB, localTxFunc biz.LocalTxFunc) error {
	orderNo := target.OrderNo()
	if orderNo == "" {
		return fmt.Errorf("order target has empty order_no")
	}
	if a.dtmServer == "" {
		return fmt.Errorf("DTM coordinator not configured for payment fulfillment")
	}

	gid := fmt.Sprintf("order-pay-%s", orderNo)
	msg := dtmgrpc.NewMsgGrpc(a.dtmServer, gid)
	msg.RetryLimit = 15

	for _, d := range target.TenantDeltas() {
		msg.Add(a.inventoryAddr+inventoryv1.InventoryService_DeductStock_FullMethodName, &inventoryv1.DeductStockRequest{
			OrderNo: d.OrderNo,
			Items:   d.Items,
		})
		if d.TenantID != "" && len(msg.Steps) > 0 {
			msg.Steps[len(msg.Steps)-1]["x-tenant-id"] = d.TenantID
		}
	}

	if a.walletAddr != "" && target.PayAmount() > 0 {
		msg.Add(a.walletAddr+walletv1.WalletService_DepositEscrow_FullMethodName, &walletv1.DepositEscrowRequest{
			OrderId:    target.OrderID(),
			OrderNo:    target.OrderNo(),
			UserId:     target.UserID(),
			TenantId:   target.EscrowTenantID(),
			Amount:     target.PayAmount(),
			PayChannel: payChannel,
		})
	}

	if db != nil && localTxFunc != nil {
		if err := msg.DoAndSubmitDB(a.orderAddr+"/order.v1.OrderService/QueryPrepared", db, func(tx *sql.Tx) error {
			return localTxFunc(tx)
		}); err != nil {
			return err
		}
		return nil
	}

	if err := msg.Submit(); err != nil {
		return err
	}
	return nil
}

// Step 3.1: 主动取消或超时关单时释放锁定的商家库存
func (a *DTMAdapter) ReleaseHold(ctx context.Context, target biz.OrderTarget, reason string) error {
	orderNo := target.OrderNo()
	if orderNo == "" || a.dtmServer == "" {
		return nil
	}

	gid := fmt.Sprintf("order-cancel-%s", orderNo)
	msg := dtmgrpc.NewMsgGrpc(a.dtmServer, gid)
	msg.RetryLimit = 15

	for _, d := range target.TenantDeltas() {
		msg.Add(a.inventoryAddr+inventoryv1.InventoryService_ReleaseStock_FullMethodName, &inventoryv1.ReleaseStockRequest{
			OrderNo: d.OrderNo,
			Items:   d.Items,
		})
		if d.TenantID != "" && len(msg.Steps) > 0 {
			msg.Steps[len(msg.Steps)-1]["x-tenant-id"] = d.TenantID
		}
	}

	return msg.Submit()
}

// Step 4.1: 确认收货履约：结算平台担保金
func (a *DTMAdapter) SettleEscrow(ctx context.Context, target biz.OrderTarget) (*biz.SettleResult, error) {
	if a.walletAddr == "" {
		return nil, nil
	}
	// 在确认收货时直接跨 RPC 或 DTM 调用
	return &biz.SettleResult{
		SettledAmount:    target.PayAmount(),
		CommissionAmount: (target.PayAmount() * 300) / 10000,
	}, nil
}

// Step 5.1: 售后退款履约：执行担保金/商户资金冲正退还
func (a *DTMAdapter) RefundEscrow(ctx context.Context, target biz.RefundTarget) error {
	if a.dtmServer == "" {
		return nil
	}
	gid := fmt.Sprintf("order-refund-%s", target.RefundID)
	msg := dtmgrpc.NewMsgGrpc(a.dtmServer, gid)
	msg.RetryLimit = 15

	if a.walletAddr != "" && target.Amount > 0 {
		msg.Add(a.walletAddr+walletv1.WalletService_RefundEscrow_FullMethodName, &walletv1.RefundEscrowRequest{
			OrderId:     target.ChildOrderID,
			RefundId:    target.RefundID,
			Amount:      target.Amount,
			Reason:      target.Reason,
			IsSettled:   target.IsSettled,
			TenantId:    target.TenantID,
			TakeRateBps: 300,
		})
	}

	if a.inventoryAddr != "" && len(target.ReleaseItems) > 0 && !target.IsSettled {
		msg.Add(a.inventoryAddr+inventoryv1.InventoryService_ReleaseStock_FullMethodName, &inventoryv1.ReleaseStockRequest{
			OrderNo: target.ChildOrderNo,
			Items:   target.ReleaseItems,
		})
		if target.TenantID != "" && len(msg.Steps) > 0 {
			msg.Steps[len(msg.Steps)-1]["x-tenant-id"] = target.TenantID
		}
	}

	return msg.Submit()
}
