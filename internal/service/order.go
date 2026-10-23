package service

import (
	"context"
	"errors"
	"strings"

	"github.com/dtm-labs/client/dtmcli"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"order/internal/biz"
	orderv1 "order/gen/order/v1"
	"order/internal/pkg/authz"
	"order/internal/pkg/paging"
	"order/internal/pkg/tenant"
)

type OrderService struct {
	orderv1.UnimplementedOrderServiceServer
	uc  *biz.OrderUsecase
	log *log.Helper
}

// Step 0.1: 构造 OrderService gRPC 服务门面实例
// 来源: Wire 依赖注入 OrderUsecase 与 logger
// 用于: 提供订单全生命周期、购物车与售后的 gRPC 接口实现
func NewOrderService(uc *biz.OrderUsecase, logger log.Logger) *OrderService {
	return &OrderService{
		uc:  uc,
		log: log.NewHelper(logger),
	}
}

// Step 0.2: 从上下文解析并强制校验买家 UserID
// 来源: Envoy ext_authz 注入的元数据 Header (x-user-id) 或调用方显式入参
// 用于: 校验买家会话合法性，防范伪造与越权调用
func (s *OrderService) requireUserID(ctx context.Context, fallback ...string) (string, error) {
	if uid, ok := authz.CallerID(ctx); ok && strings.TrimSpace(uid) != "" {
		return uid, nil
	}
	for _, fb := range fallback {
		if strings.TrimSpace(fb) != "" {
			return strings.TrimSpace(fb), nil
		}
	}
	return "", status.Error(codes.Unauthenticated, "authentication required: missing x-user-id")
}

// Step 0.3: 校验商户租户隔离权限
// 来源: 上下文租户标头 (x-tenant-id) 与目标资源所属租户
// 用于: 严格限制跨商户越权操作
func (s *OrderService) requireMerchantTenant(ctx context.Context, resourceTenantID string) error {
	callerTenant := tenant.TenantID(ctx)
	if callerTenant == "" || callerTenant == "platform" {
		return nil
	}
	if resourceTenantID != "" && callerTenant != resourceTenantID {
		return status.Error(codes.PermissionDenied, "cannot operate resource belonging to another tenant")
	}
	return nil
}

// Step 1.1: 添加商品到购物车
// 来源: 买家在前端商品页发起的加购请求 orderv1.AddCartItemRequest
// 用于: 校验参数并调用 OrderUsecase 持久化购物车项
func (s *OrderService) AddCartItem(ctx context.Context, req *orderv1.AddCartItemRequest) (*orderv1.AddCartItemResponse, error) {
	skuID := strings.TrimSpace(req.GetSkuId())
	if skuID == "" {
		return nil, status.Error(codes.InvalidArgument, "sku_id is required")
	}
	if req.GetQuantity() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "quantity must be greater than zero")
	}

	userID, err := s.requireUserID(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.uc.AddCartItem(ctx, userID, skuID, req.GetQuantity()); err != nil {
		return nil, status.Errorf(codes.Internal, "add cart item failed: %v", err)
	}

	return &orderv1.AddCartItemResponse{Success: true}, nil
}

// Step 1.2: 查询买家购物车详情与实时动态计价
// 来源: 买家访问购物车页面发起的 orderv1.GetCartRequest
// 用于: 聚合商品微服务实时价格与上下架状态并返回计算总额
func (s *OrderService) GetCart(ctx context.Context, req *orderv1.GetCartRequest) (*orderv1.GetCartResponse, error) {
	userID, err := s.requireUserID(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := s.uc.GetCart(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get cart failed: %v", err)
	}
	return resp, nil
}

// Step 1.3: 更新购物车条目数量或选中状态
// 来源: 买家在购物车界面勾选或调整数量发起的 orderv1.UpdateCartItemRequest
// 用于: 更新 Redis 购物车项数据
func (s *OrderService) UpdateCartItem(ctx context.Context, req *orderv1.UpdateCartItemRequest) (*orderv1.UpdateCartItemResponse, error) {
	skuID := strings.TrimSpace(req.GetSkuId())
	if skuID == "" {
		return nil, status.Error(codes.InvalidArgument, "sku_id is required")
	}
	if req.GetQuantity() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "quantity must be greater than zero")
	}

	userID, err := s.requireUserID(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.uc.UpdateCartItem(ctx, userID, skuID, req.GetQuantity(), req.GetSelected()); err != nil {
		return nil, status.Errorf(codes.Internal, "update cart item failed: %v", err)
	}

	return &orderv1.UpdateCartItemResponse{Success: true}, nil
}

// Step 1.4: 移除购物车指定条目
// 来源: 买家移除商品发起的 orderv1.RemoveCartItemRequest
// 用于: 从购物车中清理指定 SKU
func (s *OrderService) RemoveCartItem(ctx context.Context, req *orderv1.RemoveCartItemRequest) (*orderv1.RemoveCartItemResponse, error) {
	skuID := strings.TrimSpace(req.GetSkuId())
	if skuID == "" {
		return nil, status.Error(codes.InvalidArgument, "sku_id is required")
	}

	userID, err := s.requireUserID(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.uc.RemoveCartItem(ctx, userID, skuID); err != nil {
		return nil, status.Errorf(codes.Internal, "remove cart item failed: %v", err)
	}

	return &orderv1.RemoveCartItemResponse{Success: true}, nil
}

// Step 1.5: 清空买家购物车
// 来源: 买家一键清空或提单后清空发起的 orderv1.ClearCartRequest
// 用于: 删除买家全部购物车数据
func (s *OrderService) ClearCart(ctx context.Context, req *orderv1.ClearCartRequest) (*orderv1.ClearCartResponse, error) {
	userID, err := s.requireUserID(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.uc.ClearCart(ctx, userID); err != nil {
		return nil, status.Errorf(codes.Internal, "clear cart failed: %v", err)
	}

	return &orderv1.ClearCartResponse{Success: true}, nil
}

// Step 2.1: 提交订单 (核心建单与跨店拆单)
// 来源: 买家点击"提交订单"发起的 orderv1.CreateOrderRequest
// 用于: 跨店拆单、库存原子预占，持久化主子订单拓扑并写入超时关单延迟队列
func (s *OrderService) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	userID, err := s.requireUserID(ctx, "")
	if err != nil {
		return nil, err
	}

	resp, err := s.uc.CreateOrder(ctx, req, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create order failed: %v", err)
	}
	return resp, nil
}

// Step 4.1: 订单支付确认 (Mock 渠道或 Webhook 回调驱动)
// 来源: 买家发起支付或支付中心异步回调 orderv1.PayOrderRequest
// 用于: CAS 流转订单状态至 PAID，并触发库存实扣与担保入账
func (s *OrderService) PayOrder(ctx context.Context, req *orderv1.PayOrderRequest) (*orderv1.PayOrderResponse, error) {
	targetID := strings.TrimSpace(req.GetId())
	if targetID == "" {
		return nil, status.Error(codes.InvalidArgument, "order id is required")
	}

	resp, err := s.uc.PayOrder(ctx, targetID, req.GetPayChannel())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "pay order failed: %v", err)
	}
	return resp, nil
}

// Step 4.2: DTM 二阶段消息 QueryPrepared 回调
// 来源: Step 4.1 PayOrder 本地事务异常时 DTM 协调器发起的 gRPC 反查请求
// 用于: 将子事务屏障判定结果映射为 gRPC 状态码，未提交时以 Aborted 通知 DTM 回滚
func (s *OrderService) QueryPrepared(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	if err := s.uc.QueryPrepared(ctx); err != nil {
		if errors.Is(err, dtmcli.ErrFailure) {
			return nil, status.Error(codes.Aborted, err.Error())
		}
		s.log.WithContext(ctx).Errorf("QueryPrepared error: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}

// Step 5.1: 查询单笔订单详情
// 来源: 买家或平台查询订单请求 orderv1.GetOrderRequest
// 用于: 返回订单详情实体，校验买家资源所有权（防同店家内的跨买家水平越权）
func (s *OrderService) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	ord, err := s.uc.GetOrder(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "order not found: %v", err)
	}

	// Step 5.1.1: 显式校验买家所有权 (显式比对 callerID 与 ord.UserId，平台与管理面特权放行)
	// 来源: ord.GetUserId() 与上下文 x-user-id
	// 用于: 彻底拦截同店家内的水平越权，确保买家只能看自己的订单
	if err := authz.RequireOwnership(ctx, ord.GetUserId(), authz.PermOrderRead); err != nil {
		return nil, err
	}

	return &orderv1.GetOrderResponse{Order: ord}, nil
}

// Step 5.2: 买家查询自身订单列表
// 来源: 买家中心历史订单列表查询 orderv1.ListOrdersRequest
// 用于: 校验买家权限并返回分页订单列表
func (s *OrderService) ListOrders(ctx context.Context, req *orderv1.ListOrdersRequest) (*orderv1.ListOrdersResponse, error) {
	userID, err := s.requireUserID(ctx, "")
	if err != nil {
		return nil, err
	}

	page, pageSize := paging.FromProto(req.GetPage(), req.GetPageSize())
	orders, total, err := s.uc.ListOrders(ctx, userID, req.GetStatus(), page, pageSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list orders failed: %v", err)
	}

	return &orderv1.ListOrdersResponse{
		Orders: orders,
		Total:  total,
	}, nil
}

// Step 5.3: 买家查询主订单详情
// 来源: 买家合并支付聚合订单详情 orderv1.GetParentOrderRequest
// 用于: 返回包含下辖子订单图谱的主订单详情，校验买家资源所有权
func (s *OrderService) GetParentOrder(ctx context.Context, req *orderv1.GetParentOrderRequest) (*orderv1.GetParentOrderResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	po, err := s.uc.GetParentOrder(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "parent order not found: %v", err)
	}

	// Step 5.3.1: 显式校验主订单买家所有权
	// 来源: po.GetUserId() 与上下文 x-user-id
	// 用于: 拦截跨买家偷看他人主订单图谱
	if err := authz.RequireOwnership(ctx, po.GetUserId(), authz.PermOrderRead); err != nil {
		return nil, err
	}

	return &orderv1.GetParentOrderResponse{ParentOrder: po}, nil
}

// Step 5.4: 买家查询主订单列表
// 来源: 买家维度聚合支付订单列表 orderv1.ListParentOrdersRequest
// 用于: 返回买家主订单分页列表
func (s *OrderService) ListParentOrders(ctx context.Context, req *orderv1.ListParentOrdersRequest) (*orderv1.ListParentOrdersResponse, error) {
	userID, err := s.requireUserID(ctx, "")
	if err != nil {
		return nil, err
	}

	page, pageSize := paging.FromProto(req.GetPage(), req.GetPageSize())
	pos, total, err := s.uc.ListParentOrders(ctx, userID, req.GetStatus(), page, pageSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list parent orders failed: %v", err)
	}

	return &orderv1.ListParentOrdersResponse{
		ParentOrders: pos,
		Total:        total,
	}, nil
}

// Step 5.5: 商户查询租户所有订单
// 来源: 商户管理端订单列表查询 orderv1.AdminListOrdersRequest
// 用于: 根据租户身份返回本商户下辖的订单列表
func (s *OrderService) AdminListOrders(ctx context.Context, req *orderv1.AdminListOrdersRequest) (*orderv1.AdminListOrdersResponse, error) {
	page, pageSize := paging.FromProto(req.GetPage(), req.GetPageSize())
	orders, total, err := s.uc.ListOrders(ctx, "", req.GetStatus(), page, pageSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "admin list orders failed: %v", err)
	}

	return &orderv1.AdminListOrdersResponse{
		Orders: orders,
		Total:  total,
	}, nil
}

// Step 6.1: 商户发货履约
// 来源: 商户管理端发货录入 orderv1.AdminShipOrderRequest (carrier, tracking_no)
// 用于: CAS 流转子订单至 SHIPPED 并更新物流追踪号
func (s *OrderService) AdminShipOrder(ctx context.Context, req *orderv1.AdminShipOrderRequest) (*orderv1.AdminShipOrderResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	if err := s.uc.AdminShipOrder(ctx, id, req.GetCarrier(), req.GetTrackingNo()); err != nil {
		return nil, status.Errorf(codes.Internal, "admin ship order failed: %v", err)
	}

	return &orderv1.AdminShipOrderResponse{Success: true}, nil
}

// Step 6.2: 买家确认收货与分账清算
// 来源: 买家点击"确认收货"发起的 orderv1.ConfirmReceiptRequest
// 用于: 子订单流转至 COMPLETED，触发 WalletService 分账清算解冻担保货款
func (s *OrderService) ConfirmReceipt(ctx context.Context, req *orderv1.ConfirmReceiptRequest) (*orderv1.ConfirmReceiptResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	// Step 6.2.1: 显式校验确认收货归属权（买家本人或特权放行）
	// 来源: 子订单或旧版单体订单的 UserId
	// 用于: 防止其他买家冒领确认他人订单
	if co, err := s.uc.GetChildOrder(ctx, id); err == nil && co != nil {
		if oErr := authz.RequireOwnership(ctx, co.GetUserId(), authz.PermOrderManage); oErr != nil {
			return nil, oErr
		}
	} else if ord, oErr := s.uc.GetOrder(ctx, id); oErr == nil && ord != nil {
		if aErr := authz.RequireOwnership(ctx, ord.GetUserId(), authz.PermOrderManage); aErr != nil {
			return nil, aErr
		}
	}

	resp, err := s.uc.ConfirmReceipt(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "confirm receipt failed: %v", err)
	}
	return resp, nil
}

// Step 7.1: 买家主动取消未支付订单
// 来源: 买家在未支付倒计时内点击"取消订单" orderv1.CancelOrderRequest
// 用于: CAS 流转订单至 CANCELLED，调用履约中台释放预占库存并移除超时关单任务
func (s *OrderService) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	// Step 7.1.1: 显式校验取消操作所有权
	// 来源: 订单或主订单的 UserId
	// 用于: 杜绝恶意买家伪造 ID 越权取消他人订单
	if ord, err := s.uc.GetOrder(ctx, id); err == nil && ord != nil {
		if oErr := authz.RequireOwnership(ctx, ord.GetUserId(), authz.PermOrderManage); oErr != nil {
			return nil, oErr
		}
	} else if po, pErr := s.uc.GetParentOrder(ctx, id); pErr == nil && po != nil {
		if oErr := authz.RequireOwnership(ctx, po.GetUserId(), authz.PermOrderManage); oErr != nil {
			return nil, oErr
		}
	}

	if err := s.uc.CancelOrder(ctx, id, req.GetReason()); err != nil {
		return nil, status.Errorf(codes.Internal, "cancel order failed: %v", err)
	}

	return &orderv1.CancelOrderResponse{Success: true}, nil
}

// Step 8.1: 买家发起售后退款申请
// 来源: 买家在订单详情发起退款/退货退款 orderv1.ApplyAfterSalesRequest
// 用于: 校验订单资格，精确分摊 SKU 级退款金额，创建 APPLIED 状态售后单
func (s *OrderService) ApplyAfterSales(ctx context.Context, req *orderv1.ApplyAfterSalesRequest) (*orderv1.ApplyAfterSalesResponse, error) {
	userID, err := s.requireUserID(ctx, "")
	if err != nil {
		return nil, err
	}

	asOrder, err := s.uc.ApplyAfterSales(ctx, req, userID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "apply after sales failed: %v", err)
	}

	return &orderv1.ApplyAfterSalesResponse{
		Success:    true,
		AfterSales: asOrder,
	}, nil
}

// Step 8.2: 商家审核售后申请
// 来源: 商户在后台针对售后单进行审批 orderv1.AuditAfterSalesRequest
// 用于: 判定同意或拒绝；若为仅退款通过则即时触发冲正退款
func (s *OrderService) AuditAfterSales(ctx context.Context, req *orderv1.AuditAfterSalesRequest) (*orderv1.AuditAfterSalesResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	resp, err := s.uc.AuditAfterSales(ctx, id, req.GetApproved(), req.GetRejectReason())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "audit after sales failed: %v", err)
	}
	return resp, nil
}

// Step 8.3: 买家退货录入物流单号
// 来源: 买家寄出退货商品后录入物流单号 orderv1.BuyerReturnGoodsRequest
// 用于: 售后单状态推进至 BUYER_RETURNED
func (s *OrderService) BuyerReturnGoods(ctx context.Context, req *orderv1.BuyerReturnGoodsRequest) (*orderv1.BuyerReturnGoodsResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	resp, err := s.uc.BuyerReturnGoods(ctx, id, req.GetCarrier(), req.GetTrackingNo())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "buyer return goods failed: %v", err)
	}
	return resp, nil
}

// Step 8.4: 商家确认收到退货并触发退款清算
// 来源: 商家收货验货后确认退款 orderv1.MerchantConfirmReturnRequest
// 用于: 售后单置为 REFUNDED，调用履约中台执行复式冲正与库存释放
func (s *OrderService) MerchantConfirmReturn(ctx context.Context, req *orderv1.MerchantConfirmReturnRequest) (*orderv1.MerchantConfirmReturnResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	resp, err := s.uc.MerchantConfirmReturn(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "merchant confirm return failed: %v", err)
	}
	return resp, nil
}

// Step 8.5: 买家撤销售后申请
// 来源: 买家主动撤回尚未完成的售后请求 orderv1.CancelAfterSalesRequest
// 用于: 将售后单流转为 CANCELLED 终态
func (s *OrderService) CancelAfterSales(ctx context.Context, req *orderv1.CancelAfterSalesRequest) (*orderv1.CancelAfterSalesResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	if err := s.uc.CancelAfterSales(ctx, id); err != nil {
		return nil, status.Errorf(codes.Internal, "cancel after sales failed: %v", err)
	}

	return &orderv1.CancelAfterSalesResponse{
		Success: true,
		Status:  "CANCELLED",
	}, nil
}

// Step 8.6: 查询单个售后单详情
// 来源: 买家或商户查询指定售后单 orderv1.GetAfterSalesRequest
// 用于: 返回完整售后单详情及明细列表
func (s *OrderService) GetAfterSales(ctx context.Context, req *orderv1.GetAfterSalesRequest) (*orderv1.GetAfterSalesResponse, error) {
	id := strings.TrimSpace(req.GetId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	asOrder, err := s.uc.GetAfterSales(ctx, id)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "after sales order not found: %v", err)
	}

	return &orderv1.GetAfterSalesResponse{AfterSales: asOrder}, nil
}

// Step 8.7: 买家查询自身售后单列表
// 来源: 买家中心售后记录查询 orderv1.ListAfterSalesRequest
// 用于: 返回当前买家的售后单分页列表
func (s *OrderService) ListAfterSales(ctx context.Context, req *orderv1.ListAfterSalesRequest) (*orderv1.ListAfterSalesResponse, error) {
	userID, err := s.requireUserID(ctx, "")
	if err != nil {
		return nil, err
	}

	page, pageSize := paging.FromProto(req.GetPage(), req.GetPageSize())
	asOrders, total, err := s.uc.ListAfterSales(ctx, userID, req.GetStatus(), page, pageSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list after sales failed: %v", err)
	}

	return &orderv1.ListAfterSalesResponse{
		AfterSalesOrders: asOrders,
		Total:            total,
	}, nil
}

// Step 8.8: 商家查询租户售后单列表
// 来源: 商户管理端售后列表查询 orderv1.AdminListAfterSalesRequest
// 用于: 校验商户身份并返回当前店铺的售后单分页列表
func (s *OrderService) AdminListAfterSales(ctx context.Context, req *orderv1.AdminListAfterSalesRequest) (*orderv1.AdminListAfterSalesResponse, error) {
	callerTenant := tenant.TenantID(ctx)
	page, pageSize := paging.FromProto(req.GetPage(), req.GetPageSize())
	asOrders, total, err := s.uc.AdminListAfterSales(ctx, callerTenant, req.GetStatus(), page, pageSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "admin list after sales failed: %v", err)
	}

	return &orderv1.AdminListAfterSalesResponse{
		AfterSalesOrders: asOrders,
		Total:            total,
	}, nil
}
