// Package authz 收敛 ADR 0012 定义的资源归属授权策略。
//
// 存在的理由：归属判定此前在 customer 与 order 的服务层各内联一份，
// 两份规则并不一致（customer 硬编码了 platform-root-1 / user-admin-1 两个 ID 作为旁路，
// order 则完全不允许管理面读取），而 product / inventory / wallet 的读路径则根本没有判定。
// 策略必须由单一模块持有，否则"是否校验"就变成了各服务自行决定的事情。
package authz

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"order/internal/pkg/rbac"
	"order/internal/pkg/tenant"
)

// 通配放行所需的权限代码。
//
// 网关在 resolveUserPermissions 中为管理员与平台治理面签发 `*`，
// 因此这里用领域前缀代码而非硬编码用户 ID，即可覆盖管理面且不与身份耦合。
const (
	// PermCustomerRead 放行跨买家读取买家档案
	PermCustomerRead = "customer:read"
	// PermCustomerManage 放行代改他人买家档案
	PermCustomerManage = "customer:manage"
	// PermOrderRead 放行跨买家读取订单与售后单
	PermOrderRead = "order:read"
	// PermOrderManage 放行跨买家管理/操作他人订单与售后单
	PermOrderManage = "order:manage"
)

// Step 1.1: 从入站 gRPC Metadata 解析调用者身份
// 来源: 网关认证中间件注入的 x-user-id
// 用于: Step 1.2 资源归属比对的调用方基准；缺失时返回 ok=false 交由调用方决策
func CallerID(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	vals := md.Get(rbac.MDKeyUserID)
	if len(vals) == 0 {
		return "", false
	}
	id := strings.TrimSpace(vals[0])
	return id, id != ""
}

// Step 1.2: 判定当前调用是否来自平台治理面
// 来源: Step 2.1 注入上下文的租户标识
// 用于: 平台运营对跨租户实体的合法管理
func IsPlatformCaller(ctx context.Context) bool {
	return tenant.TenantID(ctx) == "platform"
}

// Step 2.1: 纯比对式资源归属判定
// 来源: 调用方已解析出的身份 callerID 与目标资源归属 ownerID
// 用于: Step 2.2 与 Step 3.1 复用同一份比对语义，避免各服务各写一份
func Authorize(callerID, ownerID string) error {
	if callerID == ownerID && ownerID != "" {
		return nil
	}
	return status.Error(codes.PermissionDenied, "resource ownership check failed: caller does not own the target resource")
}

// Step 3.1: 端到端资源归属授权
// 来源: 当前请求 ctx 的 x-user-id 身份、上下文租户、通配权限集，以及目标资源归属 ownerID
// 用于: 消费者面读写接口的统一准入判定
//
// 放行条件（任一成立）：调用者即资源归属者；调用来自平台治理面租户；
// 调用者持有 overridePerm 对应的通配权限。未携带任何身份的请求一律拒绝，
// 不做静默兜底——兜底会让未认证流量伪装成合法调用者。
func RequireOwnership(ctx context.Context, ownerID, overridePerm string) error {
	callerID, ok := CallerID(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "authentication required: missing x-user-id")
	}
	if IsPlatformCaller(ctx) {
		return nil
	}
	if overridePerm != "" && rbac.MatchPermission(rbac.PermissionsFromContext(ctx), overridePerm) {
		return nil
	}
	return Authorize(callerID, ownerID)
}
