package tenant

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

const (
	// HeaderXTenantID is the standard HTTP header used by edge clients to specify tenant.
	HeaderXTenantID = "X-Tenant-ID"
	// MDKeyTenantID is the canonical gRPC metadata key used to propagate tenant identity.
	MDKeyTenantID = "x-tenant-id"
)

// ErrTenantNotFound indicates missing tenant context.
var ErrTenantNotFound = errors.New("tenant_id not found in context")

type tenantCtxKey struct{}

// Step 1.1: 将租户标识写入 Context
// 来源: gRPC 拦截器从 Metadata 解析出的 tenantID
// 用于: Step 1.2 在请求生命周期内随时检索当前生效的租户 ID
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantCtxKey{}, strings.TrimSpace(tenantID))
}

// Step 1.2: 从 Context 中提取租户标识并返回错误判别
// 来源: Step 1.1 注入的 context.Context
// 用于: Step 1.3 及业务层严谨判断当前会话是否处于多租户模式
func TenantIDFromContext(ctx context.Context) (string, error) {
	if val, ok := ctx.Value(tenantCtxKey{}).(string); ok && val != "" {
		return val, nil
	}
	return "", ErrTenantNotFound
}

// Step 1.3: 便捷获取租户标识 (未指定时返回空字符串)
// 来源: 当前 context.Context
// 用于: 跨服务调用、日志追踪与 SQL 过滤范围构造
func TenantID(ctx context.Context) string {
	tid, _ := TenantIDFromContext(ctx)
	return tid
}

// Step 2.1: 服务端入站 gRPC 拦截器 (提取并绑定租户上下文)
// 来源: 上游客户端或网关在 gRPC 头部传递的 x-tenant-id 元数据
// 用于: Step 1.1 将租户标识写入请求上下文，若 requireTenant=true 且缺失则拒绝请求
func TenantServerInterceptor(requireTenant bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		var tid string
		if ok {
			vals := md.Get(MDKeyTenantID)
			if len(vals) > 0 {
				tid = strings.TrimSpace(vals[0])
			}
		}

		if tid == "" && requireTenant {
			return nil, status.Errorf(codes.InvalidArgument, "missing required metadata %q", MDKeyTenantID)
		}

		if tid != "" {
			ctx = WithTenantID(ctx, tid)
		}

		return handler(ctx, req)
	}
}

// Step 2.2: 客户端出站 gRPC 拦截器 (透传租户上下文至下游服务)
// 来源: 当前上下文中的 Step 1.3 TenantID
// 用于: 自动向发往下一跳微服务的 Metadata 注入 x-tenant-id，保证租户链路一致性
func TenantClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if tid := TenantID(ctx); tid != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, MDKeyTenantID, tid)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// Step 3.1: GORM 租户安全隔离作用域 (Tenant Scope)
// 来源: 当前请求 context.Context 中包含的租户信息
// 用于: 自动为当前 SQL 会话追加 WHERE tenant_id = ? 条件，防止跨租户数据越权
func TenantScope(ctx context.Context) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		tid := TenantID(ctx)
		if tid == "" {
			_ = db.AddError(ErrTenantNotFound)
			return db
		}
		return db.Where("tenant_id = ?", tid)
	}
}
