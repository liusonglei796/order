package rbac

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// MDKeyPermissions 是在 gRPC Metadata 中透传权限切片的元数据 Key
	MDKeyPermissions = "x-user-permissions"
	// MDKeyRoles 是在 gRPC Metadata 中透传角色切片的元数据 Key
	MDKeyRoles = "x-user-roles"
	// MDKeyUserID 是在 gRPC Metadata 中透传操作者用户 ID 的元数据 Key
	MDKeyUserID = "x-user-id"
)

type permissionsCtxKey struct{}

// Step 1.1: 将权限切片绑定至 Go 上下文
// 来源: gRPC 服务端拦截器解析 Metadata 或单元测试构造
// 用于: Step 1.2 在微服务内部执行权限核验
func WithPermissions(ctx context.Context, perms []string) context.Context {
	return context.WithValue(ctx, permissionsCtxKey{}, perms)
}

// Step 1.2: 从 Go 上下文或入站 gRPC Metadata 中提取当前用户权限集合
// 来源: Step 1.1 注入的 Context 或 incoming metadata (x-user-permissions)
// 用于: Step 2.1 权限匹配判定
func PermissionsFromContext(ctx context.Context) []string {
	if val, ok := ctx.Value(permissionsCtxKey{}).([]string); ok && len(val) > 0 {
		return val
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		vals := md.Get(MDKeyPermissions)
		if len(vals) > 0 {
			var perms []string
			for _, v := range vals {
				for _, p := range strings.Split(v, ",") {
					trimmed := strings.TrimSpace(p)
					if trimmed != "" {
						perms = append(perms, trimmed)
					}
				}
			}
			return perms
		}
	}
	return nil
}

// Step 2.1: 通用权限比对算法 (支持全量通配符 *、模块前缀通配符及精确匹配)
// 来源: 用户拥有的权限列表 userPerms 与接口所需权限 required
// 用于: 判定是否满足权限要求
func MatchPermission(userPerms []string, required string) bool {
	for _, p := range userPerms {
		if p == "*" {
			return true
		}
		if p == required {
			return true
		}
		if strings.HasSuffix(p, ":*") {
			prefix := strings.TrimSuffix(p, ":*")
			if strings.HasPrefix(required, prefix+":") {
				return true
			}
		}
	}
	return false
}

// Step 3.1: 业务代码内嵌式权限断言 (用于微服务方法内部纵深防御)
// 来源: 业务函数入参 context.Context 以及当前敏感操作所需的 permissionCode
// 用于: 若当前上下文权限不足，直接返回 gRPC PermissionDenied 错误
func RequirePermission(ctx context.Context, requiredPerm string) error {
	perms := PermissionsFromContext(ctx)
	if !MatchPermission(perms, requiredPerm) {
		return status.Errorf(codes.PermissionDenied, "defense-in-depth: missing required permission [%s]", requiredPerm)
	}
	return nil
}

// Step 4.1: 下游微服务通用 gRPC 权限拦截器 (PEP)
// 来源: 方法名与所需权限的映射表 ruleMap (key: fullMethod 如 "/order.v1.OrderService/AdminShipOrder", val: "order:ship")
// 用于: 拦截敏感管理 RPC 调用，缺失权限直接阻断返回 403 PermissionDenied
func UnaryServerInterceptor(ruleMap map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		requiredPerm, exists := ruleMap[info.FullMethod]
		if exists && requiredPerm != "" {
			perms := PermissionsFromContext(ctx)
			if !MatchPermission(perms, requiredPerm) {
				return nil, status.Errorf(codes.PermissionDenied, "permission denied for %s: missing required permission [%s]", info.FullMethod, requiredPerm)
			}
			// 将解析后的权限切片固化到 context
			ctx = WithPermissions(ctx, perms)
		}
		return handler(ctx, req)
	}
}
