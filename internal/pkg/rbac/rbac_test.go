package rbac

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Step 1.1: 单元测试 - 权限匹配与前缀通配符
// 来源: 权限点与目标操作动作
// 用于: 验证精确匹配、全量通配符 * 以及模块通配符 (如 order:*)
func TestMatchPermission(t *testing.T) {
	assert.True(t, MatchPermission([]string{"*"}, "order:ship"))
	assert.True(t, MatchPermission([]string{"order:*"}, "order:ship"))
	assert.True(t, MatchPermission([]string{"order:ship"}, "order:ship"))
	assert.False(t, MatchPermission([]string{"order:read"}, "order:ship"))
	assert.False(t, MatchPermission([]string{"product:*"}, "order:ship"))
	assert.False(t, MatchPermission([]string{}, "order:ship"))
}

// Step 1.2: 单元测试 - 纵深防御拦截器拦截与放行验证
// 来源: 模拟 gRPC 入站请求与 metadata
// 用于: 验证拦截器对敏感 RPC 的拦截与非敏感 RPC 的直接放行
func TestUnaryServerInterceptor(t *testing.T) {
	ruleMap := map[string]string{
		"/order.v1.OrderService/AdminShipOrder": "order:ship",
	}
	interceptor := UnaryServerInterceptor(ruleMap)

	infoProtected := &grpc.UnaryServerInfo{FullMethod: "/order.v1.OrderService/AdminShipOrder"}
	infoPublic := &grpc.UnaryServerInfo{FullMethod: "/order.v1.OrderService/GetOrder"}

	dummyHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	// 1. 无权限访问受保护方法 -> 拒绝
	ctxNoPerm := context.Background()
	_, err := interceptor(ctxNoPerm, nil, infoProtected, dummyHandler)
	assert.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.PermissionDenied, st.Code())

	// 2. 携带 order:ship 权限访问受保护方法 -> 放行
	mdValid := metadata.Pairs(MDKeyPermissions, "order:ship,finance:read")
	ctxValid := metadata.NewIncomingContext(context.Background(), mdValid)
	res, err := interceptor(ctxValid, nil, infoProtected, dummyHandler)
	assert.NoError(t, err)
	assert.Equal(t, "ok", res)

	// 3. 携带全量通配符 * 访问受保护方法 -> 放行
	mdAdmin := metadata.Pairs(MDKeyPermissions, "*")
	ctxAdmin := metadata.NewIncomingContext(context.Background(), mdAdmin)
	res, err = interceptor(ctxAdmin, nil, infoProtected, dummyHandler)
	assert.NoError(t, err)
	assert.Equal(t, "ok", res)

	// 4. 访问未配置权限限制的方法 -> 直接放行
	res, err = interceptor(ctxNoPerm, nil, infoPublic, dummyHandler)
	assert.NoError(t, err)
	assert.Equal(t, "ok", res)
}
