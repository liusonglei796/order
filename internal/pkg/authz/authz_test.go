package authz

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"order/internal/pkg/tenant"
)

func callerCtx(id string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-user-id", id))
}

func tenantCtx(id, tid string) context.Context {
	return tenant.WithTenantID(callerCtx(id), tid)
}

func permsCtx(id string, perms ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-user-id", id, "x-user-permissions", joinPerms(perms)))
}

func joinPerms(perms []string) string {
	out := ""
	for i, p := range perms {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error with code %v, got nil", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("want code %v, got %v (err=%v)", want, got, err)
	}
}

func TestAuthorize(t *testing.T) {
	if err := Authorize("u1", "u1"); err != nil {
		t.Fatalf("self ownership should pass, got %v", err)
	}
	assertCode(t, Authorize("u2", "u1"), codes.PermissionDenied)
	// 空 ownerID 不得因两侧同为空而误放行
	assertCode(t, Authorize("", ""), codes.PermissionDenied)
}

func TestRequireOwnership_Self(t *testing.T) {
	if err := RequireOwnership(callerCtx("u1"), "u1", PermCustomerRead); err != nil {
		t.Fatalf("self read should pass, got %v", err)
	}
}

func TestRequireOwnership_CrossDenied(t *testing.T) {
	assertCode(t, RequireOwnership(callerCtx("u2"), "u1", PermCustomerRead), codes.PermissionDenied)
}

func TestRequireOwnership_MissingIdentity(t *testing.T) {
	// 未认证流量不得被静默兜底放行
	assertCode(t, RequireOwnership(context.Background(), "u1", PermCustomerRead), codes.Unauthenticated)
	assertCode(t, RequireOwnership(tenant.WithTenantID(context.Background(), "tenant-default"), "u1", ""), codes.Unauthenticated)
	// 空白字符同样视为缺失
	assertCode(t, RequireOwnership(callerCtx("   "), "u1", ""), codes.Unauthenticated)
}

func TestRequireOwnership_PlatformPlane(t *testing.T) {
	if err := RequireOwnership(tenantCtx("anyone", "platform"), "u1", ""); err != nil {
		t.Fatalf("platform governance plane should pass, got %v", err)
	}
	// 非 platform 租户不得享受治理面特权
	assertCode(t, RequireOwnership(tenantCtx("anyone", "tenant-default"), "u1", ""), codes.PermissionDenied)
}

func TestRequireOwnership_WildcardPermission(t *testing.T) {
	// 网关为管理员签发的 * 放行
	if err := RequireOwnership(permsCtx("user-admin-1", "*"), "u1", PermCustomerManage); err != nil {
		t.Fatalf("wildcard permission should pass, got %v", err)
	}
	// 领域前缀通配
	if err := RequireOwnership(permsCtx("mgr", "customer:*"), "u1", PermCustomerManage); err != nil {
		t.Fatalf("prefix wildcard should pass, got %v", err)
	}
	// 无关权限不放行
	assertCode(t, RequireOwnership(permsCtx("viewer", "product:read"), "u1", PermCustomerManage), codes.PermissionDenied)
	// 读取权限不得隐式获得管理权限
	assertCode(t, RequireOwnership(permsCtx("viewer", "customer:read"), "u1", PermCustomerManage), codes.PermissionDenied)
}

func TestCallerID(t *testing.T) {
	if id, ok := CallerID(callerCtx("u1")); !ok || id != "u1" {
		t.Fatalf("CallerID = (%q, %v), want (\"u1\", true)", id, ok)
	}
	if _, ok := CallerID(context.Background()); ok {
		t.Fatal("background context should not yield a caller identity")
	}
}
