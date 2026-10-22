package tenant

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestTenantContext(t *testing.T) {
	ctx := context.Background()
	if tid := TenantID(ctx); tid != "" {
		t.Fatalf("expected empty, got %q", tid)
	}

	ctx = WithTenantID(ctx, "tenant-apple")
	tid, err := TenantIDFromContext(ctx)
	if err != nil || tid != "tenant-apple" {
		t.Fatalf("expected 'tenant-apple', got %q, err=%v", tid, err)
	}
}

func TestTenantServerInterceptor(t *testing.T) {
	interceptor := TenantServerInterceptor(true)

	// Missing metadata should error
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})
	if err == nil {
		t.Fatal("expected error on missing metadata, got nil")
	}

	// Present metadata should inject into ctx
	md := metadata.Pairs(MDKeyTenantID, "tenant-nike")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	res, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, req interface{}) (interface{}, error) {
		if got := TenantID(ctx); got != "tenant-nike" {
			t.Fatalf("expected 'tenant-nike', got %q", got)
		}
		return "success", nil
	})
	if err != nil || res != "success" {
		t.Fatalf("expected success, got %v, err=%v", res, err)
	}
}
