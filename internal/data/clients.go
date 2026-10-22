package data

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	inventoryv1 "order/gen/inventory/v1"
	productv1 "order/gen/product/v1"
	walletv1 "order/gen/wallet/v1"
	"order/internal/conf"
)

// Step 0.1: 构造 Product 微服务 gRPC 客户端
func NewProductServiceClient(c *conf.Order, logger log.Logger) (productv1.ProductServiceClient, func(), error) {
	addr := "127.0.0.1:50052"
	if c != nil && c.ProductGrpcAddr != "" {
		addr = c.ProductGrpcAddr
	}
	conn, err := grpc.DialContext(context.Background(), addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = conn.Close() }
	return productv1.NewProductServiceClient(conn), cleanup, nil
}

// Step 0.2: 构造 Inventory 微服务 gRPC 客户端
func NewInventoryServiceClient(c *conf.Order, logger log.Logger) (inventoryv1.InventoryServiceClient, func(), error) {
	addr := "127.0.0.1:50054"
	if c != nil && c.InventoryGrpcAddr != "" {
		addr = c.InventoryGrpcAddr
	}
	conn, err := grpc.DialContext(context.Background(), addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = conn.Close() }
	return inventoryv1.NewInventoryServiceClient(conn), cleanup, nil
}

// Step 0.3: 构造 Wallet 微服务 gRPC 客户端
func NewWalletServiceClient(c *conf.Order, logger log.Logger) (walletv1.WalletServiceClient, func(), error) {
	addr := "127.0.0.1:50056"
	if c != nil && c.WalletGrpcAddr != "" {
		addr = c.WalletGrpcAddr
	}
	conn, err := grpc.DialContext(context.Background(), addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = conn.Close() }
	return walletv1.NewWalletServiceClient(conn), cleanup, nil
}
