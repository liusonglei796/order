package server

import (
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/transport/grpc"

	orderv1 "order/gen/order/v1"
	"order/internal/conf"
	"order/internal/service"
)

// Step 0.1: 构造并配置 Kratos gRPC 传输服务
// 来源: Wire 依赖注入传入的 conf.Server, OrderService, log.Logger
// 用于: 提供纯 gRPC 端口监听，挂载全局崩溃恢复、OpenTelemetry 链路追踪与结构化访问日志中间件
func NewGRPCServer(c *conf.Server, s *service.OrderService, logger log.Logger) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Middleware(
			recovery.Recovery(),
			tracing.Server(),
			logging.Server(logger),
		),
	}
	if c.Grpc.Network != "" {
		opts = append(opts, grpc.Network(c.Grpc.Network))
	}
	if c.Grpc.Addr != "" {
		opts = append(opts, grpc.Address(c.Grpc.Addr))
	}
	if c.Grpc.Timeout != nil {
		opts = append(opts, grpc.Timeout(c.Grpc.Timeout.AsDuration()))
	}

	srv := grpc.NewServer(opts...)
	orderv1.RegisterOrderServiceServer(srv, s)
	return srv
}
