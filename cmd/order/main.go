package main

import (
	"flag"
	"os"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/grpc"

	"order/internal/conf"
	"order/internal/data"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name = "order"
	// Version is the version of the compiled software.
	Version = "v1.0.0"
	// flagconf is the config path flag.
	flagconf string

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "configs/config.yaml", "config path, eg: -conf config.yaml")
}

// Step 0.1: 装配 Kratos App 实例并注册 gRPC Server 与后台超时关单引擎
// 来源: wireApp 自动装配好的 grpc.Server 与 data.OrderExpirationWorker
// 用于: 将 gRPC 传输服务与 Redis ZSet 超时关单 Worker 统一注册到 Kratos 生命周期中
func newApp(logger log.Logger, gs *grpc.Server, expWorker *data.OrderExpirationWorker) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(
			gs,
			expWorker,
		),
	)
}

// Step 1.1: Order 微服务主入口
// 来源: 进程启动
// 用于: 读取配置文件，初始化 Logger 与 Wire 依赖注入，启动 Kratos 运行时
func main() {
	flag.Parse()
	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
		"service.id", id,
		"service.name", Name,
		"service.version", Version,
	)
	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}

	app, cleanup, err := wireApp(bc.Server, bc.Data, bc.Order, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	// start and wait for stop signal
	if err := app.Run(); err != nil {
		panic(err)
	}
}
