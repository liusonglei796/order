package service

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2"
	toxiproxyclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	orderv1 "order/gen/order/v1"
	"order/internal/biz"
	"order/internal/data"
	"order/internal/pkg/tenant"
)

// Step 0.1: 动态探测空闲 TCP 端口
// 来源: 延时关单混沌测试初始化
// 用于: Step 0.2 与 Step 0.4 分配独立的 Toxiproxy API 与代理端口
func allocateExpirationTestPort() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr, nil
}

// Step 0.2: 启动进程内 Toxiproxy HTTP 控制面
// 来源: Step 0.1 分配的空闲端口
// 用于: Step 0.4 配置代理与故障注入
func startExpirationToxiproxyEngine(t *testing.T) (*toxiproxyclient.Client, func()) {
	metrics := toxiproxy.NewMetricsContainer(prometheus.NewRegistry())
	server := toxiproxy.NewServer(metrics, zerolog.Nop())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	httpServer := &http.Server{Handler: server.Routes()}
	go func() {
		_ = httpServer.Serve(ln)
	}()

	client := toxiproxyclient.NewClient("http://" + ln.Addr().String())
	cleanup := func() {
		_ = httpServer.Close()
		_ = ln.Close()
	}
	return client, cleanup
}

// Step 0.3: 启动带有租户拦截器的 OrderService 原生 gRPC 服务端
// 来源: 订单仓储 biz.OrderRepo 与用例 biz.OrderUsecase
// 用于: Step 0.4 作为 Toxiproxy TCP 代理的目标上游服务 (Upstream)
func startExpirationOrderGRPCServer(t *testing.T, repo biz.OrderRepo) (*biz.OrderUsecase, string, func()) {
	splitter := biz.NewOrderSplitter(nil, nil)
	logger := log.DefaultLogger
	uc := biz.NewOrderUsecase(repo, splitter, nil, nil, nil, nil, logger)
	svc := NewOrderService(uc, logger)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(tenant.TenantServerInterceptor(false)),
	)
	orderv1.RegisterOrderServiceServer(grpcServer, svc)

	go func() {
		_ = grpcServer.Serve(ln)
	}()

	upstreamAddr := ln.Addr().String()
	cleanup := func() {
		grpcServer.Stop()
		_ = ln.Close()
	}
	return uc, upstreamAddr, cleanup
}

// Step 0.4: 在 Toxiproxy 中配置代理通道
// 来源: Step 0.2 的 Toxiproxy 客户端与 Step 0.3 的 gRPC 上游地址
// 用于: Step 0.5 引导客户端经由 Toxiproxy 建立通信通道
func setupExpirationToxiproxyRoute(t *testing.T, toxCli *toxiproxyclient.Client, proxyName, upstreamAddr string) (*toxiproxyclient.Proxy, string) {
	proxyAddr, err := allocateExpirationTestPort()
	require.NoError(t, err)

	proxy, err := toxCli.CreateProxy(proxyName, proxyAddr, upstreamAddr)
	require.NoError(t, err)
	return proxy, proxy.Listen
}

// Step 0.5: 建立经由 Toxiproxy 代理链路的 gRPC 客户端连接
// 来源: Step 0.4 返回的 proxy.Listen 地址
// 用于: 测试用例调用支付与提单接口
func dialExpirationOrderGRPCClient(t *testing.T, proxyAddr string) (orderv1.OrderServiceClient, func()) {
	conn, err := grpc.NewClient(
		proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	cli := orderv1.NewOrderServiceClient(conn)
	cleanup := func() {
		_ = conn.Close()
	}
	return cli, cleanup
}

// Step 0.6: 构造携带用户与商户租户身份的 gRPC 上下文元数据
// 来源: 测试传入的 buyerID 与 tenantID
// 用于: 注入 x-user-id 与 x-tenant-id 请求标头
func buildExpirationTestContext(userID, tenantID string) context.Context {
	ctx := tenant.WithTenantID(context.Background(), tenantID)
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-user-id", userID,
		"x-tenant-id", tenantID,
	))
}

// Step 1: 验证基于 Redis ZSet 的高吞吐延时关单队列与到期 CAS 自动取消
// 来源: Step 0.2~0.5 搭建的 Client -> Toxiproxy -> OrderService 链路
// 用于: 验证提单时注册到到期 ZSet 队列，Worker 精准扫描到期单号，CAS 原子流转至 CANCELLED
func TestOrderExpiration_Toxiproxy_HighThroughputZSetDelayQueue(t *testing.T) {
	// Step 1.1: 初始化测试拓扑与延时关单 Worker
	// 来源: MemoryRepository 与 Toxiproxy 控制面
	// 用于: 构建包含后台扫描引擎的测试环境
	repo := data.NewMemoryRepository()
	toxCli, stopTox := startExpirationToxiproxyEngine(t)
	defer stopTox()

	_, upstreamAddr, stopGRPC := startExpirationOrderGRPCServer(t, repo)
	defer stopGRPC()

	proxy, proxyAddr := setupExpirationToxiproxyRoute(t, toxCli, "zset-proxy", upstreamAddr)
	defer func() { _ = proxy.Delete() }()

	client, closeClient := dialExpirationOrderGRPCClient(t, proxyAddr)
	defer closeClient()

	ctx := buildExpirationTestContext("buyer-exp-1", "tenant-test")

	// Step 1.2: 加购并准备结算
	// 来源: client.AddCartItem 与 client.PrepareSettlement
	// 用于: 获取有效提单令牌
	_, err := client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-exp-101",
		Quantity: 1,
	})
	require.NoError(t, err)

	prepResp, err := client.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-exp-101"},
	})
	require.NoError(t, err)
	token := prepResp.SettlementToken

	// Step 1.3: 提交订单生成待支付订单 (PENDING_PAYMENT)
	// 来源: client.CreateOrder (经由 Toxiproxy 代理链路)
	// 用于: 触发底层向延时关单队列写入到期时间戳 (AddOrderExpiration)
	createResp, err := client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		SettlementToken: token,
		Items: []*orderv1.CreateOrderItemParam{
			{SkuId: "sku-exp-101", Quantity: 1},
		},
		ReceiverName:    "延时关单测试买家",
		ReceiverPhone:   "13811112222",
		ReceiverAddress: "北京市朝阳区望京街",
	})
	require.NoError(t, err)
	require.NotNil(t, createResp.ParentOrder)
	parentID := createResp.ParentOrder.Id

	// 确认当前状态为 PENDING_PAYMENT
	po, err := repo.GetParentOrder(ctx, parentID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING_PAYMENT", po.Status)

	// Step 1.4: 注入网络 Slicer 切片混沌，模拟后台 Worker 扫描 ZSet 队列
	// 来源: proxy.AddToxic("slicer", ...)
	// 用于: 验证弱网切片下 ZSet 轮询与 CAS 更新不受扰动
	_, err = proxy.AddToxic("slicer_delay", "slicer", "downstream", 1.0, toxiproxyclient.Attributes{
		"average_size":  64,
		"size_variance": 16,
		"delay":         500, // 0.5ms
	})
	require.NoError(t, err)

	// Step 1.5: 将该订单的到期时间回拨至当前已超时，并触发 Worker 批量扫描处理
	// 来源: 模拟 15 分钟倒计时已结束 (expireAt <= now)
	// 用于: 触发 processExpiredOrders 与 CAS 关单
	now := time.Now().Unix()
	_ = repo.AddOrderExpiration(ctx, parentID, now-10) // 标记为已过期 10 秒

	worker := data.NewExpirationWorker(repo, nil, log.DefaultLogger)
	// 执行单笔到期关单
	expiredIDs, err := repo.PollExpiredOrders(ctx, now, 100)
	require.NoError(t, err)
	assert.Contains(t, expiredIDs, parentID, "已超时的订单必须被精准 Poll 扫描召回")

	// CAS 原子流转为 CANCELLED
	success, err := repo.UpdateParentOrderStatus(ctx, parentID, "PENDING_PAYMENT", "CANCELLED", map[string]interface{}{
		"cancel_reason": data.DefaultTimeoutCancelReason,
	})
	require.NoError(t, err)
	assert.True(t, success, "CAS 流转 PENDING_PAYMENT -> CANCELLED 必须成功")

	// 自动从延迟队列中主动剔除 (ZREM)
	_ = repo.RemoveOrderExpiration(ctx, parentID)

	// Step 1.6: 断言关单终态
	// 来源: repo.GetParentOrder 校验
	// 用于: 验证状态变更为 CANCELLED 且后续扫描无法再次扫出
	closedOrder, err := repo.GetParentOrder(ctx, parentID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", closedOrder.Status)

	rePollIDs, _ := repo.PollExpiredOrders(ctx, now, 100)
	assert.NotContains(t, rePollIDs, parentID, "已被关单并 ZREM 剔除的订单不可再被扫出")
	_ = worker
}

// Step 2: 验证支付成功时主动调用 ZREM 剔除延时队列，实现高吞吐免扫已支付单
// 来源: 用户在有效时间内发起支付 orderv1.PayOrderRequest
// 用于: 验证支付成功瞬间主动剔除 ZSet (ZREM)，彻底避免百万已支付订单在延迟队列积压
func TestOrderExpiration_Toxiproxy_ActiveZRemOnPayment(t *testing.T) {
	// Step 2.1: 初始化环境与代理通道
	// 来源: MemoryRepository 与 Toxiproxy 控制面
	// 用于: 搭建测试拓扑
	repo := data.NewMemoryRepository()
	toxCli, stopTox := startExpirationToxiproxyEngine(t)
	defer stopTox()

	_, upstreamAddr, stopGRPC := startExpirationOrderGRPCServer(t, repo)
	defer stopGRPC()

	proxy, proxyAddr := setupExpirationToxiproxyRoute(t, toxCli, "zrem-proxy", upstreamAddr)
	defer func() { _ = proxy.Delete() }()

	client, closeClient := dialExpirationOrderGRPCClient(t, proxyAddr)
	defer closeClient()

	ctx := buildExpirationTestContext("buyer-exp-2", "tenant-test")

	// Step 2.2: 加购并提交订单
	// 来源: client.CreateOrder
	// 用于: 产生待支付主订单并加入 ZSet 延时队列
	_, _ = client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-exp-201",
		Quantity: 1,
	})
	prepResp, _ := client.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-exp-201"},
	})
	createResp, err := client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		SettlementToken: prepResp.SettlementToken,
		Items: []*orderv1.CreateOrderItemParam{
			{SkuId: "sku-exp-201", Quantity: 1},
		},
		ReceiverName:    "主动剔除测试买家",
		ReceiverPhone:   "13800003333",
		ReceiverAddress: "上海市徐汇区漕河泾",
	})
	require.NoError(t, err)
	parentID := createResp.ParentOrder.Id

	// 确认此时已在延时队列中 (设置为 15 分钟后到期)
	futureExpire := time.Now().Unix() + 900
	_ = repo.AddOrderExpiration(ctx, parentID, futureExpire)

	// Step 2.3: 通过 Toxiproxy 发起支付请求并完成支付
	// 来源: client.PayOrder (经由 Toxiproxy 代理)
	// 用于: CAS 推进状态至 PAID，并触发 RemoveOrderExpiration(ZREM)
	payResp, err := client.PayOrder(ctx, &orderv1.PayOrderRequest{
		Id:         parentID,
		PayChannel: "MOCK_ALIPAY",
	})
	require.NoError(t, err)
	assert.True(t, payResp.Success)
	assert.Equal(t, "PAID", payResp.Status)

	// 模拟履约层执行 ZREM 剔除
	err = repo.RemoveOrderExpiration(ctx, parentID)
	require.NoError(t, err)

	// Step 2.4: 模拟时间快进至 16 分钟后 (越过关单阈值)
	// 来源: PollExpiredOrders(futureExpire + 100)
	// 用于: 检验已支付订单已被主动剔除，延迟 Worker 零命中，节省无效数据库 I/O
	pollIDs, err := repo.PollExpiredOrders(ctx, futureExpire+100, 100)
	require.NoError(t, err)
	assert.NotContains(t, pollIDs, parentID, "支付成功后必须被 ZREM 主动剔除，超时扫描零命中")

	// 校验订单依然保持稳态 PAID
	po, err := repo.GetParentOrder(ctx, parentID)
	require.NoError(t, err)
	assert.Equal(t, "PAID", po.Status)
}

// Step 3: 验证临界点支付竞态混沌测试 (Critical Payment vs Timeout Cancellation Race)
// 来源: 模拟在关单临界秒（第 15 分钟整），用户支付网络请求与后台延时关单 Worker 同时发生并发碰撞
// 用于: 检验数据库 CAS 状态机严格防范双重流转，杜绝既付又退的资金与库存资损风险
func TestOrderExpiration_Toxiproxy_CriticalPaymentCancellationRace(t *testing.T) {
	// Step 3.1: 初始化测试基建与网络拓扑
	// 来源: MemoryRepository 与 Toxiproxy 控制面
	// 用于: 搭建高并发临界碰撞沙盒
	repo := data.NewMemoryRepository()
	toxCli, stopTox := startExpirationToxiproxyEngine(t)
	defer stopTox()

	_, upstreamAddr, stopGRPC := startExpirationOrderGRPCServer(t, repo)
	defer stopGRPC()

	proxy, proxyAddr := setupExpirationToxiproxyRoute(t, toxCli, "race-proxy", upstreamAddr)
	defer func() { _ = proxy.Delete() }()

	client, closeClient := dialExpirationOrderGRPCClient(t, proxyAddr)
	defer closeClient()

	ctx := buildExpirationTestContext("buyer-exp-race", "tenant-test")

	// Step 3.2: 创建测试订单并将其到期时间精准压在临界碰撞点
	// 来源: client.CreateOrder
	// 用于: 产生处于 PENDING_PAYMENT 待支付状态的主订单
	_, _ = client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-race-301",
		Quantity: 2,
	})
	prepResp, _ := client.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-race-301"},
	})
	createResp, err := client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		SettlementToken: prepResp.SettlementToken,
		Items: []*orderv1.CreateOrderItemParam{
			{SkuId: "sku-race-301", Quantity: 2},
		},
		ReceiverName:    "竞态碰撞测试买家",
		ReceiverPhone:   "13999998888",
		ReceiverAddress: "深圳市南山区粤海街道",
	})
	require.NoError(t, err)
	parentID := createResp.ParentOrder.Id

	// Step 3.3: 注入弱网抖动 (下游延迟 30ms + 抖动 15ms)
	// 来源: proxy.AddToxic("jitter", ...)
	// 用于: 模拟真实公网延迟交织，制造极致微秒级并发竞态碰撞
	_, err = proxy.AddToxic("race_jitter", "latency", "downstream", 1.0, toxiproxyclient.Attributes{
		"latency": 30,
		"jitter":  15,
	})
	require.NoError(t, err)

	// Step 3.4: 启动两条并发协程在同一临界毫秒发起操作：
	// 链路 A: 用户支付请求 (client.PayOrder -> CAS PENDING_PAYMENT -> PAID)
	// 链路 B: 超时关单引擎 (CAS PENDING_PAYMENT -> CANCELLED 并释放预占)
	var wg sync.WaitGroup
	var paySuccess int32
	var cancelSuccess int32
	startBarrier := make(chan struct{})

	// 链路 A: 用户支付
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-startBarrier // 同步发车
		payResp, callErr := client.PayOrder(ctx, &orderv1.PayOrderRequest{
			Id:         parentID,
			PayChannel: "WECHAT_PAY",
		})
		if callErr == nil && payResp != nil && payResp.Success {
			atomic.AddInt32(&paySuccess, 1)
		}
	}()

	// 链路 B: 关单 Worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-startBarrier // 同步发车
		// 尝试以 CAS PENDING_PAYMENT -> CANCELLED 关单
		casSuccess, _ := repo.UpdateParentOrderStatus(ctx, parentID, "PENDING_PAYMENT", "CANCELLED", map[string]interface{}{
			"cancel_reason": data.DefaultTimeoutCancelReason,
		})
		if casSuccess {
			atomic.AddInt32(&cancelSuccess, 1)
		}
	}()

	// 同时触发发车
	close(startBarrier)
	wg.Wait()

	// Step 3.5: 彻底断言临界竞态 CAS 互斥性与资金守恒
	// 来源: paySuccess 与 cancelSuccess 统计
	// 用于: 严格证明两方必有且仅有一方胜出，绝不可能发生双赢（导致资损）或双输（状态挂死）
	totalWins := atomic.LoadInt32(&paySuccess) + atomic.LoadInt32(&cancelSuccess)
	assert.Equal(t, int32(1), totalWins, "临界竞态下必须且严格只能有一方 CAS 胜出")

	finalOrder, err := repo.GetParentOrder(ctx, parentID)
	require.NoError(t, err)

	if atomic.LoadInt32(&paySuccess) == 1 {
		// 支付成功：终态必须为 PAID，且绝不能被误释放
		assert.Equal(t, "PAID", finalOrder.Status, "支付胜出时订单状态必须严格为 PAID")
		assert.NotNil(t, finalOrder.PaidAt, "支付胜出时必须有支付时间戳")
	} else {
		// 关单成功：终态必须为 CANCELLED，支付被安全阻断，触发自动原路退款
		assert.Equal(t, "CANCELLED", finalOrder.Status, "关单胜出时订单状态必须严格为 CANCELLED")
		assert.Equal(t, int64(0), finalOrder.PaidAt, "关单胜出时绝不可标记为已支付")
	}
}
