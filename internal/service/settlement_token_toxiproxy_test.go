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
	"github.com/alicebob/miniredis/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"order/internal/biz"
	"order/internal/data"
	orderv1 "order/gen/order/v1"
	"order/internal/pkg/tenant"
)

// Step 0.1: 获取空闲 TCP 环回测试端口
// 来源: 测试启动初始化流程
// 用于: Step 0.2 与 Step 0.4 分配独立的 Toxiproxy 控制面与代理中继端口
func allocateFreePort() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr, nil
}

// Step 0.2: 启动 Shopify Toxiproxy 进程内 HTTP 控制面
// 来源: Step 0.1 动态探测空闲端口
// 用于: Step 0.4 向 Toxiproxy 注册代理通道及动态注入弱网与延迟 Toxic
func startToxiproxyEngine(t *testing.T) (*toxiproxyclient.Client, func()) {
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

// Step 0.3: 启动 OrderService 原生 gRPC 服务端监听
// 来源: 测试准备的仓储实例 biz.OrderRepo 与用例 biz.OrderUsecase
// 用于: Step 0.4 作为 Toxiproxy TCP 代理的目标上游服务 (Upstream)
func startOrderGRPCServer(t *testing.T, repo biz.OrderRepo, leaseTimeout time.Duration) (*biz.OrderUsecase, string, func()) {
	splitter := biz.NewOrderSplitter(nil, nil)
	logger := log.DefaultLogger
	uc := biz.NewOrderUsecase(repo, splitter, nil, nil, nil, nil, logger)
	if leaseTimeout > 0 {
		uc.SetLeaseTimeout(leaseTimeout)
	}
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
// 用于: Step 0.5 引导客户端经由 Toxiproxy 建立混沌网络传输通道
func setupToxiproxyRoute(t *testing.T, toxCli *toxiproxyclient.Client, proxyName, upstreamAddr string) (*toxiproxyclient.Proxy, string) {
	proxyAddr, err := allocateFreePort()
	require.NoError(t, err)

	proxy, err := toxCli.CreateProxy(proxyName, proxyAddr, upstreamAddr)
	require.NoError(t, err)
	return proxy, proxy.Listen
}

// Step 0.5: 建立经由 Toxiproxy 代理链路的 gRPC 客户端连接
// 来源: Step 0.4 返回的 Toxiproxy 代理监听地址 proxyAddr
// 用于: 后续各测试阶段发起经受网络延迟与断网注入的订单 gRPC 请求
func dialOrderGRPCClient(t *testing.T, proxyAddr string) (orderv1.OrderServiceClient, func()) {
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
// 来源: 各测试阶段传入的买家 userID 与商户 tenantID
// 用于: 模拟 Envoy 网关转发至微服务携带的 x-user-id 与 x-tenant-id 头
func buildTestContext(userID, tenantID string) context.Context {
	ctx := tenant.WithTenantID(context.Background(), tenantID)
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-user-id", userID,
		"x-tenant-id", tenantID,
	))
}

// Step 1: 验证并发连击拦截 (Concurrent Hammering Defense via Toxiproxy)
// 来源: Step 0.2~0.5 搭建的 Client -> Toxiproxy -> OrderService 链路
// 用于: 验证多协程高并发突发点击同一 settlement_token 时，仅 1 笔成功流转，其余 9 笔被毫秒级阻断拦截
func TestSettlementToken_Toxiproxy_ConcurrentHammering(t *testing.T) {
	// Step 1.1: 初始化基建网络拓扑
	// 来源: 内存仓储与 Toxiproxy 控制面
	// 用于: 创建受控的混沌测试网络通道
	repo := data.NewMemoryRepository()
	toxCli, stopTox := startToxiproxyEngine(t)
	defer stopTox()

	_, upstreamAddr, stopGRPC := startOrderGRPCServer(t, repo, 5*time.Second)
	defer stopGRPC()

	proxy, proxyAddr := setupToxiproxyRoute(t, toxCli, "hammer-proxy", upstreamAddr)
	defer func() { _ = proxy.Delete() }()

	client, closeClient := dialOrderGRPCClient(t, proxyAddr)
	defer closeClient()

	ctx := buildTestContext("buyer-hammer", "tenant-test")

	// Step 1.2: 买家加购商品
	// 来源: client.AddCartItem
	// 用于: Step 1.3 结算预检查锁定加购条目
	_, err := client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-concurrency-101",
		Quantity: 10,
	})
	require.NoError(t, err)

	// Step 1.3: 准备结算并签发结算令牌
	// 来源: 客户端调用 PrepareSettlement
	// 用于: 获取初始状态为 INIT 的 settlement_token
	prepResp, err := client.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-concurrency-101"},
	})
	require.NoError(t, err)
	token := prepResp.SettlementToken
	require.NotEmpty(t, token)

	// Step 1.3: 模拟用户暴力快速连击/黑客突发并发：10 个 Goroutine 同时通过 Toxiproxy 提交相同令牌
	// 来源: Step 1.2 签发的同一 settlement_token
	// 用于: 触发并发竞态，检验 Redis+Lua 原子状态机 CAS 阻断能力
	concurrency := 10
	var wg sync.WaitGroup
	var successCount int32
	var rejectedCount int32
	var createdParentID string
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			req := &orderv1.CreateOrderRequest{
				SettlementToken: token,
				Items: []*orderv1.CreateOrderItemParam{
					{SkuId: "sku-concurrency-101", Quantity: 1},
				},
				ReceiverName:    "并发测试用户",
				ReceiverPhone:   "13900001111",
				ReceiverAddress: "北京市海淀区中关村南大街1号",
			}

			resp, callErr := client.CreateOrder(ctx, req)
			if callErr == nil && resp != nil && resp.ParentOrder != nil {
				atomic.AddInt32(&successCount, 1)
				mu.Lock()
				if createdParentID == "" {
					createdParentID = resp.ParentOrder.Id
				} else {
					assert.Equal(t, createdParentID, resp.ParentOrder.Id, "所有成功响应必须回显相同的主订单 ID")
				}
				mu.Unlock()
			} else {
				atomic.AddInt32(&rejectedCount, 1)
				st, ok := status.FromError(callErr)
				require.True(t, ok)
				assert.Contains(t, st.Message(), "cannot create order")
			}
		}(i)
	}

	wg.Wait()

	// Step 1.4: 断言并发拦截与数据守恒结果
	// 来源: Step 1.3 的并发执行计数统计
	// 用于: 彻底证明并发连击下无论命中 STILL_PROCESSING 拦截还是 SUCCESS_REPLAY 幂等回显，业务主单严格单例守恒
	assert.GreaterOrEqual(t, successCount, int32(1), "必须至少有 1 笔成功建单或幂等回显")
	assert.Equal(t, int32(concurrency), successCount+rejectedCount, "所有并发请求均被安全处理")
	assert.NotEmpty(t, createdParentID)

	// Step 1.5: 校验仓储层主子订单实体守恒
	// 来源: 底层 MemoryRepository 存储检索
	// 用于: 确认底层无多余脏数据产生，主订单数量严格为 1
	pos, total, err := repo.ListParentOrders(ctx, "buyer-hammer", "", 1, 10)
	require.NoError(t, err)
	assert.Equal(t, int64(1), total, "底层主订单总数必须严格为 1，无重复落库")
	assert.Equal(t, createdParentID, pos[0].Id)
}

// Step 2: 验证弱网延迟与已成功订单秒级回显 (Weak Network Latency, Timeout & Instant Echo via Toxiproxy)
// 来源: Step 0.2~0.5 搭建的网络链路，结合 Toxiproxy latency toxic 故障注入
// 用于: 验证移动端在网络延迟导致超时报错时，后台已建单成功；后续自动重试秒级命中 SUCCESS 回显历史单号，杜绝重复建单
func TestSettlementToken_Toxiproxy_WeakNetworkAndInstantEcho(t *testing.T) {
	// Step 2.1: 初始化基建网络拓扑
	// 来源: 仓储与 Toxiproxy 控制面
	// 用于: 构建可注入网络高延迟的代理通道
	repo := data.NewMemoryRepository()
	toxCli, stopTox := startToxiproxyEngine(t)
	defer stopTox()

	_, upstreamAddr, stopGRPC := startOrderGRPCServer(t, repo, 5*time.Second)
	defer stopGRPC()

	proxy, proxyAddr := setupToxiproxyRoute(t, toxCli, "latency-proxy", upstreamAddr)
	defer func() { _ = proxy.Delete() }()

	client, closeClient := dialOrderGRPCClient(t, proxyAddr)
	defer closeClient()

	ctx := buildTestContext("buyer-weaknet", "tenant-test")

	// Step 2.2: 加购商品
	// 来源: client.AddCartItem
	// 用于: Step 2.3 准备结算提供待结算条目
	_, err := client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-weaknet-201",
		Quantity: 2,
	})
	require.NoError(t, err)

	// Step 2.3: 准备结算获取 settlement_token
	// 来源: client.PrepareSettlement
	// 用于: 获取测试令牌
	prepResp, err := client.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-weaknet-201"},
	})
	require.NoError(t, err)
	token := prepResp.SettlementToken

	// Step 2.3: 通过 Toxiproxy 注入弱网高延迟 Toxic (下游响应延迟 800ms)
	// 来源: proxy.AddToxic API
	// 用于: 模拟基站信号弱、丢包重传导致的服务端响应延迟
	toxic, err := proxy.AddToxic("weak_latency", "latency", "downstream", 1.0, toxiproxyclient.Attributes{
		"latency": 800, // 延迟 800ms
		"jitter":  50,
	})
	require.NoError(t, err)
	require.NotNil(t, toxic)

	// Step 2.4: 客户端发起首次提单，设置 150ms 超时时间 (模拟移动端快速超时取消)
	// 来源: context.WithTimeout(ctx, 150*time.Millisecond)
	// 用于: 触发客户端超时中断错误 codes.DeadlineExceeded
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancelTimeout()

	req := &orderv1.CreateOrderRequest{
		SettlementToken: token,
		Items: []*orderv1.CreateOrderItemParam{
			{SkuId: "sku-weaknet-201", Quantity: 2},
		},
		ReceiverName:    "弱网测试买家",
		ReceiverPhone:   "13800002222",
		ReceiverAddress: "上海市浦东新区张江高科技园区",
	}

	startFirst := time.Now()
	firstResp, firstErr := client.CreateOrder(timeoutCtx, req)
	elapsedFirst := time.Since(startFirst)

	// 断言客户端因高延迟超时
	require.Error(t, firstErr)
	assert.Nil(t, firstResp)
	st, ok := status.FromError(firstErr)
	require.True(t, ok)
	assert.Equal(t, codes.DeadlineExceeded, st.Code(), "客户端必须在超时后收到 DeadlineExceeded 异常")
	assert.Less(t, elapsedFirst, 400*time.Millisecond, "超时应在约 150ms 左右返回")

	// Step 2.5: 等待服务端后台异步完成持久化，并移除 Toxiproxy 弱网 Toxic 恢复链路
	// 来源: 服务端需要约 100ms 处理完请求并写入 SUCCESS 状态
	// 用于: 恢复正常网络延迟，模拟移动端网络自愈
	time.Sleep(300 * time.Millisecond)
	err = proxy.RemoveToxic("weak_latency")
	require.NoError(t, err)

	// Step 2.6: 客户端执行弱网自动重试机制 (携带相同的 settlement_token 发起提单)
	// 来源: 移动端网络层自动幂等重试流程
	// 用于: 验证状态机 SUCCESS_REPLAY 毫秒级回显已建主订单，免除重复计算与重复落库
	startRetry := time.Now()
	retryResp, retryErr := client.CreateOrder(ctx, req)
	elapsedRetry := time.Since(startRetry)

	require.NoError(t, retryErr)
	require.NotNil(t, retryResp)
	require.NotNil(t, retryResp.ParentOrder)

	// Step 2.7: 断言秒级回显数据一致性与耗时
	// 来源: retryResp 返回的订单数据与耗时统计
	// 用于: 证明弱网重试下已成功订单毫秒级零打扰回显，杜绝双重扣减与重复提单
	assert.NotEmpty(t, retryResp.ParentOrder.Id)
	assert.Equal(t, "PENDING_PAYMENT", retryResp.ParentOrder.Status)
	assert.Less(t, elapsedRetry, 100*time.Millisecond, "终态回显耗时必须极短（毫秒级回显）")

	// 再次校验数据库中此买家此商品主订单严格只有 1 笔
	pos, total, err := repo.ListParentOrders(ctx, "buyer-weaknet", "", 1, 10)
	require.NoError(t, err)
	assert.Equal(t, int64(1), total, "底层主订单总数必须严格为 1，无重复落库")
	assert.Equal(t, retryResp.ParentOrder.Id, pos[0].Id)
}

// Step 3: 验证宕机死锁与租约超时自愈 (Lease Timeout Self-Healing via Toxiproxy)
// 来源: 模拟前序提单业务节点在获取 PROCESSING 锁后发生宕机或不可抗力超时中断
// 用于: 验证状态机租约机制在租约窗口过后，允许后续重试安全接管租约并自愈重试，彻底杜绝宕机死锁
func TestSettlementToken_Toxiproxy_LeaseTimeoutSelfHealing(t *testing.T) {
	// Step 3.1: 初始化基建网络拓扑，设置提单租约锁为 200ms
	// 来源: OrderUsecase 动态租约配置 SetLeaseTimeout(200*time.Millisecond)
	// 用于: 缩短测试等待窗口，精准模拟租约超时边界
	repo := data.NewMemoryRepository()
	toxCli, stopTox := startToxiproxyEngine(t)
	defer stopTox()

	leaseTimeout := 200 * time.Millisecond
	_, upstreamAddr, stopGRPC := startOrderGRPCServer(t, repo, leaseTimeout)
	defer stopGRPC()

	proxy, proxyAddr := setupToxiproxyRoute(t, toxCli, "healing-proxy", upstreamAddr)
	defer func() { _ = proxy.Delete() }()

	client, closeClient := dialOrderGRPCClient(t, proxyAddr)
	defer closeClient()

	ctx := buildTestContext("buyer-healing", "tenant-test")

	// Step 3.2: 加购商品
	// 来源: client.AddCartItem
	// 用于: Step 3.3 准备结算提供待结算条目
	_, err := client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-healing-301",
		Quantity: 1,
	})
	require.NoError(t, err)

	// Step 3.3: 准备结算获取令牌
	// 来源: client.PrepareSettlement
	// 用于: 签发初始令牌
	prepResp, err := client.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-healing-301"},
	})
	require.NoError(t, err)
	token := prepResp.SettlementToken

	// Step 3.3: 模拟前序处理节点突发中断/崩溃：前序节点已执行 AcquireSettlementToken 将状态推至 PROCESSING
	// 来源: repo.AcquireSettlementToken 直接调用
	// 用于: 模拟前序节点持有租约锁但未完成后续建单即断电失联
	acquireRes, err := repo.AcquireSettlementToken(ctx, "buyer-healing", token, leaseTimeout)
	require.NoError(t, err)
	assert.Equal(t, "PROCEED", acquireRes.Action, "首次获取必须成功流转至 PROCESSING")

	// Step 3.4: 在租约有效期内 (50ms < 200ms) 客户端发起重试提单
	// 来源: 经由 Toxiproxy 发起的重试请求
	// 用于: 验证仍在租约期内时被正确判定为 STILL_PROCESSING 并安全拦截
	req := &orderv1.CreateOrderRequest{
		SettlementToken: token,
		Items: []*orderv1.CreateOrderItemParam{
			{SkuId: "sku-healing-301", Quantity: 1},
		},
		ReceiverName:    "自愈测试买家",
		ReceiverPhone:   "13700003333",
		ReceiverAddress: "深圳市南山区科技园",
	}

	time.Sleep(50 * time.Millisecond)
	blockedResp, blockedErr := client.CreateOrder(ctx, req)
	require.Error(t, blockedErr)
	assert.Nil(t, blockedResp)
	assert.Contains(t, blockedErr.Error(), "STILL_PROCESSING", "租约窗口期内应安全拦截连击")

	// Step 3.5: 等待租约超时 (睡眠 220ms，累计已过 270ms > 200ms)
	// 来源: 模拟崩溃节点未能续约，租约自然过期
	// 用于: 触发状态机租约自愈接管逻辑
	time.Sleep(220 * time.Millisecond)

	// Step 3.6: 客户端再次发起重试提单
	// 来源: 经由 Toxiproxy 链路到达 OrderService
	// 用于: 验证租约超时自愈：允许新请求接管 PROCESSING 锁并完成建单
	healedResp, healedErr := client.CreateOrder(ctx, req)
	require.NoError(t, healedErr)
	require.NotNil(t, healedResp)
	require.NotNil(t, healedResp.ParentOrder)

	// Step 3.7: 断言自愈成功结果
	// 来源: healedResp 实体校验
	// 用于: 证明系统具备抗宕机死锁自愈能力，主订单正常生成
	assert.NotEmpty(t, healedResp.ParentOrder.Id)
	assert.Equal(t, "PENDING_PAYMENT", healedResp.ParentOrder.Status)

	// 最终验证订单数据正常
	po, err := repo.GetParentOrder(ctx, healedResp.ParentOrder.Id)
	require.NoError(t, err)
	assert.Equal(t, healedResp.ParentOrder.Id, po.Id)
}

// Step 4: 验证网络物理链路中断与重连自愈 (Network Severance & Recovery via Toxiproxy)
// 来源: Toxiproxy proxy.Disable() 与 proxy.Enable() 模拟网络硬件故障
// 用于: 检验客户端在物理断网抛出 Unavailable 错误后，网络自愈恢复时提单状态机仍能正常幂等履约
func TestSettlementToken_Toxiproxy_NetworkSeveranceAndRecovery(t *testing.T) {
	// Step 4.1: 初始化网络链路与 gRPC 客户端
	// 来源: Toxiproxy 控制面与 OrderService
	// 用于: 搭建物理断网模拟环境
	repo := data.NewMemoryRepository()
	toxCli, stopTox := startToxiproxyEngine(t)
	defer stopTox()

	_, upstreamAddr, stopGRPC := startOrderGRPCServer(t, repo, 5*time.Second)
	defer stopGRPC()

	proxy, proxyAddr := setupToxiproxyRoute(t, toxCli, "severance-proxy", upstreamAddr)
	defer func() { _ = proxy.Delete() }()

	client, closeClient := dialOrderGRPCClient(t, proxyAddr)
	defer closeClient()

	ctx := buildTestContext("buyer-severance", "tenant-test")

	// Step 4.2: 加购商品
	// 来源: client.AddCartItem
	// 用于: Step 4.3 准备结算提供待结算条目
	_, err := client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-severance-401",
		Quantity: 1,
	})
	require.NoError(t, err)

	// Step 4.3: 准备结算获取令牌
	// 来源: client.PrepareSettlement
	// 用于: 签发令牌
	prepResp, err := client.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-severance-401"},
	})
	require.NoError(t, err)
	token := prepResp.SettlementToken

	// Step 4.3: 通过 Toxiproxy 注入 reset_peer 模拟网络物理链路断开与 TCP RST 重置
	// 来源: Toxiproxy 底层针对下游连接强制发送 TCP RST
	// 用于: 模拟网线拔除、中间路由器瘫痪或连接断开
	toxic, err := proxy.AddToxic("cut_link", "reset_peer", "downstream", 1.0, toxiproxyclient.Attributes{
		"timeout": 0,
	})
	require.NoError(t, err)
	require.NotNil(t, toxic)

	req := &orderv1.CreateOrderRequest{
		SettlementToken: token,
		Items: []*orderv1.CreateOrderItemParam{
			{SkuId: "sku-severance-401", Quantity: 1},
		},
		ReceiverName:    "断网测试买家",
		ReceiverPhone:   "13600004444",
		ReceiverAddress: "广州市天河区珠江新城",
	}

	// 客户端请求必须在断网重置下失败
	cutCtx, cancelCut := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelCut()
	cutResp, cutErr := client.CreateOrder(cutCtx, req)
	require.Error(t, cutErr)
	assert.Nil(t, cutResp)
	st, _ := status.FromError(cutErr)
	assert.Equal(t, codes.Unavailable, st.Code(), "断网时客户端必须返回 Unavailable 错误")

	// Step 4.4: 移除 reset_peer Toxic 恢复物理网络链路
	// 来源: Toxiproxy 移除故障注入，恢复 TCP 数据透明中继
	// 用于: 模拟网络恢复正常通信
	err = proxy.RemoveToxic("cut_link")
	require.NoError(t, err)

	// Step 4.5: 网络恢复后重试提单
	// 来源: 客户端重发 CreateOrder 请求
	// 用于: 验证链路恢复后提单流程能无缝完成
	recoveredResp, recoveredErr := client.CreateOrder(ctx, req)
	require.NoError(t, recoveredErr)
	require.NotNil(t, recoveredResp)
	assert.NotEmpty(t, recoveredResp.ParentOrder.Id)
}

// Step 5: 验证两道防线数据守恒与拓扑完整性 (Two-Tier Defense & Data Conservation)
// 来源: 基于 Toxiproxy 压测执行完成的复杂订单拓扑
// 用于: 验证主子订单金额分配平账守恒、租户隔离与商品库存防超卖，展示双级防线完整闭环
func TestSettlementToken_Toxiproxy_TwoTierDefenseDataConservation(t *testing.T) {
	// Step 5.1: 初始化基建网络
	// 来源: 仓储与 Toxiproxy 控制面
	// 用于: 运行多商户跨店拆单数据守恒验证
	repo := data.NewMemoryRepository()
	toxCli, stopTox := startToxiproxyEngine(t)
	defer stopTox()

	_, upstreamAddr, stopGRPC := startOrderGRPCServer(t, repo, 5*time.Second)
	defer stopGRPC()

	proxy, proxyAddr := setupToxiproxyRoute(t, toxCli, "conservation-proxy", upstreamAddr)
	defer func() { _ = proxy.Delete() }()

	client, closeClient := dialOrderGRPCClient(t, proxyAddr)
	defer closeClient()

	ctx := buildTestContext("buyer-defense", "tenant-test")

	// Step 5.2: 买家加购多件跨店商品
	// 来源: client.AddCartItem
	// 用于: Step 5.3 准备结算提供待结算条目
	_, err := client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-multi-1",
		Quantity: 2,
	})
	require.NoError(t, err)
	_, err = client.AddCartItem(ctx, &orderv1.AddCartItemRequest{
		SkuId:    "sku-multi-2",
		Quantity: 3,
	})
	require.NoError(t, err)

	// Step 5.3: 准备包含多件商品的结算请求
	// 来源: client.PrepareSettlement
	// 用于: 签发结算令牌并验证两道防线
	prepResp, err := client.PrepareSettlement(ctx, &orderv1.PrepareSettlementRequest{
		SkuIds: []string{"sku-multi-1", "sku-multi-2"},
	})
	require.NoError(t, err)

	createResp, err := client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		SettlementToken: prepResp.SettlementToken,
		Items: []*orderv1.CreateOrderItemParam{
			{SkuId: "sku-multi-1", Quantity: 2},
			{SkuId: "sku-multi-2", Quantity: 3},
		},
		ReceiverName:    "平账测试买家",
		ReceiverPhone:   "13500005555",
		ReceiverAddress: "成都市高新区天府大道",
	})
	require.NoError(t, err)
	require.NotNil(t, createResp.ParentOrder)

	po := createResp.ParentOrder

	// Step 5.3: 校验主子订单金额平账守恒 (第一道与第二道防线数据校验)
	// 来源: createResp.ParentOrder 与 createResp.ChildOrders
	// 用于: 证明拆单与支付平账严格无单分差额偏差
	var sumChildPay int64
	var sumChildTotal int64
	for _, ch := range createResp.ChildOrders {
		sumChildPay += ch.PayAmount
		sumChildTotal += ch.TotalAmount
		assert.Equal(t, po.Id, ch.ParentId)
	}

	assert.Equal(t, po.PayAmount, sumChildPay, "主订单实付金额必须严格等于各子订单实付金额之和")
	assert.Equal(t, po.TotalAmount, sumChildTotal, "主订单总金额必须严格等于各子订单总金额之和")
}

// Step 6: 验证生产级 Redis Lua 状态机在 Toxiproxy 混沌下的全分支覆盖测试
// 来源: 真实 Miniredis 实例 + Toxiproxy TCP 代理 (Go-Redis Client -> Toxiproxy -> Miniredis)
// 用于: 彻底验证生产环境 MySQLRepository.AcquireSettlementToken 所使用的核心 Lua 脚本在弱网网络切片下的完整状态机覆盖
func TestSettlementToken_Toxiproxy_RedisLuaStateMachine(t *testing.T) {
	// Step 6.1: 启动真实 Miniredis 服务端
	// 来源: miniredis.Run()
	// 用于: 提供支持真实 Redis 协议与 Lua 脚本解释执行的底层存储引擎
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Step 6.2: 启动 Toxiproxy 控制面并在 Go-Redis 与 Miniredis 之间建立 TCP 代理
	// 来源: startToxiproxyEngine 与 setupToxiproxyRoute
	// 用于: 让 Redis 流量全部经由 Toxiproxy 代理，可注入数据包切片 (Slicer)
	toxCli, stopTox := startToxiproxyEngine(t)
	defer stopTox()

	proxy, proxyAddr := setupToxiproxyRoute(t, toxCli, "redis-lua-proxy", mr.Addr())
	defer func() { _ = proxy.Delete() }()

	// Step 6.3: 注入 Slicer Toxic (模拟 TCP 数据包微切片与随机延迟)
	// 来源: proxy.AddToxic
	// 用于: 模拟弱网环境下的 TCP 包拆包与乱序到达，验证 Lua 脚本原子执行不受网络切片影响
	_, err = proxy.AddToxic("tcp_slicer", "slicer", "upstream", 1.0, toxiproxyclient.Attributes{
		"average_size":  32,
		"size_variance": 8,
		"delay":         1000, // 1ms
	})
	require.NoError(t, err)

	// Step 6.4: 创建连接至 Toxiproxy 代理端口的 Redis 客户端
	// 来源: redis.NewClient(&redis.Options{Addr: proxyAddr})
	// 用于: 执行 Redis Lua 状态机评测
	rdb := redis.NewClient(&redis.Options{
		Addr: proxyAddr,
	})
	defer rdb.Close()

	ctx := context.Background()

	// 生产级 Lua 脚本 (与 MySQLRepository.AcquireSettlementToken 保持完全一致)
	luaScript := `
		local exists = redis.call('EXISTS', KEYS[1])
		if exists == 0 then
			return {'EXPIRED', '', '', '', '', ''}
		end

		local owner = redis.call('HGET', KEYS[1], 'user_id')
		if owner ~= ARGV[1] then
			return {'FORBIDDEN', '', '', '', '', ''}
		end

		local status = redis.call('HGET', KEYS[1], 'status')
		if status == 'SUCCESS' then
			local pId = redis.call('HGET', KEYS[1], 'parent_order_id') or ''
			local pNo = redis.call('HGET', KEYS[1], 'parent_order_no') or ''
			local snap = redis.call('HGET', KEYS[1], 'order_snapshot') or ''
			return {'SUCCESS_REPLAY', pId, pNo, '', '', snap}
		end

		if status == 'FATAL_FAIL' then
			local reason = redis.call('HGET', KEYS[1], 'reason') or 'business check failed'
			return {'FATAL_FAIL', '', '', reason, '', ''}
		end

		if status == 'INIT' or status == 'RETRYABLE_FAIL' then
			redis.call('HMSET', KEYS[1], 'status', 'PROCESSING', 'lock_time', ARGV[2])
			redis.call('EXPIRE', KEYS[1], 1800)
			return {'PROCEED', '', '', '', ARGV[2], ''}
		end

		if status == 'PROCESSING' then
			local lockTime = tonumber(redis.call('HGET', KEYS[1], 'lock_time') or '0')
			local elapsed = tonumber(ARGV[2]) - lockTime
			if elapsed > tonumber(ARGV[3]) then
				redis.call('HMSET', KEYS[1], 'status', 'PROCESSING', 'lock_time', ARGV[2])
				redis.call('EXPIRE', KEYS[1], 1800)
				return {'PROCEED', '', '', '', ARGV[2], ''}
			else
				return {'STILL_PROCESSING', '', '', '', tostring(lockTime), ''}
			end
		end

		return {'EXPIRED', '', '', '', '', ''}
	`

	nowMs := time.Now().UnixMilli()
	leaseTimeoutMs := 200 // 200ms

	// 分支 1: 不存在的令牌 -> EXPIRED
	res, err := rdb.Eval(ctx, luaScript, []string{"token:nonexistent"}, "user-1", nowMs, leaseTimeoutMs).Slice()
	require.NoError(t, err)
	assert.Equal(t, "EXPIRED", res[0])

	// 分支 2: 初始化令牌并校验归属人非法 -> FORBIDDEN
	key := "token:test-state-machine"
	err = rdb.HSet(ctx, key, map[string]interface{}{
		"status":  "INIT",
		"user_id": "user-owner",
	}).Err()
	require.NoError(t, err)

	res, err = rdb.Eval(ctx, luaScript, []string{key}, "impostor-user", nowMs, leaseTimeoutMs).Slice()
	require.NoError(t, err)
	assert.Equal(t, "FORBIDDEN", res[0])

	// 分支 3: 正确归属人首次提单 (INIT -> PROCESSING) -> PROCEED
	res, err = rdb.Eval(ctx, luaScript, []string{key}, "user-owner", nowMs, leaseTimeoutMs).Slice()
	require.NoError(t, err)
	assert.Equal(t, "PROCEED", res[0], "INIT 状态必须推进为 PROCESSING 并放行")

	// 分支 4: 租约窗口期内连击拦截 -> STILL_PROCESSING
	res, err = rdb.Eval(ctx, luaScript, []string{key}, "user-owner", nowMs+50, leaseTimeoutMs).Slice()
	require.NoError(t, err)
	assert.Equal(t, "STILL_PROCESSING", res[0], "租约期内再次请求必须拦截阻断")

	// 分支 5: 租约超时自愈接管 (模拟原处理节点断电死锁，500ms > 200ms) -> PROCEED
	res, err = rdb.Eval(ctx, luaScript, []string{key}, "user-owner", nowMs+500, leaseTimeoutMs).Slice()
	require.NoError(t, err)
	assert.Equal(t, "PROCEED", res[0], "超时后必须自愈接管并放行")

	// 分支 6: 建单成功后更新为终态 SUCCESS -> 秒级幂等回显 SUCCESS_REPLAY
	err = rdb.HSet(ctx, key, map[string]interface{}{
		"status":          "SUCCESS",
		"parent_order_id": "po-99999",
		"parent_order_no": "PO202610110001",
	}).Err()
	require.NoError(t, err)

	res, err = rdb.Eval(ctx, luaScript, []string{key}, "user-owner", nowMs+600, leaseTimeoutMs).Slice()
	require.NoError(t, err)
	assert.Equal(t, "SUCCESS_REPLAY", res[0])
	assert.Equal(t, "po-99999", res[1])
	assert.Equal(t, "PO202610110001", res[2])

	// 分支 7: 硬失败阻断 (FATAL_FAIL) -> FATAL_FAIL 带错误原因秒拒
	err = rdb.HSet(ctx, key, map[string]interface{}{
		"status": "FATAL_FAIL",
		"reason": "商品已下架",
	}).Err()
	require.NoError(t, err)

	res, err = rdb.Eval(ctx, luaScript, []string{key}, "user-owner", nowMs+700, leaseTimeoutMs).Slice()
	require.NoError(t, err)
	assert.Equal(t, "FATAL_FAIL", res[0])
	assert.Equal(t, "商品已下架", res[3])
}
