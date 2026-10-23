# Order Microservice (订单交易微服务)

基于 [Kratos v2](https://github.com/go-kratos/kratos) 与 gRPC 框架构建的高性能、分布式订单与交易核心系统。独立承载多租户购物车管理、防重结算令牌、多商家跨店铺拆单、DTM Saga 分布式库存预占、Redis ZSet 超时关单与 SKU 级售后链路。

---

## 🌟 核心架构与功能特性

1. **防重结算令牌架构体系 (Settlement Token Design)**：
   - 包含完备的架构设计文档与实操演练指南（详见 [docs/settlement-token-design.md](docs/settlement-token-design.md)）；
   - 涵盖两道防线体系（Redis Lua 原子状态机 + MySQL 唯一索引兜底）、确定性主单号推导、CAS 租约心跳续期与纯内存快照秒级回显，并已完整归档全套 Toxiproxy 混沌工程测试。

2. **跨商家拆单引擎 (Order Splitting Pipeline)**：
   - 4 阶段拆单流水线：商品按商家租户分组 -> 平台/商家满减优惠按比例分摊 -> 舍入分厘差额末位平账 -> 派生主子订单与明细数据；
   - 主订单（`ParentOrder`）关联全局支付与买家视图，子订单（`ChildOrder`）绑定独立商家履约。

3. **高可用库存预占与分布式事务**：
   - 支持 DTM Saga 分布式事务预占库存，并在故障时自动执行逆向补偿回滚；
   - 具备本地直连补偿降级机制，防止弱网下库存泄漏。

4. **超时自动关单 Worker (Order Expiration)**：
   - 基于 Redis ZSet 实现高吞吐毫秒级延迟队列；
   - 监听未支付订单超时事件，原子触发主动关单与跨服务库存释放；
   - 支付成功时主动执行 `ZREM` 移出延迟队列，避免临界区关单竞态。

5. **多租户与细粒度安全授权**：
   - 内置多租户（Tenant Isolation）上下文传递拦截器；
   - 集成 Casbin RBAC 细粒度资源鉴权与数据行级所有权校验（Ownership Check）。

---

## 📂 项目工程目录结构

```text
D:\repo\order
├── api/
│   └── proto/                     # Protocol Buffer 接口定义契约
│       ├── google/api/            # Google HTTP API 契约扩展
│       ├── inventory/v1/          # 库存服务契约
│       ├── order/v1/              # 订单服务契约
│       ├── product/v1/            # 商品服务契约
│       └── wallet/v1/             # 钱包微服务契约
├── cmd/
│   └── order/                     # 服务程序入口
│       ├── main.go                # Kratos 生命周期主程序
│       ├── wire.go                # Wire 依赖注入声明
│       └── wire_gen.go            # 编译期自动生成的依赖注入实现
├── configs/
│   └── config.yaml                # 服务运行配置（MySQL、Redis、gRPC端口、下游地址）
├── gen/                           # Protobuf 自动生成的 gRPC 客户端/服务端代码与 Swagger
│   ├── inventory/v1/
│   ├── order/v1/
│   ├── product/v1/
│   └── wallet/v1/
├── internal/                      # 内部业务逻辑（不对外暴露）
│   ├── biz/                       # 核心业务领域逻辑层
│   │   ├── biz.go                 # Wire ProviderSet
│   │   ├── fulfillment.go         # 履约适配
│   │   ├── order.go               # 订单领域实体与状态机
│   │   ├── order_repo.go          # 订单领域仓储接口
│   │   └── splitter.go            # 拆单引擎与优惠平摊
│   ├── conf/                      # 配置文件 Protobuf 映射对象
│   ├── data/                      # 数据持久化与基础设施交互层
│   │   ├── clients.go             # 下游微服务 gRPC 客户端连接池
│   │   ├── data.go                # 数据库/Redis 统一挂载与 Wire ProviderSet
│   │   ├── dtm_fulfiller.go       # DTM 分布式事务履约器
│   │   ├── expiration_worker.go   # Redis ZSet 超时关单轮询 Worker
│   │   ├── memory.go              # 本地内存兜底仓储
│   │   └── order.go               # GORM MySQL 仓储实现
│   ├── pkg/                       # 通用基础库
│   │   ├── authz/                 # 数据所有权校验与身份解析
│   │   ├── paging/                # 分页规整器与边界保护
│   │   ├── rbac/                  # Casbin RBAC 拦截器
│   │   └── tenant/                # 多租户 Context 拦截器
│   ├── server/                    # gRPC 传输层
│   │   ├── grpc.go                # Kratos gRPC Server 初始化与中间件挂载
│   │   └── server.go              # Wire ProviderSet
│   └── service/                   # 订单微服务门面实现（API 入口适配）
│       ├── order.go               # 实现 orderv1.OrderServiceServer 接口
│       └── service.go             # Wire ProviderSet
├── docs/                          # 架构设计与技术文档
│   └── settlement-token-design.md # 防重结算令牌架构方案与实操指南
├── migrations/                    # MySQL 数据库迁移脚本
│   ├── 20260929_dtm_barrier.sql
│   └── 20260929_tenant_isolation.sql
├── deployments/                   # 部署配置
│   └── docker/
│       └── order.Dockerfile      # 生产容器镜像构建文件
├── .gitignore
├── Dockerfile
├── go.mod                         # Go 依赖模块声明 (module order)
├── go.sum                         # 依赖指纹
├── Makefile                       # 项目构建指令集
└── README.md                      # 项目说明文档
```

---

## 🛠️ 快速开始

### 1. 环境依赖
- Go 1.26+
- MySQL 8.0+
- Redis 6.0+
- DTM 分布式事务管理器（可选，生产开启）

### 2. 本地依赖检查与构建
```bash
# 整理并验证依赖
go mod tidy

# 编译生成二进制可执行文件
make build
# 或者直接：
go build -trimpath -o bin/order.exe ./cmd/order
```

### 3. 运行自动化测试
工程已覆盖完备的单测、内存数据库事务回归测试与 Toxiproxy 弱网混沌测试：
```bash
# 执行全部包测试
go test -v ./...

# 专门测试拆单与结算防重
go test -v ./internal/service/...
```

### 4. 启动服务
```bash
# 启动 Order gRPC 微服务（默认监听 50054 端口）
./bin/order.exe -conf configs/config.yaml
```

---

## 🔌 核心 gRPC 接口一览

- **购物车接口**：
  - `GetCart` / `AddCartItem` / `UpdateCartItem` / `RemoveCartItem` / `ClearCart`
- **下单与拆单**：
  - `CreateOrder`: 执行跨商家拆单，调用 DTM Saga 预占库存并入库持久化主子订单与明细项。
- **订单生命周期管理**：
  - `GetOrder` / `ListOrders`: 订单状态与明细查询（带租户所有权过滤）。
  - `PayOrder`: 订单支付成功后推进主子订单状态为 PAID 并移出超时延迟队列。
  - `CancelOrder`: 手动取消订单并释放库存。
  - `ConfirmReceipt`: 确认收货并驱动结算履约。
- **售后流程**：
  - `ApplyAfterSales` / `AuditAfterSales` / `CompleteAfterSales`: SKU 级正反向售后流转。
