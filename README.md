# ChatApp TCP Gateway

高性能、可限流、可观测的 TCP 长连接网关。当前基线为 **.NET 10 / SDK 10.0.301**
（与 ChatApp.Server、ChatApp.RealtimeServices 对齐，详见 [docs/sdk-baseline.md](docs/sdk-baseline.md)）。
默认使用 JSON 载荷；协议与传输层已经和具体序列化实现解耦。

## 项目结构

- Core/Authentication：实时鉴权契约和鉴权结果。
- Core/Messaging：网络业务消息模型；History 子目录放置历史分页协议。
- Core/Protocol：固定包头、命令、帧解析与写入规则。
- Core/Serialization：可插拔载荷编解码契约。
- Infrastructure/Authentication：AccessToken 查询和鉴权实现。
- Infrastructure/Caching：Redis/Garnet 连接生命周期及缓存键约定。
- Infrastructure/Serialization/Json：AOT 安全的 JSON 源生成编解码。
- Gateway/Configuration：TCP 运行参数。
- Gateway/Diagnostics：System.Diagnostics.Metrics 指标。
- Gateway/Networking：监听服务、会话、发送队列和池化帧。
- Gateway/Messaging：JetStream 事件消费、在线分发、消息回执和会话撤销。
- ../ChatApp.RealtimeServices/ChatApp.Realtime.Integration：共享 NATS/JetStream 契约与客户端。
- tests/：协议、序列化和内存所有权测试。
- tools/ChatApp.TcpGateway.LoadGenerator：连接、心跳、扇出和慢消费者负载工具。
- tools/ChatApp.Realtime.PipelineLoadGenerator：持久消息、回执和历史查询链路负载工具。
- tools/ChatApp.Performance.Orchestrator：多 Gateway/RealtimeServices、双负载与资源采样的一键编排器。

项目内依赖方向为 Gateway -> Infrastructure -> Core。Core 不依赖日志、Redis
或宿主框架。跨进程消息直接复用同级 RealtimeServices 的 Integration/Abstractions，
TCP 网关不实现消息数据库或 Outbox。消息送达/已读状态也由 RealtimeServices
通过独立的 chat.message-receipts Subject 持久化并发布状态事件；历史读取通过
chat.message-history.query 的 Core NATS request/reply 完成。

## 性能与稳定性设计

- 每个连接拥有“条数 + 字节数”双重上限的发送 Channel，单写循环保证消息顺序。
- 默认每连接最多排队 256 条且不超过 256 KiB；超限时断开慢消费者。
- 用户多终端快照只在连接增删时复制，聊天转发热路径不再逐消息分配数组。
- 出站帧使用 ArrayPool，并通过引用计数在多终端之间共享。
- JSON 直接写入最终帧，不再先创建 JSON byte[] 再复制到网络包。
- Pipe 输入缓存具有 pause/resume 阈值，单连接内存可控。
- 连接数、鉴权时间、空闲时间、发送时间、每秒包数与每秒入站字节数均有上限。
- 帧解析后、反序列化/NATS 投递前拒绝超限 payload，并对 ChatMessage 做附件数量等廉价结构早检。
- 热路径不写 Info 文件日志；运行数据通过 Meter 指标输出。
- Redis/Garnet 在 TCP 服务之前启动，宿主停止时按顺序释放。

## 序列化扩展

业务层只依赖 IPayloadCodec<T>。当前 JsonPayloadCodec<T> 使用
JsonSerializerContext（源生成 JsonTypeInfo）。默认发布为 JIT + TieredPGO；
Native AOT 可选，见 [AGENTS.md](AGENTS.md)。

增加二进制协议时：

1. 在 Infrastructure/Serialization 下新增 Binary 目录。
2. 为每个消息类型实现 IPayloadCodec<T>。
3. 在 InfrastructureServiceCollectionExtensions 中替换对应注册。
4. 保持 Core/Protocol 和 Gateway/Networking 不变。

如果未来需要 JSON 与二进制客户端同时在线，再在包头中加入协议版本或通过
登录握手协商格式；这属于协议版本升级，不能直接改变现有 10 字节包头。

## 配置

appsettings.json 的 TcpGateway 节控制监听地址、连接上限、Pipe 阈值、
发送队列条数、MaxOutboundQueuedBytes 字节预算、超时、MaxPacketsPerSecond、
MaxInboundBytesPerSecond、MaxInboundPayloadBytes 与 MaxChatAttachments。Redis 节控制
Garnet/Redis 地址和启动超时。RealtimeIntegration 节控制 NATS Subjects、Streams、
durable consumer、MessageReceiptsSubject、MessageHistoryQueriesSubject 与查询超时；
InstanceId 必须单实例唯一且重启稳定，网关的
`ManageStreams` 必须保持 false。队列的实时条数、字节数及拒绝原因分别通过
`gateway.outbound.queued.frames`、`gateway.outbound.queued.bytes` 和
`gateway.outbound.rejected` 指标暴露。

## 可观测性

两个进程均接入 OpenTelemetry Metrics/Tracing，跨 NATS/JetStream 和 Outbox 使用 W3C
`traceparent`/`tracestate` 继续同一条 Trace。Gateway 与 RealtimeServices 都可通过
`Observability:OtlpEnabled=true` 把指标和 Trace 发到 OTLP Collector；生产环境以 OTLP
为主，采样率由 `TraceSampleRatio` 控制。

Gateway 的独立 Prometheus HttpListener 仅保留给本地诊断，默认关闭；该 exporter 仍是
预发布开发组件，不作为稳定性门禁。RealtimeServices 使用 Kestrel
提供 `GET /metrics` 的 Prometheus 文本端点，并把原 JSON 快照移动到
`GET /diagnostics/runtime`。Outbox pending、最老待发布消息年龄、最大尝试次数、历史查询
队列/执行中数量、NATS 连接/重连、JetStream pending/redelivery/ACK、运行时和 Npgsql
连接池指标均进入 OpenTelemetry Meter。首轮阈值与仪表盘要求见
[可观测性与告警基线](docs/observability-alerts.md)。

AccessToken 缓存键保持现有约定：

    cache:AT:{SHA256_HEX_TOKEN}

缓存值位于 Hash 的 value 字段。

## 构建与验证

基线 SDK 见 `global.json`：固定为 .NET 10 SDK 10.0.301（`allowPrerelease: false`），
目标框架 `net10.0`。所有项目（Core / Infrastructure / Observability / Gateway / Host / Tests）
共享同一 SDK 与 TFM，不引入 preview。

    dotnet restore ChatApp.TcpGateway.sln
    dotnet build ChatApp.TcpGateway.sln -c Release
    dotnet test ChatApp.TcpGateway.sln -c Release
    dotnet publish ChatApp.TcpGateway.csproj -c Release -r win-x64 --self-contained true

连接风暴烟雾测试：

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode connection --connections 1000 --duration-seconds 5

鉴权后心跳 RTT 测试：

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode heartbeat --connections 100 --duration-seconds 30 --token "<access-token>" --messages-per-second 10

聊天扇出和慢消费者测试：

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode chat --connections 100 --duration-seconds 30 --token "<access-token>" --messages-per-second 10 --payload-bytes 512 --slow-readers 5

非法包断开测试：

    dotnet run --project tools/ChatApp.TcpGateway.LoadGenerator -c Release -- --mode invalid-packet --connections 100 --duration-seconds 10

完整参数和多用户用法见
[TCP 负载生成器说明](tools/ChatApp.TcpGateway.LoadGenerator/README.md)和
[持久链路负载生成器说明](tools/ChatApp.Realtime.PipelineLoadGenerator/README.md)。
跨项目消息语义见[消息链路说明](docs/realtime-message-flow.md)，性能测试方法见
[性能基线说明](docs/performance-baseline.md)，一键组合基准见
[多进程编排器](tools/ChatApp.Performance.Orchestrator/README.md)，后续功能与验收标准见
[优化路线图](docs/optimization-roadmap.md)。
