# 优化与功能路线图

## 当前完成的工程基线

- .NET 10 / SDK 10.0.301（与 Server、RealtimeServices 对齐，见 `docs/sdk-baseline.md`）、
  集中依赖和严格分析器构建。默认发布为 **JIT + TieredPGO**（`PublishAot=false`）；
  协议 JSON 走源生成 `JsonSerializerContext`，便于日后重新启用 Native AOT。
- DirectSocket 固定池化缓冲区增量解析已成为默认入站路径；Pipelines 保留为配置回退。
  连接/速率/鉴权/空闲/发送超时和慢消费者隔离继续生效。
- 出站帧池化、用户会话快照和“条数 + 字节数”双重队列预算；全局入站预算
  （`GlobalInboundBudget`）覆盖 Socket/Pipe 暂存与 lane 池化 payload。
- JSON 编解码通过接口隔离，为后续二进制协议保留扩展点。
- TCP 网关已接入同级 RealtimeServices 的 NATS/JetStream 集成模块。
- 消息幂等持久化、事务 Outbox、每网关 durable consumer 和 ACK/NAK 已完成。
- 设备送达与用户已读回执已通过独立 JetStream Subject 接入。
- 回执状态与 Outbox 原子提交；已读隐式包含送达，重复/乱序不回退。
- 历史消息已使用 Core NATS request/reply 接入，不占用持久写入流。
- 历史分页使用 (UserId, ReceivedAtMs DESC, MessageId DESC) 复合索引和
  keyset cursor，不使用 OFFSET。
- TCP 已提供 MessageHistoryRequest(106) / MessageHistoryPage(107)；网关只采用
  已认证会话的 UserId。
- 单页最多 100 条，响应软预算 64 KiB（`WireResponseSoftLimit`）、硬上限
  `MaxPayloadSize`（80 KiB），查询工作器为有界并发。
- 群聊成员管理、撤回/编辑/反应、附件生命周期事件、Push Token 注册等协议面
  已部分落地；持续扩展时优先拆分 `TcpGatewayService` / `RealtimeEventDispatcher`。
- 真实跨进程探针已覆盖消息、已读回执、Outbox 和历史查询完整闭环。
- 已新增持久消息全链路负载工具，输出固定内存直方图及 JSON/Markdown 报告。
- History 查询队列深度和执行中数量已纳入运行时快照与 Meter 指标。
- NATS 连接/断线/重连失败、本地消息丢弃和慢消费者已纳入 Meter 指标。
- JetStream pending、delivery/redelivery、ACK in-flight、ACK 延迟和失败已按 consumer 采集。
- 隔离的真实 NATS 断线恢复演练已验证连接状态 1 -> 0 -> 1 和自动重连计数。
- 多进程编排器已统一启动/清理服务、执行双负载、采样进程与 Docker、输出指标前后差值。
- TCP 负载工具已补齐不包含令牌明文的 JSON/Markdown 结构化报告。
- 编排器的自动化 TCP 临时鉴权使用随机令牌和仅用户可读写的令牌文件；令牌不进入进程
  参数、报告或日志，测试结束自动删除令牌文件和缓存记录。
- 性能门禁在配置 TCP 负载时强制校验全部预期连接成功且 0 失败；Linux 双 Gateway
  安全组合回归已验证 pipeline 1,368/1,368、TCP 40/40、p95 101 ms。
- 首轮本机 30 分钟正式基线已通过：1000 个 TCP 长连接全部成功，持久链路
  94,715/94,715 成功、52.61 pipeline/s、p95/p99 437.5/441.5 ms，消息积压最终归零。
- 基准报告已补齐进程与容器内存起止/增量，并保存脱敏版本化 JSON 和 10% 同机复核线。
- Outbox 已实现容量 1 的事务提交主动唤醒并保留 200 ms 跨实例/恢复兜底轮询；
  相同 30 分钟 A/B 吞吐提升 120.2%，完整链路 p95/p99 降低 71.9%/62.6%，0 失败。
- History 已实现收件人/发件人索引分支有界 Top-N；真实短测数据上的扫描行从 1,535
  降至 11、缓冲页从 1,162 降至 17、执行时间从 6.055 ms 降至 0.071 ms。
- 固定速率容量曲线与 5 分钟确认已完成：目标 120/s 时持续实际 115.35/s、
  34,626/34,626 成功、p95/p99 174/214 ms；初始单节点运行预算为 80/s。
- 故障注入工具与首轮短测已完成：Garnet 568/568、PostgreSQL 修复后 575/575；
  PostgreSQL 短停不再终止宿主；NATS pause/unpause 短断线 513/513、最终积压归零。
- Runtime V2 Phase 1+2 已完成：
  - `TcpClientSession` 拆分为 `Outbound`（Durable FIFO + Ephemeral keyed mailbox + 两种发送驱动模型）与
    `Transport`（Close/Dispose、空闲/发送 deadline、SendFrameAsync）两个 partial。
  - 全局共享执行器替代每连接资源：`DeadlineWheel`（Auth/Send/Idle deadline）、
    `SessionCommandExecutor`（OrderedWrite/Query 共享 worker 池，按 connectionId 串行）、
    `OutboundPumpCoordinator`（OnDemandSendPump 共享出站 worker 池）。
  - `OutboundSendMode` A/B 切换：`PersistentSendLoop`（默认，每连接永久 SendLoop）vs
    `OnDemandSendPump`（共享 worker 池，CAS 调度，burst 上限 + 公平轮转）。
  - 新增 Runtime V2 可观测指标：`gateway.deadline_wheel.active_deadlines`、
    `gateway.outbound_pump.ready_queue.depth`、`gateway.outbound_pump.total_scheduled`、
    `gateway.outbound_pump.worker_count`（PersistentSendLoop 模式下后三者不注册）。
  - `SessionLifecycleCoordinator` 拆分为 `Presence` 与 `DeviceSession` 两个 partial，
    `GroupCommandHandler` 拆分为 `Create`/`Mutate`/`Helpers`，`TcpGatewayService` 由 ~1800 行降至 669 行，
    `RealtimeEventDispatcher` 由 1316 行降至 190 行（9 个独立 handler 的 facade）。
  - 路由接口收敛：`IGatewayDirectory.LookupOnlineGatewaysAsync` 返回
    `GatewayDirectoryLookupResult`（健康/降级显式区分），`IWatcherGatewayDirectory` 的
    Register/Unregister 增加 `gatewaySessionId` 参数以支持设备会话级 watcher 计数；
    Redis/InMemory 双实现、`PresenceCommandHandler` 与对应单测已同步更新。
  - `DeadlineWheel` 计数器正确性修复：新增 `_fired` 集合使 `Cancel` 在已触发注册上幂等忽略
    （修复重复递减导致计数为 -1）；`DisposeAsync` 清理桶与计数器（修复残留计数）。
  - `SessionCommandExecutor.StopAsync` 移除 `_workers.Length == 0` early-return，
    确保未启动 worker 时仍排空队列释放入站预算与池化缓冲区。
  - 单元测试 225/225 通过；本机短时 A/B 验证通过（100 连接 20s + 50 连接 30s 10msg/s chat，
    两种模式吞吐 ~498/s 一致，OnDemandSendPump 线程数 50 vs PersistentSendLoop 52）。
  - `scratch/Run-RuntimeV2-Soak-Linux.ps1` 作为 Linux 长测 runbook，含前置依赖清单与对比建议。
- **轻量 Actor Runtime 与 Ephemeral 流程接入已完成**：
  - 新增 BCL-only `ActorRuntime/`，采用分片单消费者、有界 MPSC 入口、intrusive ready queue、
    前 4 项内联 + `ArrayPool<T>` 溢出环形邮箱；核心热路径不使用 `Queue<T>`。
  - 仅在数组槽位寻址局部使用 `Unsafe.Add`，不持有裸指针；保留托管内存生命周期与
    `ArrayPool` 归还约束。
  - FIFO 使用生产者 mailbox credit，满载同步返回背压；completion 使用独立高优先级入口，
    deadline 可主动唤醒，`Drain` 覆盖入口、邮箱、busy actor 与异步操作。
  - 所有 replace/drop/fault/stop 路径均有资源释放回调；异步操作异常、超时、停机均可恢复
    suspended actor，避免入站预算与池化 payload 泄漏。
  - `EphemeralCommandPipeline` 已接入 `SessionRuntime`，由
    `UseActorRuntimeForEphemeralCommands` 控制新旧流程切换；当前配置启用，旧执行器仍可回退。
  - 新增 Actor Runtime gauges、单元/集成测试和独立微基准项目；设计与实测数据见
    `docs/actor-runtime.md`。
- **性能修复轮（P0 热路径）已完成**：
  - **心跳分桶真正分桶扫描**：`HeartbeatBucketRegistry` 按 connectionId/userId 分桶注册，
    每 tick 只枚举一个桶（O(N/bucketCount)），不再 `_sessions.Values.ToArray()` 全量扫描。
    连接桶与用户桶分离，同一用户的多连接在同一用户桶去重，避免一个周期内重复刷新。
    `HeartbeatCoordinator` 每秒刷新 ~333 用户（10k 连接 / 30 桶），Redis 流量平滑。
  - **心跳刷新指标正确区分成功/失败**：`RefreshLeaseAsync` / `RefreshPresenceAsync` 返回 bool，
    `HeartbeatCoordinator` 据此记录 success/failure metric，不再将失败记为成功。
  - **CommandContext 转为 readonly struct**：消除每命令堆分配，Dispatcher 直接传递值类型上下文。
  - **TypingFanoutHost 有界发布**：keyed pending dictionary + 单槽唤醒 channel + 固定 publisher worker，
    替代无界 `_ = PublishEphemeralTypingSafeAsync(evt)` fire-and-forget。NATS 故障时有界丢弃，
    本地 fanout 不被远端发布阻塞。
  - **ResponseByteBudget 边界修复**：新增 `TruncateOutcome` 枚举（Full/Truncated/ItemTooLarge/EnvelopeTooLarge）。
    `itemCount <= 0` 时校验空信封是否超过硬上限；单条 item 超过硬上限时返回 `item_too_large` 错误，
    避免返回 `HasMore=true, NextCursor=null` 的无法推进空页。History/ConversationList/SyncBootstrap
    三个调用方均处理 outcome 返回明确错误响应。
  - **Presence Redis 热路径优化**：
    - `SetOnline`/`SetOffline`/`Refresh` Lua 脚本移除 `ZREMRANGEBYSCORE`，改用 `ZCOUNT key (now +inf)`
      检测 0↔1 转换。热路径无写操作（原每次刷新都清理过期成员）。
    - `IsOnlineAsync` 从 2 次 Redis 往返（`ZREMRANGEBYSCORE` + `ZCARD`）降为 1 次（`ZCOUNT`）。
    - `GetOnlineManyAsync` 每用户从 2 条命令降为 1 条（`ZCOUNT`），batch 总命令数减半。
    - 新增 `RunMaintenanceAsync` + `PresenceMaintenanceService` 后台服务（默认 5 分钟周期），
      批量清理过期 ZSET 成员，回收崩溃实例残留内存。`_activeUsers` 跟踪避免 SCAN。
    - `RedisWatcherGatewayDirectory` 的 `RegisterWatchersAsync`/`UnregisterWatchersAsync`
      从串行 `await` N 次 Lua 改为 `batch.Execute()` 单次往返（最多 100 用户）。
  - **Realtime JetStream 分区并行消费**：`RealtimeEventConsumerService` 按
    `TargetUserId % PartitionCount` 路由到 N 个 `Channel<RealtimeEventDelivery>`，
    每分区单 worker 保证同一用户局部顺序，跨分区并行提升吞吐。
    `RealtimeEventPartitionCount` 默认 1（串行，向后兼容），生产建议 4～8。
    ACK 在 worker 内完成，channel 满时主循环阻塞触发 JetStream `MaxAckPending` 背压。
  - 单元测试 227/227 通过（+2 ResponseByteBudget 边界测试）。
- **功能完善轮（P1 业务闭环）已完成**：
  - **Resume 同步水位恢复**：`SessionLifecycleCoordinator.QueryResumeWatermarkAsync` 通过
    最小 `SyncBootstrapQuery`（`ListLimit=0`/`HistoryLimitPerConversation=0`/`MaxConversationsWithHistory=0`）
    查询 `ServerTimeMs` 作为 `LastConversationSequence` 写入 `ResumeResponse`，500ms 超时兜底 null
    （客户端回退"始终 SyncBootstrap"）。新增 `GatewayDependencyOperation.ResumeWatermarkQuery` 依赖操作枚举。
    配合既有 P0 的 Session epoch/fencing、Token 撤销、主动 Logout、被踢设备不可 Resume 闭环。
  - **群聊廉价结构校验补齐**：新增 `GroupCommandHandler.Validation.cs`（partial）集中校验逻辑——
    AddMembers 限制 50 成员上限/正 ID/去重；CreateGroup 限制 Title 128 字符 + 初始成员 200 上限/正 ID/去重；
    ChangeMemberRole 校验 `ConversationMemberRole` 枚举合法性后再转 Realtime Role；通用 Mutate 校验
    RequestId 64 字符 + ConversationId 128 字符。`OperationCanceledException` 在 Create/Mutate 中
    正确传播（断线/停机时不返回"服务不可用"误导客户端）。权限矩阵/群主转让/最后 Owner 退群等
    仍由 RealtimeServices 判定。
  - **Push Token 注册原子化**：`RedisPushTokenStore` 改用 Lua 脚本将 HSET + ZADD + 超限淘汰 + PEXPIRE
    合并为单次原子操作，消除并发注册下的上限与 TTL 竞态。Hash tag `{userId}` 确保 Hash 与 ZSET
    落在 Redis Cluster 同一 slot。FCM/APNs/WebPush Provider abstraction、仅离线推送、Collapse key、
    DLQ、无效 Token 自动删除等仍列为后续。
  - **Relationship 授权缓存主动失效**：`RelationshipListHandler` 在 `friendship`/`blocked-user` 变更时
    双向失效 `IDirectConversationAuthorizer` 缓存（`(actor,target)` 与 `(target,actor)`），避免 30s/10s
    缓存窗口内继续允许已禁止的 Typing/Presence 通知。`friend-request` 不失效（未建立关系）。
    ValueTask 经 `AsTask()` 安全 fire-and-forget（CA2012）。好友列表分页/请求接受拒绝等 TCP 产品面仍待补。
  - **协议版本与兼容性基础**：新增 `GatewayFeature` 定义 FeatureBits 与 `ProtocolPayloadFormat` 常量；
    `ServerHello.PayloadFormat` 改为类型化字段（当前固定 `Json`，为未来 Protobuf 协商预留）；
    `CommandDescriptor` 新增 `Deprecated` 标志 + `CommandCatalog.IsDeprecated` 方法支持弃用命令策略
    （标记弃用的命令返回 `UnsupportedCommand` 错误帧引导客户端迁移）。`ResumeRequest` 在 Catalog 中
    清理为 `ServerToClient`（非独立 wire 命令，Resume 通过 `ClientHello.ResumeToken` 字段触发）。
    后续已完成最低支持版本、FeatureBits 强制检查和命令级能力协商，详见
    `docs/protocol-capabilities.md`。
  - 单元测试 227/227 通过；dotnet build 0 警告/错误。

## Phase 3：直接 Socket 增量解析（短测门禁已完成）

Runtime V2 已实现 `InboundTransportMode.DirectSocket` / `Pipelines` 双路径。DirectSocket
使用每连接一个固定池化接收缓冲区；小帧和粘包原地解析，只有跨接收缓冲区的大 Payload
才租最终数组，并将后续 Socket 字节直接读入该数组。两种路径共用状态校验、速率限制、
lane 分发和资源释放逻辑。`Pipelines` 不是错误实现，仍作为生产快速回退路径保留。

### 目标状态机

```
Receive
  ↓
ReadHeader (固定接收缓冲区内原地解析)
  ↓
Validate Command / Direction / Payload Limit
  ↓
ReadPayload (仅跨 Receive 的 Payload 才租缓冲区)
  ↓
Inline or Queue
  ↓
Receive next frame
```

每连接只保存：固定池化接收缓冲区、读写游标，以及仅跨缓冲大包需要的 optional rented
payload buffer。入队后 payload 所有权直接转移给 `SessionCommand`，不再做第二次复制。

### 验证状态

已完成：

- 单元/集成回归覆盖半包 Header、分段认证、同一 TCP 写入内多帧粘包和 6 KiB 跨缓冲 Chat；
- 本机 Release 构建 0 警告/错误，259/259 测试通过；
- Linux 双 Gateway 短时 A/B：1000 个认证连接、每连接 20 heartbeat/s、每种模式 45 秒；
  两轮均 1000/1000 连接成功、0 失败；
- Pipelines / DirectSocket 吞吐为 19,937.94 / 19,934.12 heartbeat/s（-0.019%，持平），
  p99 为 8.75 / 8.32 ms（-4.99%），Gateway 总平均 CPU 为 7.21% / 5.91%（-17.96%），
  最大工作集为 331.2 / 320.1 MiB（-3.34%）；
- DirectSocket 单轮 p95 为 6.71 ms，较 Pipelines 的 5.93 ms 增加 0.78 ms；低于预设
  “相对 10% 或绝对 +1 ms 取较宽者”的短测噪声门槛。前一轮相同 TCP 负载中
  DirectSocket p95 反而更低，未观察到稳定的尾延迟回退；
- 短时门禁通过后，`DirectSocket` 已切为应用和性能工具默认值；配置
  `TcpGateway:InboundTransportMode=Pipelines` 可立即回退。
- 默认切换后的 Linux 真实集成短测通过：200 个认证 Heartbeat 连接全部成功，
  持久 Pipeline 1,216/1,216 成功、40.12/s、p95/p99 253.5/364.5 ms，
  JetStream/Outbox 最终积压均为 0。该测试同时发现并修复了 RealtimeServices
  `ResolveSyncWatermarksAsync` 在 Npgsql `SequentialAccess` 下先读第 6 列再回读第 5 列的错误。

收尾/发布前仍需与后续修改合并执行：

- 10,000 空闲连接
- 512 B Chat
- 64 KiB Chat
- 慢速发送攻击
- 全局入站预算耗尽
- 连接风暴
- 8～24 小时浸泡
- allocation/sec、GC 和每连接内存的稳定窗口对比

### 合并条件

最终删除 Pipelines 回退路径前，直接状态机还要证明（相对 Pipelines 基线）：

1. 工作集下降
2. 每连接内存下降
3. allocation/sec 下降
4. 吞吐不退化
5. p95/p99 不退化
6. 代码复杂度和漏洞面仍可接受

当前结论是“默认启用、保留回退”，不是删除 Pipelines。长测与大包/攻击场景会和后续
功能修改一起执行，避免每个小改动重复占用 Linux 长测窗口。脱敏结果见
`docs/performance-baselines/2026-07-28-linux-inbound-transport-ab.json`。

## 下一轮主目标：全链路性能与稳定性门禁

这轮完成后再进入大规模业务功能扩展。当前不应凭感觉继续微优化；先建立可重复的
性能基线和故障验收，才能判断 JSON、数据库、NATS 或 TCP 哪一层是真正瓶颈。

第一阶段已完成：现有 TCP 负载工具负责连接、心跳、扇出和慢消费者，新工具独立测量
消息写入、Outbox、已读回执和历史查询的持久链路，并生成机器可读报告。5 秒短跑仅用于
验证工具和指标闭环，不作为容量结论；执行规范与当前烟测结果见 `performance-baseline.md`。

### P1：多进程基准场景

已完成：一键编排器可启动并采样多个 TCP 网关和 RealtimeServices，把 TCP 与持久
链路负载、显式 NATS/PostgreSQL/Garnet 容器、Prometheus 前后快照纳入同一报告。

Outbox 尾延迟优化已经完成并通过相同 30 分钟 A/B。优化后 208,523 条完整链路全部
成功，吞吐 115.84 pipeline/s，p95/p99 为 123/165 ms。Gateway 和 RealtimeServices
工作集仍稳定，因此当前不应优先改 TCP 热路径或把 JSON 替换为二进制。

History SQL 的两个分支扫描放大已经消除。固定速率曲线显示 120/s 可在 5 分钟内
持续达到 115.35/s 且 0 失败；高档位存在明显非单调抖动，因此 252.08/s 只作为短时
峰值，不作为容量承诺。下一步转向依赖故障注入和恢复收敛验证。

待完成：

1. 用 `Run-ConversationCombo.ps1` 在 Linux 正式机复跑：会话历史翻页 + 列表/SyncBootstrap
   与 TCP chat 扇出/慢消费者并行；校准会话阶段 p95 阈值。须避开 Runtime V2 soak 时间窗。
2. 用生产近似的数据规模与资源限制复跑 8–24 小时浸泡，并校准告警阈值。
   首次 Runtime V2 8h 运行在 PersistentSendLoop 阶段退出（负载进程 code 137、
   Gateway 提前退出，未形成有效长测结论）；随后两种发送模式各 30 分钟验证均通过。
   当前 Linux 上无运行中的 soak。重跑前先修复长测进程存活/资源限制与失败取证，
   再按 working set、p95、JetStream pending、Outbox pending 决定默认发送模式。
3. 将版本化 JSON 的短期门禁接入定时 CI；硬件或拓扑变化时重建基线。
   （依赖 P1 CI 与发布门禁中的 Linux 自托管 runner 接入。）

验收标准：测试可一条命令重复执行；报告包含吞吐、端到端 p50/p95/p99、错误率、
CPU、工作集、分配、Gen2/LOH、TCP 排队字节、JetStream pending/重投和 Outbox pending。

### P1：可观测性与容量信号

已完成：

- Gateway 与 RealtimeServices 接入 OpenTelemetry Metrics/Tracing，并支持稳定版 OTLP 导出。
- W3C Trace Context 已贯穿 TCP 命令、NATS/JetStream、消费处理、PostgreSQL/Outbox 和回推事件；旧 JSON 事件仍可兼容反序列化。
- RealtimeServices 提供 Prometheus `/metrics`，JSON 快照迁移到 `/diagnostics/runtime`；Gateway 的预发布 HttpListener exporter 默认关闭。
- History 耗时/失败/队列深度、Outbox pending/最老消息年龄/最大尝试次数、运行时和 Npgsql Meter 已纳入采集。
- NATS 生命周期和 JetStream 投递/确认指标已接入；初始告警阈值见 `observability-alerts.md`。
- 已在 Linux 测试机部署 Prometheus、Grafana、初始告警规则与实时仪表盘（`deploy/observability/` 含
  `prometheus.yml` + `chatapp-realtime.yml` 规则 + `chatapp-realtime.json` 仪表盘 + docker-compose 编排），
  并用真实 RealtimeServices `/metrics` target 验证采集成功；绝对阈值待正式基准校准。

下一步：

1. 在 OTLP Collector 中统一转发 Prometheus/Trace，并校验跨进程 Trace 查询体验。
2. 为 Alertmanager 选择并配置实际通知通道；在此之前告警规则只负责暴露触发状态。
3. 日志只保留结构化故障信息，避免在聊天热路径恢复高频 Info 日志。

验收标准：一次消息可关联 Gateway -> NATS -> RealtimeServices -> PostgreSQL/Outbox；
关键容量瓶颈能从指标中定位，而不依赖临时加日志。

### P1：故障与恢复测试

已完成：

- NATS Core 客户端的隔离断线/重连指标演练（连接状态 1 → 0 → 1 与自动重连计数已验证）。
- 故障注入工具与首轮短测：Garnet 568/568、PostgreSQL 修复后 575/575；PostgreSQL 短停不再终止宿主；
  NATS pause/unpause 短断线 513/513、最终积压归零。

待完成：

- 滚动重启网关和 RealtimeServices，确认 durable consumer、Outbox 和客户端去重。
- 短暂断开 JetStream、PostgreSQL、Garnet，验证超时、退避、重投、积压收敛和恢复后无静默丢失。
- 注入慢客户端和超大历史页，确认有界队列、字节预算和连接隔离。
- 校验同毫秒消息的游标翻页不遗漏、不重复，重复请求结果稳定。

验收标准：依赖恢复后自动收敛；内存不随离线时长或慢客户端数量无界增长。

### P1：CI 与发布门禁

已完成：

- 跨平台 `ChatApp.Performance.Gate` 已实现：对编排器报告做失败闭环检查，拒绝缺失
  JetStream/Outbox 指标的报告；Linux 8 小时原始报告已复验通过。
- `ChatApp.Performance.Gate` 同时支持 `--require-conversation-stages`（history / list / sync_bootstrap）
  阶段 p95 闭环，缺失即失败。
- Native AOT 发布为可选实验（默认关闭，见 `AGENTS.md`），以 JIT/TieredPGO 吞吐测试决定是否重新启用。

待完成：

- 当前仓库未配置 Git 远程与 CI workflow（`.github/workflows/`、`.gitlab-ci.yml`、`azure-pipelines.yml`
  均不存在）。需要先注册 Linux 自托管 CI runner，再把 Release 构建、全部测试、数据库契约检查、
  真实 NATS/PostgreSQL 探针、定时浸泡与性能门禁接入 CI。
- 保存基准结果、门禁结果并比较历史版本，性能退化必须有明确说明。
- .NET 11 稳定版发布后，与 Server/RealtimeServices **同步**升级 SDK/依赖并重跑基线
  （当前基线为 .NET 10，见 `docs/sdk-baseline.md`）。

## 当前执行顺序

1. **已完成**：DirectSocket 增量解析、双路径回退、259 项回归和 Linux 1000 连接短时 A/B；
   默认值已从 Pipelines 切换为 DirectSocket。
2. **后续修改完成后统一长测**：使用 DirectSocket 默认路径执行 8～24 小时 soak，同时保留
   Pipelines 对照轮；覆盖 10k 空闲、Heartbeat、512 B/64 KiB Chat、慢速发送、预算耗尽和连接风暴。
   - 2026-07-28 已在 Linux 后台启动 8 小时 DirectSocket + PersistentSendLoop soak；
     1000 TCP 连接、80 pipeline ops/s、32 并发，先预热 300 秒。报告目录：
     `.artifacts/performance/soak-directsocket-20260728`。进程结束且报告通过前不作为正式基线。
3. **待办**：用 `Run-ConversationCombo.ps1` 在 Linux 正式机复跑：会话历史翻页 + 列表/SyncBootstrap
   与 TCP chat 扇出/慢消费者并行；校准会话阶段 p95 阈值。须避开 Runtime V2 soak 时间窗。
4. **待办**：注册 Linux 自托管 CI runner，并把 Release、测试、真实依赖探针、定时浸泡和性能门禁接入；
   当前仓库未配置 Git 远程，因此这是环境接入任务。
5. **待办**：配置 Alertmanager 的实际通知通道与 OTLP Collector 的 Trace 汇聚，完成从指标发现到告警响应的闭环。
6. **并行可推进**（无 Linux 资源冲突，soak 期间可同步推进）：
   - 按需继续拆分 oversized Gateway 类型（当前 `TcpGatewayService` 669 行、`GatewayMetrics` 492 行，
     均低于 600 行警戒线，非阻塞）。
   - 补充已迁移模块的单元测试：`OutboundPumpCoordinator` / `DeadlineWheel` / `SessionCommandExecutor`
     已有覆盖；`SessionRuntime` / `SessionControlHandler` / `TcpClientSession` 仍缺独立单测（与传输/codec 耦合，
     需抽象测试边界）。
   - ~~业务功能扩展（群聊权限边界、推送通知端到端、附件元数据闭环）按业务优先级评估。~~
     **本轮已完成 P1 业务闭环**（详见"性能门禁后进入的功能阶段 > 已完成"）：Resume 水位恢复、
     群聊廉价校验、Push Token 原子化、Relationship 授权缓存失效、协议版本兼容性基础。
     剩余深度功能（Push Provider abstraction、附件完整闭环、Relationship TCP 产品面、
     Resume 可靠性压力场景）仍按业务优先级评估；协议能力协商已在本轮完成。

## 性能门禁后进入的功能阶段

会话列表、未读、按会话历史、多设备 SyncBootstrap、群成员变更、撤回/编辑/反应与
附件生命周期事件契约已部分完成。

### 已完成（本轮 P1 业务闭环）

1. **Resume 同步水位恢复**：`ResumeResponse.LastConversationSequence` 通过 SyncBootstrap
   `ServerTimeMs` 填充，不再是 null。配合 P0 的 epoch/fencing、Token 撤销、主动 Logout、
   被踢设备不可 Resume 闭环。Gateway 已适配 Realtime 的 `changed_at_ms` 水位；
   v1 JSON 字段名暂时保留 `afterReceivedAtMs` 以避免破坏客户端。
2. **群聊廉价结构校验**：AddMembers/CreateGroup 成员数量/正 ID/去重、Title 长度、
   ChangeMemberRole 枚举合法性、RequestId/ConversationId 长度、OperationCanceledException 传播。
   权限矩阵/群主转让/最后 Owner 退群/Member 分页/RequestId 幂等/审计事件仍由 RealtimeServices 判定。
3. **Push Token 注册原子化**：Lua 脚本合并 HSET+ZADD+淘汰+TTL，Hash tag 对齐 Cluster slot。
4. **Relationship 授权缓存失效**：friendship/blocked-user 变更双向失效 Typing/Presence 缓存。
5. **协议版本与兼容性深化**：除 GatewayFeature 常量、PayloadFormat、Deprecated 策略外，
   已加入可配置最低客户端版本、实际版本/FeatureBits 交集、会话协商快照、
   `CommandCapabilities` 兼容开关和命令级 `RequiredFeature` 门控。未协商扩展命令返回
   非致命 `FeatureNotNegotiated`；DirectSocket/Pipelines × 两种出站模式均通过真实 TCP 测试。
   Resume 成功路径也会保存协商状态；未来二进制位仍不对外回显。
6. **Resume 可靠性补强**（2026-07-29 完成）：跨 Gateway 重连风暴防护、Redis 故障快速失败、
   旧设备被踢后立即重连的代次校验、Resume 路径可观测性闭环。
   - **Redis 应用层熔断器**（`IRedisCircuitBreaker` / `RedisCircuitBreaker`）：三状态机
     Closed→Open→HalfOpen，连续失败阈值由 `RedisOptions.CircuitBreakerFailureThreshold`
     控制（默认 5），开路时长由 `CircuitBreakerOpenDuration` 控制（默认 5s）。
     `RedisResumeTokenStore` / `RedisDeviceSessionLeaseStore` 所有 Redis 调用前检查熔断器，
     Open 状态快速失败返回 null/默认值，避免重连风暴串行排队 Redis 超时。
   - **RevokeAsync DemandMaster**：撤销 ResumeToken 的 `KeyDeleteAsync` 强制落在主节点，
     防止主从切换期间撤销失效导致旧 Token 在 TTL 窗口内被跨 Gateway 恢复。
   - **Resume 路径广播 SessionRevoked**：`TryResumeAsync` 接管设备租约发现跨 Gateway 旧
     SessionId 时，与 `ReplaceSameDeviceSessionsAsync` 一致地调用
     `PublishSessionRevokedEventAsync`，确保旧 Gateway 在事件到达前不会继续向已恢复
     session 发送出站帧。
   - **Resume 可观测性指标**：`gateway.resume.attempts` / `gateway.resume.succeeded` /
     `gateway.resume.failed`（tag: reason=invalid_token/redis_failure/circuit_open/lease_mismatch）/
     `gateway.redis.circuit_breaker.open`。`SessionLifecycleCoordinator.TryResumeAsync` 在入口、
     成功、各失败路径分别记录，用于识别 Redis 故障期间的快速失败比例与代次冲突。
   - **测试覆盖**：`RedisCircuitBreakerTests`（8 项状态机/线程安全）+
     `SessionLifecycleCoordinatorTests`（9 项 Resume 路径行为与指标验证）。共 287 项测试通过。
   - **仍待验证**：跨 Gateway 重连风暴的 Linux soak 压力测试（依赖真实 Redis 故障注入）。
7. **群聊 RequestId 幂等缓存**（2026-07-29 完成）：Gateway 层前置快速路径，
   避免客户端重试（网络抖动/超时重发）重复命中 Redis/NATS 往返。
   - **`GroupRequestIdempotencyCache`**：基于 `ConcurrentDictionary` 的有界 TTL+LRU 缓存，
     键为 `(ActorUserId, RequestId)`，默认容量 4096 条 / TTL 30 秒（约 ~800 KiB）。
     容量超限时先回收过期条目（CAS 防并发 sweep，10 秒间隔），仍超限则跳过缓存（自然背压）。
     `EvictUser` 支持用户级清理（登出/被踢）。线程安全：读取无锁，回收用 `Interlocked` CAS。
   - **缓存范围**：仅缓存 Realtime 正常返回的 `GroupConversationResult`（含业务失败如
     not_owner / member_limit_exceeded）；异常路径的 `group_unavailable` 不经过缓存，
     确保瞬态故障可重试。Realtime 侧 `ActorSessionId` 回声跳过仍是幂等主防线。
   - **集成路径**：`SendGroupCommandAsync`（AddMembers/RemoveMember/Leave/ChangeRole/
     ListMembers）和 `HandleCreateGroupRequestAsync`（CreateGroup）均在调用 Realtime 前
     检查缓存，命中时直接映射缓存结果返回；Realtime 返回后写入缓存。
   - **可观测性指标**：`gateway.group.idempotent.hit` / `gateway.group.idempotent.miss`，
     通过 `OnLookup` 回调绑定到 `GatewayMetrics`，用于观测客户端重试率与缓存命中率。
   - **测试覆盖**：`GroupRequestIdempotencyCacheTests`（14 项：TTL 过期、容量回收、
     用户级清理、幂等回调、跨用户隔离、覆写、业务失败缓存）。共 301 项测试通过。
   - **架构边界**：权限矩阵（Owner/Admin/Member）、群主转让、最后 Owner 退出、禁止移除群主、
     审计事件均为 RealtimeServices 侧职责，Gateway 不重复实现（见 `GroupCommandHandler.Validation.cs`
     注释）。Member 分页需要跨仓库扩展 `GroupConversationCommand`（当前无分页字段），
     本阶段不扩展协议框架。
8. **R1：真实 Ephemeral A/B 测试**（2026-07-29 完成）：覆盖三条 Ephemeral 管道在真实业务路径
   （授权 I/O + fanout + 资源释放）下的行为正确性，不再只测纯 Actor State 递增。
   - **三条管道并行验证**：
     - Legacy `SessionCommandExecutor`（全局无界 ready channel + per-connection 队列，共享 worker 阻塞授权 I/O）
     - Actor Generic Ephemeral Pipeline（per-connectionId Actor + FIFO mailbox + AsyncOperationExecutor，仍用 SessionCommand + ArrayPool）
     - Specialized TypingActor（per-(sender,target) Key + LatestOnly mailbox + DomainWorkLane 零装箱 + 缓存授权）
   - **10+1 场景全覆盖**（`tests/ChatApp.TcpGateway.Tests/Networking/TypingActorABTests.cs`，11 项）：
     1. 授权缓存命中——第二次 Notify 同 Key 跳过 I/O（CallCount 不变）。
     2. 授权缓存未命中——提交 I/O 并 Suspend，BusyActors 短暂为 1 后归零。
     3. 授权拒绝——丢弃 Notify，不发射 fanout。
     4. NATS 延迟——慢 Key（2s）不阻塞快 Key（800ms 内完成 fanout），用 CompletedCallCount
        区分 in-flight 与已完成，避免 CallCount 入口递增导致的误判。
     5. NATS 断线——授权抛异常后回投 denied Completion，Actor 恢复；重试 Notify 成功。
     6. 同 Key 高频覆盖——typing=true→false 在授权进行中到达，LatestOnly 合并；
        先等 AuthPending=true 再发第二条，避免 DrainIngress 一次性合并导致无发射。
        最终 fanout coalescing 仅保留最新状态（typing=false），符合"消费者只关心最终状态"设计。
     7. 连接 churn——空闲 Actor 被 IdleSweep 回收，ActiveActors 归零。
     8. 10,000 活跃 Actor——16 分片、4096 入站容量，10k 唯一 (sender,target) Key 全部授权完成。
     9. 单热点 Actor——100 条 Notify 轰炸同 Key + 1 条其他 Key，热点不阻塞其他；LatestOnly 合并到 1 次授权。
     10. 预算和 ArrayPool 归零——Generic Actor 路径 50 条 SessionCommand 处理后
         GlobalInboundBudget 归零、ArrayPool 缓冲全部归还、PendingAsyncOperations 归零。
     11. Legacy vs TypingActor 对比——5000 条同 Key Notify：Legacy 处理 5000 次；
         TypingActor 因 LatestOnly 合并仅触发 1 次授权 I/O（语义断言，非脆弱时序）。
   - **关键 Bug 修复**：
     - `BoundedMpscRing.TryDequeue` 多消费者竞态：CAS 推进 tail，避免 (0,0) 无效 Key 被处理。
     - `ReceiveAuthorizationCompleted` 主动 TryEmit：授权完成时若无新 Notify 到达，
       ResumeMailbox 无消息可恢复，必须在此主动发射当前 DesiredIsTyping。
   - **测试覆盖**：`TypingActorABTests` 11/11 通过，全套件 312/312 通过，0 警告 0 错误。
   - **下一步（R2）**：见下方"R2：连接级与协议级压力测试"。

### R2：连接级与协议级压力测试（下一轮）

R1 验证了 Ephemeral 三管道的业务正确性。R2 转向连接生命周期、协议组装与长时间稳定性：

1. **Receive Buffer 大小与动态升降级**：
   - 初始 512 B / 1 KiB Receive Buffer 下的小帧吞吐与分配。
   - 活跃连接动态升级到 4 KiB（基于帧大小阈值）。
   - 长时间空闲后降级回小 Buffer（释放 ArrayPool 槽位）。
2. **Header/Payload 装配 deadline**：
   - 半截 Header 超过 deadline 后连接被关闭（慢速/恶意客户端）。
   - 慢速 Payload（分多帧 TCP 段到达）超过 deadline 的处理。
3. **大小帧混合负载**：
   - 512 B Chat 与 64 KiB Chat 混合，验证 Outbound 队列预算与软/硬响应上限。
4. **空闲连接规模与稳定性**：
   - 10,000 空闲连接下的内存占用、心跳 Redis 流量平滑度、ThreadPool 压力。
5. **连接风暴**：
   - 短时大量连接涌入（如 1k/s 持续 10s）下的握手背压、Auth lane 容量、
     Inline lane 不被业务命令挤占。
6. **8～24 小时浸泡**：
   - Linux 长机执行（`scratch/Run-RuntimeV2-Soak-Linux.ps1`），
     覆盖内存泄漏、ThreadPool 饱和、Redis 连接池、NATS 重连、Actor IdleSweep 长期正确性。

### 后续按业务优先级再评估

1. ~~**Resume 完整测试**：跨 Gateway 重连风暴、Token 重放与并发使用测试；Resume 后触发
   增量 SyncBootstrap 的客户端策略验证。~~ **已基本完成**（见上"Resume 可靠性补强"），
   剩余 Linux soak 压力验证待安排。
2. **群聊产品面深度补齐**：~~RequestId 幂等~~（已完成，见上"群聊 RequestId 幂等缓存"）。
   Owner/Admin/Member 权限矩阵（Realtime 侧）、群主转让、最后一位 Owner 退出规则、
   禁止移除自己/群主、Member 列表分页（需跨仓库扩展 `GroupConversationCommand`）、
   成员数量与批量变更上限、审计事件（Realtime 侧持久层职责）。
3. **Push 端到端**：FCM/APNs/WebPush Provider abstraction、仅离线/无活跃设备时推送、
   会话静音/@mention/消息类型策略、Collapse key/idempotency、重试/DLQ/速率限制、
   Provider 返回无效 Token 时自动删除、Token 加密或最小权限保护。
   - **已完成（Gateway 侧基础设施）**：
     - `IPushTokenStore` / `RedisPushTokenStore`：原子化 Lua 注册/注销，Hash tag `{userId}`
       确保 Cluster 同 slot，90 天 TTL，超限淘汰最旧，多设备上限 8。
     - `PushTokenCommandHandler`：`deviceIdHash` 取自认证会话（不可伪造），Register 幂等覆写，
       Unregister 支持按设备/按 Token 两种模式，返回 `ActiveTokenCount`。
     - `PushPlatform` 枚举已包含 `Fcm=1` / `Apns=2` / `WebPush=3`。
     - Presence 基础设施：`IGlobalPresenceStore`（Redis ZSET 0↔1 转换）+
       `IGatewayDirectory.GetOnlineGatewaysWithStatusAsync`（区分 `UserOffline` vs `LookupFailure`），
       Publisher 可据此判断"用户全局离线 → 需要 Push"。
   - **仍需跨仓库实现（RealtimeServices / 独立 Push Service）**：
     - FCM/APNs/WebPush Provider client 与 payload builder。
     - Publisher 侧 Push 触发：`GetOnlineGatewaysWithStatusAsync` 返回 `UserOffline` 时，
       不投递 Gateway，而是入队 Push 任务（Pull tokens via `IPushTokenStore.ListAsync`）。
     - 仅离线/无活跃设备推送策略、静音/@mention/消息类型过滤。
     - Collapse Key、Retry/DLQ、速率限制。
     - Provider 返回无效 Token 时回调 `IPushTokenStore.UnregisterByTokenAsync`。
     - Token 加密或最小权限保护（当前 Token 明文存储于 Redis，需评估加密方案）。
     - **跨仓库依赖**：`IPushTokenStore` / `PushTokenRecord` 当前位于
       `ChatApp.TcpGateway.Core.Push`，RealtimeServices 无法直接引用。需提取到
       `ChatApp.Realtime.Abstractions` 或发布为独立 NuGet 包（见 AGENTS.md
       "Long-term: publish shared Realtime contracts as a versioned package"）。
4. **附件完整闭环**：上传凭证与所有权、上传完成 Finalize、扫毒/内容审核状态、
   消息发送时验证附件 Ready、临时附件过期清理、删除/失效通知、下载授权、
   消息撤回后附件保留策略。
   - **已完成（Gateway 侧）**：
     - `InboundPayloadEarlyValidator`：ChatMessage 入站前廉价结构校验（附件数 ≤32、
       ID 长度 1..64），不分配业务对象。
     - `MessagingCommandHandler.ChatMessage`：语义校验（空内容 + 无附件 = 协议违规），
       转发 `AttachmentIds` 至 Realtime。
     - `AttachmentLifecycleHandler`：消费 `AttachmentLifecycleChanged` 事件，
       向上传者本机会话推送 `AttachmentLifecycleUpdate`（Status/RejectReason/Thumbnail）。
     - `AttachmentWireMapper`：Realtime → wire `AttachmentRef` 映射（Bound → Available）。
     - `AttachmentRef` wire DTO：6 状态枚举（Scanning/Available/UploadConfirmed/Rejected/
       Expired/ThumbnailUpdated）+ DownloadApiHint/DownloadToken 字段。
   - **仍需跨仓库实现（RealtimeServices / Server HTTP API）**：
     - 所有权验证：`MessagingCommandHandler` 当前不校验 `AttachmentIds` 归属，由 Realtime
       `BindToMessageAsync` 拒绝非本人附件。如需 Gateway 前置校验需新增
       `IRealtimeMessageBus.VerifyAttachmentOwnershipAsync` 查询。
     - 上传 Finalize / Initiate：当前无 C2S 协议命令（仅有 S2C `AttachmentLifecycleChanged=154`）。
       需新增 `PacketCommand.AttachmentFinalizeRequest/Response` 等，且 Realtime 侧需
       `IRealtimeAttachmentStore.FinalizeUploadAsync`（Ticketed→Uploaded 转换）。
     - 扫毒/内容审核：`Scanning`/`Rejected` 状态的下游推送已就绪，但扫描触发与发布者
       在两个仓库之外（独立 worker / Server）。
     - 临时附件过期清理：`ix_attachments_unbound_age` 索引已存在（Migration012），
       但无 sweep worker。需 RealtimeServices 或独立服务定期扫描并发布 `Expired` 事件。
     - 下载授权：wire DTO 有 `DownloadApiHint`/`DownloadToken` 字段，但 Token 签发与
       校验在 Server HTTP API（`GET /api/attachments/{id}/download`），Gateway 不参与。
     - **Migration012 CHECK 约束过期**：`status IN (0,1,2,3)` 不包含 `Uploaded=4 /
       Scanning=5 / Rejected=6`，需新增 migration 放宽约束。
     - **枚举不一致**：Gateway `AttachmentWireStatus` 有 6 状态，
       Realtime Abstractions `AttachmentWireStatus` 仅 2 状态（Scanning/Available），
       `AttachmentRefMapper` 无法产出完整状态，需对齐。
5. **Relationship 产品接口**：好友列表分页、好友请求列表、接受/拒绝、拉黑/取消拉黑、
   Relationship version/watermark。
   - **已完成（Gateway 侧下游推送 + 授权缓存）**：
     - `RelationshipListChanged=153`（S2C）+ `RelationshipListHandler`：消费
       `FriendRequestListChanged` / `FriendListChanged` / `BlockedListChanged` 事件，
       向客户端推送 `RelationshipListChangedUpdate`（Resource/Action/ResourceId/Message）。
     - `IDirectConversationAuthorizer` 缓存主动失效：`RelationshipListHandler` 在
       `friendship` / `blocked-user` 变更时双向失效 `(actor,target)` 与 `(target,actor)`
       缓存（30s allow / 10s deny），避免 Typing/Presence 在缓存窗口内继续允许已禁止的通知。
       `friend-request` 不失效（未建立关系）。
   - **仍需跨仓库实现（RealtimeServices + Gateway 协议面）**：
     - C2S 协议命令：当前无 `RelationshipListRequest` / `FriendRequestSend` / `BlockUser` 等
       入站命令。新增 PacketCommand 值（159+ 范围）+ DTO + CommandCatalog 描述符 +
       `RelationshipCommandHandler` 属于 Gateway 填空（不扩展协议框架），但需要
       `IRealtimeMessageBus` 新增对应查询/命令方法。
     - RealtimeServices 侧域：无 `IRelationshipStore` / `IRelationshipQueryProcessor` /
       `IRelationshipCommandProcessor` / Postgres migration / NATS consumer。三个事件类型
       （`FriendRequestListChanged=1` / `FriendListChanged=2` / `BlockedListChanged=3`）
       已在枚举中预留但无发布者。
     - Relationship watermark：`ConversationSyncWatermark` 存在但仅限会话维度，
       需扩展为 Relationship 级别版本/水位用于增量同步。
6. ~~**协议版本与兼容性深化**：最低支持版本、FeatureBits 实际强制检查、命令级能力协商、
   Deprecated command 策略落地。~~ **已完成**。下一次协议升级只在新增 v2/binary 功能时继续，
   现阶段回到 Resume 可靠性、Push、附件和 Relationship 产品功能。
7. ~~将 `TcpGatewayService` / `RealtimeEventDispatcher` 拆成可独立测试的处理器模块。~~
   **已完成**：`TcpGatewayService` 由 ~1800 行降至 669 行（抽取 `TcpListenerHost` /
   `SessionLifecycleCoordinator` / `TypingFanoutHost` / `SessionRuntime` / `HeartbeatCoordinator` /
   `SessionControlHandler` / `CommandDispatcher`），`RealtimeEventDispatcher` 由 1316 行降至 190 行
   （9 个独立 handler 的 facade）。后续按需继续拆分 `TcpGatewayService` 剩余的 669 行与
   `GatewayMetrics` 492 行（均低于 600 行警戒线，非阻塞）。

## 二进制协议时机

开发阶段继续使用 JSON。只有全链路基准证明 JSON 是主要 CPU 或分配瓶颈后，再实现
二进制编码；升级必须通过协议版本或能力协商保留旧 JSON 客户端兼容性。
