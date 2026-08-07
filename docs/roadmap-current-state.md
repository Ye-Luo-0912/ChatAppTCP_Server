# 当前状态

本文件描述系统**当前**的真实状态，供 Agent 和人类决策依据。历史变更见
`roadmap-changelog.md`，待办见 `roadmap-todo.md`。本文件随代码演进同步更新，
不保留已过期数字。

## 工程基线

- **SDK**：.NET 10 / SDK 10.0.301（`global.json` 锁定，`allowPrerelease: false`），
  与 RealtimeServices 对齐；见 `docs/sdk-baseline.md`。
- **发布模式**：默认 **JIT + TieredPGO**（`PublishAot=false`）。Native AOT 为可选实验，
  StackExchange.Redis 等依赖仍存在 trim/AOT 警告，未重新启用。
- **JSON 序列化**：协议/存储 JSON 全部走源生成 `GatewayJsonSerializerContext`，
  不使用反射 `JsonSerializerOptions`，为未来重新启用 AOT 保留可能。
- 构建/测试：`dotnet build` 0 警告 0 错误；Gateway 全量测试 **517** 项中
  **516** 通过、**1** 项外部 Redis 环境测试跳过；RealtimeServices 全量测试 **285/285** 通过。

## 架构边界

依赖方向：**Gateway → Infrastructure → Core**，**Observability** 为叶依赖被
Infrastructure 和 Gateway 共享，且只依赖协议包与 Logging。跨进程消息通过仓库本地
feed 中的 `ChatApp.Realtime.Contracts 2.3.0` / `ChatApp.Realtime.Integration 3.0.0`
版本化包引用；所有项目有锁文件，独立克隆可以 locked restore/build。
完整边界表见 `AGENTS.md`。

## 协议不变量

1. 固定 10 字节包头；payload codec 可插拔（`IPayloadCodec<T>`），当前线上格式为
   camelCase JSON（源生成 `GatewayJsonSerializerContext`）。
2. `ClientHello.featureBits` opt-in 兼容；命令级 feature 强制仅在
   `CommandCapabilities` 协商后生效。`CommandCatalog.RequiredFeature` 与
   `GatewayFeatureSet.Implemented` 必须同步。
3. 连接状态机（严格串行于读循环 **Inline** lane）：
   `ClientHello` → `ServerHello`（或 Resume 成功 → 已认证）→
   `AuthenticationRequest`（Resume 已认证则跳过）→ 业务命令（仅 `IsAuthenticated` 后）。
4. `RequireClientHello=true`（默认）时，握手前 `AuthenticationRequest` 为致命协议违规。
5. `ClientHello` / `AuthenticationRequest` / `Heartbeat` / `PresenceUnwatch` 为 **Inline**，
   永不 OrderedWrite，防止多帧 TCP 段重排握手与鉴权。
6. 响应预算：软上限 `PacketProtocol.WireResponseSoftLimit`（64 KiB）；
   硬上限 `MaxPayloadSize`（80 KiB）。

## 运行时默认值

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `InboundTransportMode` | `DirectSocket` | 固定池化缓冲区增量解析；`Pipelines` 为回退路径 |
| `OutboundSendMode` | `PersistentSendLoop` | 每连接永久 SendLoop；可选 `OnDemandSendPump`（共享 worker 池）或 `PerSessionDrain`（按需 per-connection drain） |
| `PublishAot` | `false` | JIT + TieredPGO |
| `RequireClientHello` | `true` | 握手前鉴权为致命违规 |
| `ReplaceSameDeviceSession` | 见 options | 同设备登录替换 |
| `EnableEphemeralPresenceAndTyping` | 见 options | Specialized Typing 路径开关 |
| `UseActorRuntimeForEphemeralCommands` | 见 options | Actor Runtime 与旧执行器切换 |

## 当前文件规模

| 文件 | 行数 | 备注 |
|------|------|------|
| `Gateway/Networking/TcpGatewayService.cs` | 869 | 仍为最大 Gateway 文件，但已抽取多个协作类型 |
| `Observability/Metrics/GatewayMetrics.cs` | 871 | 含心跳、Push、Resume、群组幂等等多域指标 |
| `Gateway/Networking/Sessions/TcpClientSession.cs` | 491 | partial：主文件 + Outbound(649) + Transport(208) |
| `Gateway/Networking/Sessions/HeartbeatCoordinator.cs` | 349 | 分桶扫描 + 固定 Worker 池 |
| `Gateway/Messaging/RealtimeEventDispatcher.cs` | 188 | 9 个独立 handler 的 facade |

## 已完成且生效的能力

### 传输与会话

- DirectSocket 增量解析为默认入站路径；Pipelines 保留为配置回退。
- 全局共享执行器替代每连接资源：`DeadlineWheel`（Auth/Idle deadline）、
  `SessionCommandExecutor`（OrderedWrite/Query 共享 worker 池，按 connectionId 串行）、
  `OutboundPumpCoordinator`（OnDemandSendPump 共享出站 worker 池）。
- **`_fired` 已移除**：`DeadlineWheel` 不再维护 `_fired` + `_cancelled` 双 HashSet
  （原长期运行内存泄漏源），改用分桶 + 计数器。发送超时由独立 `SendTimeoutTracker`
  周期扫描，不为每帧创建闭包或竞争全局锁。帧装配超时由 `FrameAssemblyTimeoutTracker` 管理。
- 出站三种模式：`PersistentSendLoop`（默认）/ `OnDemandSendPump` / `PerSessionDrain`
  （零分配 CAS，`DrainOperation` 复用 `ManualResetValueTaskSourceCore<bool>`）。
- 心跳分桶扫描：`HeartbeatBucketRegistry` 按 connectionId/userId 分桶，每 tick 仅枚举一桶；
  固定 Redis Worker 池 + 有界 Channel 背压。队列深度/最老项年龄/schedule_lag/overdue/
  full_cycle 指标已接入。
- 连接/速率/鉴权/空闲/发送/帧装配超时和慢消费者隔离生效。
- 全局入站预算（`GlobalInboundBudget`）覆盖 Socket/Pipe 暂存与 lane 池化 payload；
  全局出站预算（`GlobalOutboundBudget`）按"条数 + 字节数"双重队列。

### Actor Runtime（轻量）

- BCL-only `ActorRuntime/`：分片单消费者、有界 MPSC 入口、intrusive ready queue、
  前 4 项内联 + `ArrayPool<T>` 溢出环形邮箱。核心热路径不使用 `Queue<T>`。
- ActorCell 拆分为 `FifoActorCell`（inline 4 + ArrayPool）+ `LatestActorCell`（单消息替换）+ `StateOnlyActorCell`。
- 三条 Ephemeral 管道并行可用：Legacy `SessionCommandExecutor`、
  Actor Generic Ephemeral Pipeline、Specialized TypingActor（per-(sender,target) Key + LatestOnly）。
- 设计与实测见 `docs/actor-runtime.md`。

### 持久化与集成

- 消息幂等持久化、事务 Outbox、每网关 durable consumer、ACK/NAK 已完成。
- 设备送达与用户已读回执通过独立 JetStream Subject 接入；
  回执状态与 Outbox 原子提交，已读隐式包含送达，重复/乱序不回退。
- 历史消息走 Core NATS request/reply，不占持久写入流；
  (UserId, ReceivedAtMs DESC, MessageId DESC) 复合索引 + keyset cursor，不用 OFFSET。
- Outbox 容量 1 主动唤醒 + 200ms 跨实例兜底轮询。
- Realtime JetStream 分区并行消费（`RealtimeEventPartitionCount`，默认 1）。

### 协议与能力协商

- `GatewayFeature` 定义 FeatureBits 与 `ProtocolPayloadFormat` 常量。
- 可配置最低客户端版本、实际版本/FeatureBits 交集、会话协商快照、
  `CommandCapabilities` 兼容开关、命令级 `RequiredFeature` 门控。
- `CommandDescriptor` 含 `Deprecated` 标志，弃用命令返回 `UnsupportedCommand`。
- 详见 `docs/protocol-capabilities.md`。

### Resume（已完成事务化）

- Resume 同步水位恢复：`ResumeResponse.LastConversationSequence` 通过 SyncBootstrap
  `ServerTimeMs` 填充（500ms 超时兜底 null）。
- Redis 应用层熔断器（`IRedisCircuitBreaker`）：Closed→Open→HalfOpen 三状态机，
  `RedisResumeTokenStore` / `RedisDeviceSessionLeaseStore` 所有 Redis 调用前检查。
- `RevokeAsync` DemandMaster，防止主从切换期间撤销失效。
- Resume 路径广播 `SessionRevoked`，跨 Gateway 旧 session 不继续发送出站帧。
- Resume 可观测性：`gateway.resume.attempts/succeeded/failed`（tag: reason）、
  `gateway.redis.circuit_breaker.open`。
- **事务化已完成（8 项全部实现）**：Token Claim/Commit/Abort（Lua 原子）、
  AdmissionState 三态（Unauthenticated/Promoted/Released）、TakeOver 顺序（先 TakeOver 后关旧连接）
  与回滚（`RollbackResumeLocalStateAsync`）、DependencyUnavailable 可重试、
  同设备 fencing（确定性 `DeviceIdHash`）、旧 Socket 关闭验证（`ReadClosed`/`WriteClosed`，800ms 传播）、
  `SessionRevokedPayload` 结构化（`{ transportId }`）。
  见 `roadmap-changelog.md` 2026-08-01 条目。
### Push（Gateway 侧已完成，跨仓库待补）

- `IPushTokenStore` / `RedisPushTokenStore`：原子化 Lua 注册/注销，Hash tag `{userId}`
  确保 Cluster 同 slot，90 天 TTL，超限淘汰最旧，多设备上限 8。
- `PushTokenCommandHandler`：`deviceIdHash` 取自认证会话（不可伪造），Register 幂等覆写，
  Unregister 支持按设备/按 Token。
- `PushPlatform` 枚举：`Fcm=1` / `Apns=2` / `WebPush=3`。
- Presence 基础设施：`IGlobalPresenceStore`（Redis ZSET 0↔1 转换）+
  `IGatewayDirectory.GetOnlineGatewaysWithStatusAsync`（区分 `UserOffline` vs `LookupFailure`）。
- **Gateway 侧闭环已全部完成**：配置 Fail-fast（`Push.Enabled` 默认 false / `ProviderMode` 校验）、
  `PushDispatchDisposition`（NoTargets/FullySucceeded/PermanentlyCompleted/Retryable/PartiallyRetryable）、
  Token Retry（仅重试失败 Token）、幂等（复合键 UserId+CommandKind+RequestId+CanonicalPayloadHash + Redis L2）、
  DLQ、Provider 并发限制、无效 Token 注销、PushWorker 拆出（独立服务隔离网络资源）、
  AES-GCM Token 加密、Push delivery payload 不在 Information 级别记录。
- **跨仓库待补**：真实 FCM/APNs/WebPush Provider、`IPushTokenStore` 提取到共享 Abstractions、
  Publisher 侧 Push 触发（`UserOffline` 入队），见 `roadmap-todo.md` 主线一。
### 群组

- 廉价结构校验（`GroupCommandHandler.Validation.cs`）：成员上限/正 ID/去重、Title 长度、
  枚举合法性、RequestId/ConversationId 长度、`OperationCanceledException` 传播。
- RequestId 幂等缓存（`GroupRequestIdempotencyCache`）：有界 TTL+LRU，4096 条/30s，
  仅缓存正常返回（含业务失败），异常路径不缓存可重试。
- 群组幂等指纹改用 **SHA-256** 稳定化（归一化二进制表示），Redis L2 条件写（Lua），
  消除跨进程不稳定 `System.HashCode`。
- 权限矩阵/群主转让/最后 Owner 退群/审计事件仍由 RealtimeServices 承担。
- **仍待补**：稳定指纹强化、DB keyset pagination、不可变 Cursor，见 `roadmap-todo.md` 主线三。

### Relationship（Gateway 侧协议层 + Realtime 后端 + 增量同步已完成）

- `RelationshipListChanged=153`（S2C）+ `RelationshipListHandler`：消费
  `FriendRequestListChanged` / `FriendListChanged` / `BlockedListChanged` 事件。
- **事件发布者已闭环**：`NpgsqlRelationshipStore` 6 个 mutation 操作（Send/Accept/Decline
  FriendRequest、RemoveFriend、Block/Unblock）经 `OutboxInsertHelper` 写入 outbox →
  `OutboxPublisherWorker` 走 NATS/JetStream；`RelationshipListHandler` 消费后失效
  Typing/Presence 授权缓存并推送 `RelationshipListChanged`。
- `IDirectConversationAuthorizer` 缓存主动失效：friendship/blocked-user 变更双向失效。
- **Gateway 协议层已完成**：`RelationshipCommandHandler` + `IRelationshipBackend` 端口
  （`RealtimeRelationshipBackend` 生产实现，`StubRelationshipBackend` 保留供单测注入），
  C2S 命令路由已接入 `CommandDispatcher`。
- **跨仓库业务逻辑已完成（2026-08-03）**：RealtimeServices 侧域
  （`IRelationshipStore` / `NpgsqlRelationshipStore` / Migration052 /
  `DefaultRelationshipCommandProcessor` / `DefaultRelationshipListQueryProcessor` /
  NATS consumer）。详见 `roadmap-changelog.md` 2026-08-03 条目。
- **增量同步已完成（2026-08-03）**：
  - `IRelationshipStore.List*` 支持 `afterChangedAtMs` 服务端水位过滤。
  - `IRelationshipSyncCursorStore` + `NpgsqlRelationshipSyncCursorStore` +
    `Migration053` 设备级游标存储（单调推进）。
  - `SyncBootstrapQuery.RelationshipWatermarks` / `RelationshipListLimit` +
    `SyncBootstrapPage.RelationshipCatchUps` 字段。
  - `DefaultSyncBootstrapQueryProcessor.BuildRelationshipCatchUpsAsync`：
    水位优先级 client > 设备游标；查询失败降级为空 catch-up。
  - `EnforceByteBudget` 阶段 2.5/2.6 关系条目纳入字节预算硬约束。
  - Gateway wire 类型 `RelationshipSyncWatermark` / `RelationshipCatchUp` 已注册。
  - 详见 `roadmap-changelog.md` 2026-08-03 条目。

### 附件（Gateway 侧协议层 + Finalize 后端已完成，跨仓库部分待补）

- `InboundPayloadEarlyValidator`：ChatMessage 入站前廉价结构校验（附件数 ≤32、ID 长度 1..64）。
- `AttachmentLifecycleHandler`：消费 `AttachmentLifecycleChanged` 事件，推送 `AttachmentLifecycleUpdate`。
- `AttachmentWireMapper` + `AttachmentRef` wire DTO：6 状态枚举 + DownloadApiHint/DownloadToken。
- **Gateway 协议层已完成**：`AttachmentCommandHandler` + `IAttachmentBackend` 端口就绪，
  Initiate/Finalize C2S 命令路由已接入 `CommandDispatcher`。
  `RealtimeAttachmentBackend`（生产）经 `IRealtimeMessageBus.FinalizeAttachmentUploadAsync`
  转发到 Realtime 侧完成 Ticketed→Uploaded 转换；`StubAttachmentBackend` 保留供单测注入。
  Gateway `AttachmentWireStatus` 6 状态 vs Realtime Abstractions 2 状态
  前 2 值已对齐，扩展状态（UploadConfirmed/Rejected/Expired/ThumbnailUpdated）仅由
  `AttachmentLifecycleHandler` 下游推送使用，不参与 `AttachmentWireMapper` 映射。
- **Finalize 后端已完成（2026-08-03）**：详见 `roadmap-changelog.md`。
- **跨仓库待补**：所有权校验、扫描/审核、过期清理（sweep worker）、下载授权，
  见 `roadmap-todo.md` 主线四。
### 可观测性

- Gateway 与 RealtimeServices 接入 OpenTelemetry Metrics/Tracing，支持 OTLP 导出。
- W3C Trace Context 贯穿 TCP 命令 → NATS/JetStream → 消费 → PostgreSQL/Outbox → 回推事件。
- RealtimeServices Prometheus `/metrics`，JSON 快照迁移到 `/diagnostics/runtime`。
- Linux 测试机已部署 Prometheus + Grafana + 初始告警规则 + 仪表盘（`deploy/observability/`）。
- 告警阈值待正式基准校准；Alertmanager 通知通道待配置。
- 详见 `docs/observability-alerts.md`。

## 性能基线状态

- 本机 30 分钟基线已通过：1000 TCP 长连接，94,715/94,715 持久链路成功，
  52.61 pipeline/s，p95/p99 437.5/441.5 ms。
- Outbox 优化后 A/B：208,523 条完整链路，115.84 pipeline/s，p95/p99 123/165 ms。
- History SQL 分支放大消除：扫描行 1,535→11，缓冲页 1,162→17，执行时间 6.055ms→0.071ms。
- 容量曲线：120/s 目标 5 分钟持续 115.35/s，34,626/34,626 成功，p95/p99 174/214 ms。
- DirectSocket vs Pipelines Linux A/B：1000 连接，吞吐持平（-0.019%），
  p99 -4.99%，CPU -17.96%，工作集 -3.34%。
- 故障注入短测：Garnet 568/568、PostgreSQL 575/575、NATS pause/unpause 513/513。
- **未完成**：8-24 小时 soak、连接风暴、慢速发送攻击、全局入站预算耗尽等长测，
  见 `roadmap-todo.md`。

## CI 与发布门禁

- `ChatApp.Performance.Gate` 已实现：对编排器报告做失败闭环检查，
  支持 `--require-conversation-stages` 阶段 p95 闭环。
- `.github/workflows/build.yml` 已配置 locked restore、Release build 与完整架构/行为测试门禁。
- Linux 自托管 runner、真实依赖探针、定时浸泡和性能门禁仍属于运行环境治理，不影响独立构建。
