# 待办路线图

本文件列出**尚未完成**的工作。当前状态见 `roadmap-current-state.md`，
历史变更见 `roadmap-changelog.md`。完成的项目从本文件移除并记入 changelog。

## 主线一：Push 正式闭环

当前 Push 仅为基础桥接完成，未达到生产闭环。按以下顺序依次完成：

1. **Push Feature 配置与生产 Fail-fast**
   - `Push.Enabled` 默认 false；`Push.ProviderMode` 必须为 `Disabled` / `TestNoop` / `Production`。
   - `Production` 模式启动校验：任何平台使用 `NoopPushProvider` 即启动失败。
   - `NoopPushProvider` 仅用于 Development/Test，须返回 retryable 失败，不得返回成功送达。
   - `PushOptions` 须 gated，防止静默吞没 Push。
   - 启动校验覆盖 Push 配置完整性。

2. **真实 FCM/APNs/WebPush Provider**
   - 跨仓库实现 Provider client 与 payload builder。
   - `IPushTokenStore` / `PushTokenRecord` 当前位于 `ChatApp.TcpGateway.Core.Push`，
     RealtimeServices 无法引用——需提取到 `ChatApp.Realtime.Abstractions` 或发布独立 NuGet 包
     （见 `AGENTS.md` "Long-term: publish shared Realtime contracts as a versioned package"）。
   - Publisher 侧 Push 触发：`GetOnlineGatewaysWithStatusAsync` 返回 `UserOffline` 时入队 Push 任务。

3. **投递 Disposition**
   - `PushDispatcher` 返回 `PushDispatchDisposition`：
     `NoTargets` / `FullySucceeded` / `PermanentlyCompleted` / `Retryable` / `PartiallyRetryable`。
   - `PushConsumer` ACK 规则：`FullySucceeded` / `NoTargets` / `PermanentlyCompleted` → ACK；
     `Retryable` → NAK；`PartiallyRetryable` → 仅重试失败 token。

4. **Token 粒度 Retry**
   - 失败 token 独立重试，不整体重投。

5. **DeliveryId 幂等**
   - Push 投递幂等，防止重复推送。

6. **Retry / DLQ**
   - 重试退避策略 + 死信队列。

7. **Provider 并发和速率限制**
   - FCM/APNs/WebPush 各自的并发与速率限制。

8. **无效 Token 可靠注销**
   - Provider 返回无效 Token 时回调 `IPushTokenStore.UnregisterByTokenAsync`。

9. **将 Provider Worker 从 Gateway 拆出**
   - `PushConsumer` 与 Provider 调用从 TCP Gateway 迁移到独立 `PushWorker` 服务，
     隔离网络资源。

10. **加密或最小权限保护 Push Token**
    - 当前 Token 明文存储于 Redis，需评估加密方案或最小权限保护。

## 主线二：Resume 真正事务化

1. **Token Claim/Commit/Abort**
   - Resume Prepare 阶段使用原子 token claiming（`TryClaim` / `CommitClaim` / `ReleaseClaim` via Lua），
     替代 `GETDEL`，防止 token 在 Commit 前被消费。

2. **Admission 状态显式化**
   - Session 跟踪显式 `AdmissionState`（`Unauthenticated` / `Promoted` / `Released`），
     替代从 `UserId` 推断，防止连接计数泄漏。
   - Resume Commit 使用 `AdmissionPromoted` 显式状态标记。

3. **Redis TakeOver 先于本机旧连接关闭**
   - Redis 接管流程必须先 `TakeOver` 再关闭本机旧连接。

4. **TakeOver 成功但本地 Commit 失败时释放新 Lease**
   - TakeOver 成功后本地 Commit 失败须回滚：`RollbackResumeLocalStateAsync`
     反转 `UserSessionRegistry.Add` 与 Presence 发布。

5. **客户端可真正重试 DependencyUnavailable**
   - `ResumeTokenValidationResult` 含 `DependencyUnavailable` 状态，区分 Redis 失败与无效 token。
   - `RedisResumeTokenStore.TryValidateAsync` Redis 失败时抛 `RedisException`（非返回 null），
     确保 fail-closed。
   - `RedisDeviceSessionLeaseStore.TakeOverAsync` 熔断器开路或 Redis 异常时抛 `RedisException`。

6. **Redis Failover 恢复阶段完整验证**
   - 同设备 fencing：`TakeOverSameDevice` 匹配 UserId + DeviceIdHash + 不同 ConnectionLeaseId
     （忽略 SessionId）。`DeviceIdHash` 按 userId 确定性派生（黄金比例乘子 + 1，确保非零）。
   - `ResumeVerification` 注入确定性 `DeviceIdHash`，跨连接/网关一致。
   - `TakeOverUnavailable` 指标记录 + `AuthenticationRejected` 关闭连接。

7. **旧 Socket 读写必须失败**
   - `TakeoverCompetitionScenario` 验证旧 Socket 的 `ReadClosed` 与 `WriteClosed`
     （800ms 传播延迟后检查）。
   - `SessionRevoked` 事件使用结构化 `SessionRevokedPayload { transportId }`，非裸 ConnectionLeaseId。
   - `SessionLifecycleCoordinator` 即使 NATS 发布失败也关闭本机 victim transport。

8. **最终同设备唯一 Transport**
   - Redis lease 探针验证 transportId 变更与 sessionId 一致性。

## 主线三：Group 后端分页和稳定指纹

1. **稳定 128/256 位 Fingerprint**
   - 群组幂等 payload hash 已改用 SHA-256（归一化二进制表示），需持续验证稳定性。
   - 归一化字段：FingerprintVersion, Operation, ConversationId, Title, TargetUserId, NewRole,
     sorted MemberUserIds。**不可**使用 `System.HashCode`（进程随机种子，跨进程不稳定）。

2. **Redis Put-if-absent-or-compare**
   - Redis L2 idempotency `TryAdd` 使用条件 Lua 写（仅当 key 缺失或存储的 payloadHash 匹配才写），
     **非**无条件 HSET，防止并发 Miss last-writer-wins 覆写。

3. **Realtime DB Ledger 作为唯一权威**
   - Gateway 不重复实现权限矩阵（Owner/Admin/Member）、群主转让、最后 Owner 退群、审计事件。

4. **DB keyset pagination**
   - Member 列表分页需跨仓库扩展 `GroupConversationCommand`（当前无分页字段）。

5. **不可变 Cursor 顺序**
   - 分页 cursor 顺序稳定，不遗漏/不重复。

6. **权限和审计继续由 RealtimeServices 承担**
   - 禁止移除自己/群主等业务规则在 Realtime 侧判定。

## 主线四：附件和 Relationship

**前置依赖**：Push 和 Resume 收敛后再补。

### 附件

1. **Attachment Initiate/Finalize**
   - 新增 `PacketCommand.AttachmentFinalizeRequest/Response` 等 C2S 协议命令
     （当前仅有 S2C `AttachmentLifecycleChanged=154`）。
   - Realtime 侧需 `IRealtimeAttachmentStore.FinalizeUploadAsync`（Ticketed→Uploaded 转换）。

2. **所有权**
   - `MessagingCommandHandler` 当前不校验 `AttachmentIds` 归属，由 Realtime `BindToMessageAsync` 拒绝。
   - 如需 Gateway 前置校验，新增 `IRealtimeMessageBus.VerifyAttachmentOwnershipAsync`。

3. **扫描/审核**
   - `Scanning`/`Rejected` 状态下游推送已就绪，但扫描触发与发布者在两仓库之外（独立 worker / Server）。

4. **过期清理**
   - `ix_attachments_unbound_age` 索引已存在（Migration012），但无 sweep worker。
   - 需 RealtimeServices 或独立服务定期扫描并发布 `Expired` 事件。

5. **下载授权**
   - wire DTO 有 `DownloadApiHint`/`DownloadToken` 字段，但 Token 签发与校验在 Server HTTP API，
     Gateway 不参与。

6. **Migration012 / 枚举对齐**
   - Migration012 CHECK 约束 `status IN (0,1,2,3)` 过期，需新增 migration 放宽
     （含 `Uploaded=4` / `Scanning=5` / `Rejected=6`）。
   - Gateway `AttachmentWireStatus` 6 状态 vs Realtime Abstractions 2 状态（Scanning/Available），
     需对齐。

### Relationship

1. **Relationship 请求、接受、拒绝、拉黑**
   - 新增 PacketCommand 值（159+ 范围）+ DTO + CommandCatalog 描述符 + `RelationshipCommandHandler`。
   - `IRealtimeMessageBus` 新增对应查询/命令方法。
   - RealtimeServices 侧域：无 `IRelationshipStore` / `IRelationshipQueryProcessor` /
     `IRelationshipCommandProcessor` / Postgres migration / NATS consumer。
     三个事件类型（`FriendRequestListChanged=1` / `FriendListChanged=2` / `BlockedListChanged=3`）
     已在枚举预留但无发布者。

2. **Relationship Watermark**
   - `ConversationSyncWatermark` 仅限会话维度，需扩展为 Relationship 级别版本/水位用于增量同步。

3. **增量同步**
   - 好友列表分页、好友请求列表、接受/拒绝流程的客户端增量同步。

## 其他待办（非主线，按优先级评估）

### 性能长测（Linux 测试机）

执行规范见 `AGENTS.md` "Linux test environment" 与 `scratch/Run-RuntimeV2-Soak-Linux.ps1`。

- 10,000 空闲连接
- 512 B Chat
- 64 KiB Chat
- 慢速发送攻击
- 全局入站预算耗尽
- 连接风暴（如 1k/s 持续 10s）
- 8～24 小时浸泡（覆盖内存泄漏、ThreadPool 饱和、Redis 连接池、NATS 重连、Actor IdleSweep）
- allocation/sec、GC、每连接内存稳定窗口对比
- Runtime V2 8h 首轮在 PersistentSendLoop 阶段退出（负载进程 code 137），未形成有效结论；
  重跑前先修复长测进程存活/资源限制与失败取证，再决定默认发送模式。

### 故障与恢复测试

- 滚动重启网关和 RealtimeServices，确认 durable consumer、Outbox、客户端去重。
- 短暂断开 JetStream、PostgreSQL、Garnet，验证超时、退避、重投、积压收敛、恢复后无静默丢失。
- 注入慢客户端和超大历史页，确认有界队列、字节预算、连接隔离。
- 校验同毫秒消息游标翻页不遗漏/不重复，重复请求结果稳定。
- 跨 Gateway 重连风暴 Linux soak（依赖真实 Redis 故障注入）。

### 性能基线复跑

- 用 `Run-ConversationCombo.ps1` 在 Linux 正式机复跑：会话历史翻页 + 列表/SyncBootstrap
  与 TCP chat 扇出/慢消费者并行；校准会话阶段 p95 阈值。须避开 Runtime V2 soak 时间窗。
- 用生产近似数据规模与资源限制复跑 8-24 小时浸泡，校准告警阈值。
- 版本化 JSON 短期门禁接入定时 CI（依赖 P1 CI Linux 自托管 runner）。

### 可观测性收尾

- OTLP Collector 统一转发 Prometheus/Trace，校验跨进程 Trace 查询体验。
- Alertmanager 选择并配置实际通知通道（当前告警规则仅暴露触发状态）。
- 日志只保留结构化故障信息，避免聊天热路径恢复高频 Info 日志。

### CI 与发布门禁

- 注册 Linux 自托管 CI runner。
- 接入 Release 构建、全部测试、数据库契约检查、真实 NATS/PostgreSQL 探针、定时浸泡、性能门禁。
- 保存基准/门禁结果并比较历史版本，性能退化须明确说明。
- .NET 11 稳定版发布后与 Server/RealtimeServices **同步**升级 SDK/依赖并重跑基线。

### 代码质量（非阻塞）

- 继续拆分 oversized Gateway 类型（`TcpGatewayService` 869 行、`GatewayMetrics` 871 行，
  均超 600 行警戒线，但非阻塞）。
- 补充已迁移模块独立单测：`SessionRuntime` / `SessionControlHandler` / `TcpClientSession`
  仍缺独立单测（与传输/codec 耦合，需抽象测试边界）。

## 二进制协议时机

开发阶段继续使用 JSON。只有全链路基准证明 JSON 是主要 CPU 或分配瓶颈后，再实现二进制编码；
升级必须通过协议版本或能力协商保留旧 JSON 客户端兼容性。
