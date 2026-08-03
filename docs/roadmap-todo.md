# 待办路线图

本文件列出**尚未完成**的工作。当前状态见 `roadmap-current-state.md`，
历史变更见 `roadmap-changelog.md`。完成的项目从本文件移除并记入 changelog。

## 主线一：Push 正式闭环（Gateway 侧已完成，跨仓库待补）

Gateway 侧 Push 闭环已全部完成（配置 Fail-fast、Disposition、Token Retry、幂等、DLQ、
Provider 并发限制、无效 Token 注销、PushWorker 拆出、AES-GCM Token 加密）。
以下为跨仓库待补项：

2. **真实 FCM/APNs/WebPush Provider**
   - ~~跨仓库实现 Provider client 与 payload builder。~~
     **已完成（2026-08-03）**：`FcmPushProvider` / `ApnsPushProvider` / `WebPushPushProvider`
     + payload builder + `PushProviderOptions` + DI 注册（`AddRealPushProviders`）已实现于
     `PushWorker/Providers/`，构建通过、381/381 测试无回归。详见 `roadmap-changelog.md` 2026-08-03 条目。
   - ~~`IPushTokenStore` / `PushTokenRecord` 当前位于 `ChatApp.TcpGateway.Core.Push`，
     RealtimeServices 无法引用——需提取到 `ChatApp.Realtime.Abstractions` 或发布独立 NuGet 包
     （见 `AGENTS.md` "Long-term: publish shared Realtime contracts as a versioned package"）。~~
     **已完成（2026-08-03）**：`PushPlatform` / `PushTokenLimits` / `PushTokenRecord` / `IPushTokenStore`
     已提取到 `ChatApp.Realtime.Abstractions.Push`，Core.csproj 引用 Realtime.Abstractions（BCL-only 契约层）。
   - Publisher 侧 Push 触发：`GetOnlineGatewaysWithStatusAsync` 返回 `UserOffline` 时入队 Push 任务。

## 主线二：Resume 真正事务化（已完成）

全部 8 项已实现：Token Claim/Commit/Abort、AdmissionState 三态、TakeOver 顺序与回滚、
DependencyUnavailable 可重试、同设备 fencing、旧 Socket 关闭验证、SessionRevokedPayload 结构化。
见 `roadmap-changelog.md` 2026-08-01 条目。

## 主线三：Group 后端分页和稳定指纹（已完成）

经核验，跨仓库待补项均已实现，详见 `roadmap-changelog.md` 2026-08-03 条目：

- **Realtime DB Ledger 作为唯一权威**：`NpgsqlRealtimeGroupStore`（约 2000 行）承担全部
  权限矩阵（Owner/Admin/Member）、群主转让（仅 Owner 可变更角色、不能转让给自己、
  Owner 不能自降级）、最后 Owner 退群拒绝（"Owner 退群前须先转让所有权"）、审计 Outbox
  （事务内 `RecordInTransactionAsync`）、幂等账本（`group_mutation_requests`）、
  软删除（`left_at_ms`/`dissolved_at_ms`）、membership periods、audience_version 递增。
- **DB keyset pagination**：设计决策为 Realtime 侧返回全量成员（按 role/joined_at_ms/user_id 升序），
  Gateway 本地执行 keyset 分页（`PaginateMembers`），避免改动 RealtimeServices 协议；
  幂等缓存保存全量结果，不同分页参数命中同一缓存后各自切片。
- **不可变 Cursor 顺序**：cursor 编码 `(role, joined_at_ms, user_id)` 元组（base64），
  Realtime 排序保证 keyset 稳定，不遗漏/不重复；cursor 非法时退化为首页。
- **权限和审计由 RealtimeServices 承担**：`NpgsqlGroupOperationAuditStore` 双路径
  （事务外 best-effort + 事务内 Outbox），`group_operation_audit` 表（Migration028）已就绪。

## 主线四：附件和 Relationship（Gateway 侧协议层已完成，跨仓库待补）

Gateway 侧协议命令、DTO、Handler、端口 + Stub 已全部实现。
以下为跨仓库待补项：

### 附件

1. **Attachment Finalize 后端**
   - ~~Gateway `AttachmentCommandHandler` + `IAttachmentBackend` 端口已就绪（当前 Stub）。~~
     ~~Realtime 侧需实现 `FinalizeUploadAsync`（Ticketed→Uploaded 转换）并接入
     `IRealtimeMessageBus`（新增 `FinalizeAttachmentUploadAsync` 方法）。~~
     **已完成（2026-08-03）**：`RealtimeAttachmentBackend` 替换 stub，端到端打通
     `AttachmentFinalizeRequest` → `IRealtimeMessageBus.FinalizeAttachmentUploadAsync` →
     Realtime 侧 `FinalizeUploadAsync`（Ticketed→Uploaded）。详见 `roadmap-changelog.md`。

2. **所有权校验**
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
   - Gateway `AttachmentWireStatus` 6 状态 vs Realtime Abstractions 2 状态（Scanning/Available）
     前 2 值已对齐，扩展状态（UploadConfirmed/Rejected/Expired/ThumbnailUpdated）仅由
     `AttachmentLifecycleHandler` 下游推送使用，不参与 `AttachmentWireMapper` 映射。

### Relationship

1. **Relationship 后端**
   - ~~Gateway `RelationshipCommandHandler` + `IRelationshipBackend` 端口已就绪（当前 Stub）。~~
     ~~`IRealtimeMessageBus` 需新增 `MutateRelationshipAsync` / `QueryRelationshipListAsync` 方法。~~
     **已完成（2026-08-03）**：`RealtimeRelationshipBackend` 替换 stub，端到端打通
     `RelationshipCommandRequest` / `RelationshipListRequest` →
     `IRealtimeMessageBus.MutateRelationshipAsync` / `QueryRelationshipListAsync` →
     Realtime 侧 `NatsRelationshipCommandConsumer` / `NatsRelationshipListQueryConsumer` →
     `RelationshipCommandWorker` / `RelationshipListQueryWorker` →
     `DefaultRelationshipCommandProcessor` / `DefaultRelationshipListQueryProcessor` →
     `NpgsqlRelationshipStore`（好友请求/友谊/黑名单 DB 操作）。
     Gateway 与 Realtime 两侧 byte 枚举（`RelationshipOperation` / `RelationshipListType`）
     数值一一对应，通过强制转换映射。440/440 测试无回归。
   - ~~RealtimeServices 侧域业务逻辑仍待补：`IRelationshipStore` / Postgres migration /
     `IRelationshipCommandProcessor` / `IRelationshipListQueryProcessor` 真实实现。~~
     **已完成（2026-08-03）**：
     - `IRelationshipStore` 接口 + `NoopRelationshipStore` 默认实现。
     - `NpgsqlRelationshipStore`：好友请求状态机（Pending/Accepted/Declined）、
       友谊规范化存储（`user_id_low/user_id_high`）、黑名单复用 `T_BlockRecords`、
       幂等账本（`relationship_mutation_requests`）、游标分页。
     - `Migration052_Relationships`：`friend_requests` + `friendships` +
       `relationship_mutation_requests` 三表 + 索引。
     - `DefaultRelationshipCommandProcessor` / `DefaultRelationshipListQueryProcessor`。
     - 三处 DI 注册（Core/Postgres/Host）+ NATS consumer 注册。
     - 修复预存在的 `CapturingAttachmentStore` 缺少 `FinalizeUploadAsync` 测试问题。
     - RealtimeServices.slnx 构建 0 错误；TcpGateway 440/440 测试通过。
     三个事件类型（`FriendRequestListChanged=1` / `FriendListChanged=2` / `BlockedListChanged=3`）
     已在枚举预留但无发布者（待增量同步需求驱动）。

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