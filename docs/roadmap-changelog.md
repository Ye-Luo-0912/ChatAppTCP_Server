# 历史变更

本文件记录已完成的历史变更，按时间倒序排列。当前状态见 `roadmap-current-state.md`，
未完成工作见 `roadmap-todo.md`。本文件不再保留过期测试数字（当时通过数仅作参考）。

## 2026-08-30

### BIN-INTEGRATION-3 连接级二进制 payload 双端接入

Gateway 侧完成 `chatapp-bin-v1` 双 codec 接入与验收（JSON 默认路径不变）：

- 协商：`GatewayFeature.BinaryPayload` 入 `GatewayFeatureSet.Implemented`（修复缺位导致协商永不发生的 bug）；
  `TcpGatewayOptions.EnableBinaryPayloadFormat`（默认 false）门控；非 Resume 路径协商出 BinaryPayload 时
  `ServerHello.PayloadFormat` 回应 `chatapp-bin-v1`，`TcpClientSession.NegotiatedPayloadFormat` 握手后不可变；
  Resume 路径强制 JSON 且协商位同步剥离该能力位。
- 入站：`SessionPayload.Deserialize` 按会话格式分流；二进制走共享 `TcpBinaryWireCodec` 寄存器 +
  `BinaryPayloadMapper.ToLocal`（68 个本地↔共享映射 + 11 个共享类型恒等）；畸形/超限 fail-closed 关连接。
- 出站：`OutboundFrameFactory.Create(command, codec, session, value)` 按会话格式分流 + `CreateBinary`
  （`TcpBinaryWireEncoder` 编码，DestinationTooSmall 单次扩容重试）；fanout 用 `FormatGroupedFrame`
  按格式各至多编码一次共享帧。
- 验证：新增 9 项真 TCP 集成测试（协商成功/JSON fallback×2/Resume-JSON/二进制往返/混合格式 fanout/
  畸形 fail-closed/未知命令/二进制 GoAway），全量 630 项测试全绿；80/320/640 msg/s 短测
  （`scratch/binary-shorttest-report-20260830.md`）payload −47%、640/s CPU −28.5%、
  零漏投/零重复、p99 ≤1.9 ms。
- 跨进程真机验证：部署 relgate 全栈 + e2e 驱动（.tmp-bin-e2e / .tmp-call-e2e 经 SSH 隧道）12/12 与 27/27 通过；过程中发现并修复 6 处 fanout/响应站点漏做格式分组的真实 bug（旧 3 参 JSON-only Create 向二进制会话投 JSON 帧），新增混合格式回归测试防复发；网关测试 632 全绿。
- 依赖：共享契约包 `ChatApp.Protocol.Tcp.Binary.Schemas` 0.5.4（含新增编码寄存器
  `TcpBinaryWireEncoder`；本地 feed Protocol.Tcp 0.5.3 残缺旧构建弃用，统一 0.5.4）。

## 2026-08-14

### REL-E2E-4 Gateway 关系读取端收口

Gateway 关系读取 handler 从 Core/Realtime 内部 DTO 迁到 Shared `TcpRelationshipList*`
wire 契约，作为 Client↔Gateway 之间关系只读列表的唯一 schema。

- `RelationshipCommandHandler.HandleListAsync` 使用 Shared
  `TcpRelationshipListRequest/Response`；内部经 `HistoryWireMapper.MapRelationshipItems`
  显式把 Realtime `RelationshipListItem` 映射为 Shared `TcpRelationshipListItem`，
  禁止把 Realtime 内部 DTO 直接序列化给客户端。
- 稳定错误码落地：`ProjectionUnavailable` / `ProjectionChanged` / `GapDetected` /
  `InvalidCursor` / `PageSizeOutOfRange` / `BadRequest`；`ResetRequired` 仅在
  projection-changed / gap 时置位，失败不推动 Client 水位。
- 命令级门控：`CommandCatalog.RelationshipListRequest` 增加
  `RequiredFeature = GatewayFeature.RelationshipRead`（Shared 分配位 `1u<<13`），
  加入 `GatewayFeatureSet.Implemented`；严格客户端需协商能力位，legacy 客户端向后兼容。
- DI 与序列化：`GatewayJsonSerializerContext` 注册 Shared `TcpRelationshipList*` 类型，
  `InfrastructureServiceCollectionExtensions` 改用 Shared list codec；5 个既有测试文件
  更新 handler 构造参数。
- 新增端到端集成测试 `RelationshipListReadIntegrationTests`（5 项）：item 映射、分页
  （HasMore/NextCursor）、gap 触发 reset、unavailable fail-closed、page size 越界。
  全部 574 项测试通过（1 项 Redis 用例按设计跳过）。
- Server/Client 侧剩余工作（HTTP 权威逐项对照、Client 水位恢复）见 `roadmap-todo.md`。

## 2026-08-11

### TCP-MEM-1 正式测量完成（Linux 真机 192.168.5.49）

完整批次 `3 画像 × 3 轮 × 10 分钟` 全部 `VALID`（`memory-profile-20260811-011250Z`，
`TCP-MEM-1: PASSED (all profiles/repeats valid)`）。该批次只产证据、不改功能，
详细归因与数值见 `roadmap-current-state.md` 性能基线段。

- 证据：每轮 2 Gateway 的 `Max PSS`（smaps_rollup）、`Max VmRSS/VmHWM`（/proc）、
  cgroup 峰值、`/proc/{pid}/fd` 峰值，每轮 2 个 gcdump（共 18 个），
  `ss -tinm` socket 归属（每 Gateway ≈5002 socket = 5000 连接 + 监听）。
- 画像内存梯度符合预期：active（213–230 MiB PSS）> heartbeat（197–209 MiB）>
  silent（175–183 MiB）。
- 管道修复：
  - 后台任务 `[ordered]@{}` 反序列化为 `OrderedDictionary`，类型过滤需覆盖
    `IDictionary`、`Get-OptionalProperty` 需按字典键读取，使 gcdump/socket 证据正确聚合；
  - `ss` 归属正则改为 `pid=<pid>,`（逗号而非空白前缀），使 socket 计数正确归因；
  - `$pid` 只读变量冲突改用 `$gatewayPid`；
  - active 画像死信门放宽到「本画像消息理论上限」（slow-reader 场景非内存归因语义，
    实测约 6% 消息被限流死信，原 `slowReaders*2=200` 过紧导致误报 INVALID）。

## 2026-08-10

### TCP-MEM-1 测量工具交付

- 编排器新增 Linux 内存归因：PSS/smaps_rollup、`/proc/{pid}/fd` 峰值、cgroup sock/oom。
- `scripts/Run-MemoryProfile.ps1` 编排 10k 静默 / heartbeat-only / 1% active+slow-reader
  三类画像 × 每轮 10–15 分钟，测量中段并行采集 gcdump 与 `ss -tinm`/sockstat 证据。
- 相关脚本全部通过 PowerShell AST 解析，orchestrator Release 构建 `0 warning / 0 error`。
- 修复归因 Markdown 汇总里 `-f` 逗号/除法优先级导致的 "Index (zero based)..."
  格式化报错（改为先算标量再格式化），smoke 报告生成链路可用。

## 2026-08-04

### P1 跨仓库与功能语义问题（5 项全部完成）

完成 P1-1 ~ P1-5 五个跨仓库/功能语义问题的闭环，两个仓库均构建通过、测试全绿：

- **P1-1 版本化 Contract Package**：将 `ChatApp.Realtime.Abstractions` 提取为版本化
  NuGet 包（本地 feed `ChatApp.Realtime.Contracts` / `ChatApp.Realtime.Integration`），
  消除 Core 对 sibling 仓库的源码级相对路径引用；`RealtimeContractMetadata` 记录契约元数据；
  启用 `RestorePackagesWithLockFile` 保证可复现构建；`ContractCompatibilityTests`
  验证协议版本对齐与序列化往返。
- **P1-2 AudienceVersion 消费闭环**：Gateway 新增 `ConversationAudienceCache`
  （striped lock + bounded TTL + LRU 淘汰 + 版本校验）；Realtime 新增
  `GroupConversationOperation.QueryAudience=8` 专用受众查询契约，Gateway 在版本落后时
  刷新成员并正确处理 `AudienceKind=Conversation` 且 `TargetUserIds=null` 的事件路由。
- **P1-3 附件闭环**：Realtime 侧 `AttachmentScanProcessor`（Uploaded→Scanning→
  Available/Rejected，含对象存储 HEAD 校验）+ `AttachmentSweeper`（未绑定过期清理）+
  `state_version` 条件更新防旧结果覆盖；Gateway 侧新增 `AttachmentDownloadAuthorizeRequest(167)/
  Response(168)` 下载授权命令 + wire DTO + `IAttachmentBackend.AuthorizeDownloadAsync` NATS 转发。
- **P1-4 群已读回执端到端**：Realtime 侧 `GroupConversationOperation.QueryReadReceipts=9` +
  处理器权限校验（仅消息发送者）+ `IRealtimeMessageBus.QueryReadReceiptsAsync`；Gateway 侧
  `MessageReadReceiptQueryRequest(169)/Response(170)` 命令 + wire DTO + 端到端 round-trip 测试。
- **P1-5 Relationship 并发状态机测试**：新增 6 个并发测试（双方同时发请求、接受/拒绝并发、
  删好友与重发并发、拉黑与接受并发、重复 RequestId 跨 Gateway 幂等、已注销用户拒绝变更）。
  测试发现并修复 `NpgsqlRelationshipStore` 中 `NpgsqlDataReader` 未释放即 `RollbackAsync`
  的真实并发 bug（`NpgsqlOperationInProgressException`）。

测试基线：TcpGateway **447/447**，RealtimeServices **278/278**。

## 2026-08-03

### 主线四-关系：Relationship 事件发布者核验（已完成，无代码改动）

核验三个关系事件类型（`FriendRequestListChanged` / `FriendListChanged` / `BlockedListChanged`）
的发布链路，确认发布者已随 Relationship 后端闭环一并实现，roadmap 中"无发布者"描述过时：

- **RealtimeServices 发布者**：`NpgsqlRelationshipStore` 6 个 mutation 操作
  （SendFriendRequest / AcceptFriendRequest / DeclineFriendRequest / RemoveFriend /
  BlockUser / UnblockUser）每次成功变更在同一事务内经 `OutboxInsertHelper.InsertManyAsync`
  写入 outbox 表，由 `OutboxPublisherWorker` 异步发布到 NATS/JetStream。
  `RealtimeEvent.PayloadJson` 使用 `RealtimeDomainNotificationPayload`
  （Resource / Action / ResourceId / Message）。
- **Gateway 消费端**：`RealtimeEventDispatcher` 已注册 3 个事件类型 →
  `RelationshipListHandler` 通过 `GatewayJsonSerializerContext` 解析 payload，
  friendship/blocked-user 变更双向失效 `IDirectConversationAuthorizer` 缓存 +
  Specialized TypingActor 授权，并推送 `RelationshipListChanged`。
- 文档更新：`roadmap-todo.md` / `roadmap-current-state.md` 修正"无发布者"描述。
- 无代码改动，构建/测试基线不变（TcpGateway 440/440，RealtimeServices 259/259）。

### 主线四-关系：Relationship 增量同步水位与 SyncBootstrap 集成（已完成）

在 SyncBootstrap 内引入 Relationship 维度的水位/增量同步能力，与既有会话维度水位解耦并行：

- **RealtimeServices 侧**（sibling 仓库）：
  - `IRelationshipStore.List*` 三方法新增 `afterChangedAtMs` 服务端水位过滤参数：
    - `ListFriendsAsync` / `ListFriendRequestsAsync` 在 SQL 中按 `created_at_ms > @after` 过滤；
    - `ListBlockedUsersAsync` 底层 `T_BlockRecords` 表无变更时间戳，参数保留接口对称但服务端不过滤
      （由客户端按本地缓存 diff，`NewAfterChangedAtMs` 始终为 0）。
  - `NpgsqlRelationshipStore` 三 list 实现 `afterChangedAtMs` 服务端过滤。
  - 新增 `IRelationshipSyncCursorStore` 设备级游标存储接口
    （`Load/UpsertManyAsync 单调推进/Delete/DeleteByUser/DeleteInactive`），
    与 `IRealtimeDeviceSyncCursorStore` 平行但以 list_type 为维度。
  - `NpgsqlRelationshipSyncCursorStore`：`ON CONFLICT DO UPDATE WHERE 旧水位 < 新水位` 单调推进。
  - `NoopRelationshipSyncCursorStore` 占位实现。
  - `Migration053_RelationshipSyncCursors`：`relationship_sync_cursors` 表
    （PK `user_id+device_id_hash+list_type`）。
  - `SyncBootstrapQuery` 新增 `RelationshipWatermarks` / `RelationshipListLimit`。
  - `SyncBootstrapPage` 新增 `RelationshipCatchUps` 字段。
  - 新增 `RelationshipSyncWatermark` / `RelationshipCatchUp` 抽象类型。
  - `DefaultSyncBootstrapQueryProcessor` 集成：
    - `BuildRelationshipCatchUpsAsync`：水位来源优先级 client watermarks > 设备持久化游标。
    - `EnforceByteBudget` 阶段 2.5/2.6：关系条目纳入字节预算硬约束
      （优先级低于会话 catch-up，高于会话列表项）。
    - `BuildRelationshipCursorsToPersist`：仅推进非 reset 且实际返回条目的水位
      （BlockedUsers 的 `NewAfterChangedAtMs=0` 天然被过滤）。
    - 关系列表查询失败降级为空 catch-up，不影响会话同步。
  - `RealtimeJsonSerializerContext` 注册 `RelationshipSyncCursor` / `List<RelationshipSyncCursor>`。
  - 三处 DI 注册同步（Core/Postgres/Host）。
  - `DefaultRelationshipListQueryProcessor` 调用签名同步。
  - 修复 `RealtimeDatabaseSchema` 预存在的 `schema_migration_checkpoints` 未终止字符串 bug。
- **Gateway 侧**（本仓库）：
  - 新增 wire 类型 `Core/Messaging/Sync/RelationshipSyncWatermark`
    （`ListType + AfterChangedAtMs`）与 `Core/Messaging/Sync/RelationshipCatchUp`
    （`Items/HasMore/NextCursor/NewAfterChangedAtMs/ResetRequired/ResetReason`）。
  - `SyncBootstrapRequest` 新增 `RelationshipWatermarks` / `RelationshipListLimit`。
  - `SyncBootstrapResponse` 新增 `RelationshipCatchUps`（空时序列化为 null 向后兼容）。
  - `GatewayJsonSerializerContext` 注册 4 个新类型
    （`RelationshipSyncWatermark` / `List<>` / `RelationshipCatchUp` / `List<>`）。
  - `HistoryQueryCommandHandler.SyncBootstrap`：
    - 校验 `RelationshipWatermarks` 列表大小、ListType 范围（1..3）、水位非负；
    - 校验 `RelationshipListLimit` 范围；
    - 映射 wire `RelationshipSyncWatermark` → Realtime 抽象类型；
    - 映射 Realtime `RelationshipCatchUp` → wire 类型（含 `RelationshipItem` 字段映射）。
- 构建：TcpGateway.sln 0 警告 0 错误；RealtimeServices 0 警告 0 错误。
- 测试：RealtimeServices 259/259 通过；TcpGateway 439/440 通过
  （1 个无关的 `LazySegmentedOutboundQueue.ConcurrentProducer_Consumer_Pipeline`
  30s 并发计时测试在批量负载下抖动，单独运行通过）。

### 主线四-关系：Relationship 域 RealtimeServices 业务逻辑闭环（已完成）

RealtimeServices 侧 Relationship 域业务逻辑全部实现，从 Gateway 到 DB 端到端打通：

- **Abstractions 层**（`ChatApp.Realtime.Abstractions`）：
  - `IRelationshipStore`（`Stores/IRelationshipStore.cs`）：9 方法接口（6 变更 + 3 列表查询）+
    `RelationshipMutatePersistResult` 结果结构体（Ok/Fail 工厂）。
- **Infrastructure.Core 层**：
  - `NoopRelationshipStore`（`Stores/`）：默认占位实现。
  - `DefaultRelationshipCommandProcessor`（`Relationships/`）：校验 tombstone → 按 Operation
    分发到 store → 映射结果。遵循 `DefaultGroupConversationProcessor` 模式。
  - `DefaultRelationshipListQueryProcessor`（`Relationships/`）：按 ListType 分发到 store，
    计算 `hasMore` + `nextCursor`（base64 编码 offset）。
- **Infrastructure.Postgres 层**：
  - `Migration052_Relationships`：`friend_requests`（PK request_id，status 0=Pending/1=Accepted/
    2=Declined）+ `friendships`（canonical `user_id_low/user_id_high`，UNIQUE 约束）+
    `relationship_mutation_requests`（幂等账本，PK actor_user_id+request_id）三表 + 索引。
  - `NpgsqlRelationshipStore`（`Stores/`）：完整 DB 实现，含：
    - 幂等去重：`TryReadIdempotencyAsync` + `RecordIdempotencyAsync`（事务内）。
    - 好友请求：双向 pending 检查、FOR UPDATE 行锁、状态机转换。
    - 友谊：`CanonicalPair`（Math.Min/Max）规范化存储，UNIQUE 约束防重复。
    - 黑名单：复用 `public."T_BlockRecords"`（与 `NpgsqlBlockListStore` 共享），
      `ON CONFLICT DO NOTHING` 幂等写入。
    - 列表查询：OFFSET-based 游标分页（base64 编码 int32 offset）。
  - `RealtimeDatabaseSchema`：新增 `FriendRequestsTableSql` / `FriendshipsTableSql` /
    `RelationshipMutationRequestsTableSql` 属性。
- **DI 注册**（三处 + NATS）：
  - `RealtimeCoreRegistration`：`NoopRelationshipStore` + `DefaultRelationshipCommandProcessor` +
    `DefaultRelationshipListQueryProcessor` + Noop consumers（默认注册）。
  - `RealtimePostgresRegistration`：`NpgsqlRelationshipStore`（RemoveAll + AddSingleton 覆盖）。
  - `RealtimeServicesRegistration`：`RelationshipCommandWorker` + `RelationshipListQueryWorker`。
  - `RealtimeNatsRegistration`：`NatsRelationshipCommandConsumer` +
    `NatsRelationshipListQueryConsumer`（覆盖 Noop 默认）。
- **附带修复**：`CapturingAttachmentStore`（RealtimeTests）补充 `FinalizeUploadAsync` 方法
  （预存在的测试 mock 缺失，非本次引入）。
- 构建：RealtimeServices.slnx 0 警告 0 错误；TcpGateway.sln 0 警告 0 错误。
- 测试：TcpGateway 440/440 通过无回归。

### 主线四-关系：Relationship 后端 Gateway 适配闭环（已完成）

Gateway 侧 `IRelationshipBackend` 端口已从 stub 替换为真实 RealtimeServices 适配实现，
关系变更命令（`RelationshipCommandRequest`）与列表查询（`RelationshipListRequest`）
端到端打通：

- **RealtimeServices 侧**（sibling 仓库）：
  - `RelationshipCommand` / `RelationshipCommandResult` / `RelationshipListQuery` /
    `RelationshipListResult` / `RelationshipListItem` / `RelationshipOperation` /
    `RelationshipListType` 定义于 `ChatApp.Realtime.Abstractions.Relationships`。
  - `NatsRelationshipCommandConsumer` + `NatsRelationshipListQueryConsumer`
    （`Infrastructure.Nats`）消费 Core NATS request/reply。
  - `RelationshipCommandWorker` + `RelationshipListQueryWorker`
    （`ChatApp.RealtimeServices`）并发处理命令/查询，含过载保护与 metrics。
  - `NoopRelationshipCommandConsumer`（`Infrastructure.Core`）供测试注入。
  - `IRealtimeMessageBus.MutateRelationshipAsync` / `QueryRelationshipListAsync` +
    `NatsRealtimeMessageBus` / `RealtimeRequestClient` 实现 +
    `RealtimeJsonSerializerContext` 注册 6 个 Relationship DTO 类型。
- **Gateway 侧**（本仓库）：
  - `RealtimeRelationshipBackend`（`Gateway/Commands/Relationships/IRelationshipBackend.cs`）
    注入 `IRealtimeMessageBus`，构造 `RelationshipCommand` / `RelationshipListQuery` 转发
    并映射 `RelationshipCommandResult` / `RelationshipListResult` →
    `RelationshipCommandBackendResult` / `RelationshipListBackendResult`。
    使用 `Realtime*` using 别名解决 Core / Realtime 枚举命名冲突
    （`RelationshipOperation` / `RelationshipListType`）。
    总线异常不吞咽，由 `RelationshipCommandHandler` catch-all 统一映射为
    `relationship_service_unavailable`。
  - `Program.cs` 注册 `RealtimeRelationshipBackend` 替换 `StubRelationshipBackend`
    （stub 保留供单测注入）。
- 测试：440/440 通过无回归；8 个 `IRealtimeMessageBus` 测试替身补充
  `MutateRelationshipAsync` / `QueryRelationshipListAsync` stub 实现。

### 主线四-附件：Attachment Finalize 后端闭环（已完成）

Gateway 侧 `IAttachmentBackend` 端口已从 stub 替换为真实 RealtimeServices 适配实现，
附件上传确认命令（`AttachmentFinalizeRequest`）端到端打通：

- **RealtimeServices 侧**（sibling 仓库）：
  - `AttachmentFinalizeCommand` / `AttachmentFinalizeResult` /
    `IAttachmentFinalizeProcessor` 定义于 `ChatApp.Realtime.Abstractions.Attachments`。
  - `DefaultAttachmentFinalizeProcessor`（`Infrastructure.Core`）执行校验 +
    `IRealtimeAttachmentStore.FinalizeUploadAsync`（Ticketed→Uploaded 原子 UPDATE）。
  - `NatsAttachmentFinalizeConsumer` + `AttachmentFinalizeWorker`（`Infrastructure.Nats`/
    `ChatApp.RealtimeServices`）消费 Core NATS request/reply。
  - `Migration051` 放宽 `attachments.status` CHECK 至 `0..6`（Uploaded=4/Scanning=5/Rejected=6）。
  - `IRealtimeMessageBus.FinalizeAttachmentUploadAsync` + `NatsRealtimeMessageBus`/
    `RealtimeRequestClient` 实现 + `RealtimeJsonSerializerContext`/`RealtimeWireSerializer` 注册。
- **Gateway 侧**（本仓库）：
  - `RealtimeAttachmentBackend`（`Gateway/Commands/Attachments/IAttachmentBackend.cs`）注入
    `IRealtimeMessageBus`，构造 `AttachmentFinalizeCommand` 转发并映射 `AttachmentFinalizeResult`
    → `AttachmentFinalizeBackendResult`。总线异常（含 `RealtimeServerBusyException`、NATS 超时）
    不吞咽，由 `AttachmentCommandHandler` catch-all 统一映射为 `attachment_service_unavailable`，
    与 `GroupCommandHandler` 等其他 Realtime 命令处理器异常约定一致。
  - `Program.cs` 注册 `RealtimeAttachmentBackend` 替换 `StubAttachmentBackend`
    （stub 保留供单测注入，不依赖 NATS 总线）。
- 测试：381/381 通过无回归；8 个 `IRealtimeMessageBus` 测试替身补充
  `FinalizeAttachmentUploadAsync` stub 实现。

### 主线三：Group 后端分页和稳定指纹（已完成）

经核验 RealtimeServices 仓库，跨仓库待补项均已实现（非 stub）：

- **`NpgsqlRealtimeGroupStore`**（`ChatApp.Realtime.Infrastructure.Postgres/Stores/`，约 2000 行）
  完整实现 `IRealtimeGroupStore` 全部方法：
  - 权限矩阵：AddMembers/RemoveMember 仅 Owner/Admin，ChangeRole/Dissolve 仅 Owner。
  - 群主转让：不能转让给自己；Owner 不能自降级（须先转让）。
  - 最后 Owner 退群：拒绝（"Owner 退群前须先转让所有权"）。
  - 审计 Outbox：事务内 `IGroupOperationAuditStore.RecordInTransactionAsync`，失败回滚。
  - 幂等账本：`group_mutation_requests`（Migration024，`FOR UPDATE` 串行化）。
  - 软删除：`left_at_ms`/`dissolved_at_ms`（Migration029），重新加群复活原行。
  - membership periods（Migration035/038）、`audience_version` 递增（Migration049）、
    用户生命周期 advisory lock。
- **`NpgsqlGroupOperationAuditStore`** 双路径：`RecordAsync`（事务外 best-effort）+
  `RecordInTransactionAsync`（事务内 Outbox，复用调用方连接/事务）。
  审计表 `group_operation_audit`（Migration028）。
- **keyset 分页**：设计决策为 Realtime 返回全量成员（`role, joined_at_ms, user_id` 升序），
  Gateway 本地 `PaginateMembers` 切片，cursor 编码 `(role, joined_at_ms, user_id)` base64，
  避免改动 RealtimeServices 协议；幂等缓存全量结果，翻页命中同一缓存。

### 主线一-c：真实 FCM/APNs/WebPush Provider 骨架（已完成）

PushWorker 侧三个真实 Provider client + payload builder 已实现（构建通过，381/381 测试无回归）：

- **FcmPushProvider**：OAuth2 Service Account JWT（RS256 签名，1h 有效 5min 提前刷新）换取
  access token；HTTP v1 API `POST /v1/projects/{projectId}/messages:send`；`FcmPayloadBuilder`
  构造含 `notification` / `data` / `android.collapse_key` / `apns.headers[apns-collapse-id]` 的请求体。
- **ApnsPushProvider**：Provider JWT（p8 私钥 ES256 签名，1h 有效 5min 提前刷新）；
  `POST /3/device/{token}` HTTP/2；`apns-push-type`/`apns-priority`/`apns-collapse-id` 头；
  `ApnsPayloadBuilder` 构造 `aps.alert` + `[JsonExtensionData]` 自定义字段。
- **WebPushPushProvider**：RFC 8291 AES128GCM 加密（`WebPushEncryptor`）+ VAPID JWT（ES256）认证；
  订阅 JSON 解析（`WebPushSubscription.Parse`）；`Topic` 头实现折叠；`TTL` 2419200。
- **DI 注册**：`PushProviderRegistrationExtensions.AddRealPushProviders` 仅在 Production 模式下注册
  三个 typed `HttpClient`（HTTP/2）+ `IPushProvider`；`PushProviderOptions` 统一配置平台凭证与超时。
- **错误映射**：HTTP 429 → `rate_limited`，5xx → `provider_unavailable`，410/Unregistered →
  `invalid_token`，400 → `payload_too_large`/`invalid_token`，403 → 清缓存 + `provider_unavailable`。

## 2026-08-01

### 主线二：Resume 真正事务化（已完成）

全部 8 项已实现：

- **Token Claim/Commit/Abort**：Resume Prepare 阶段改用 `TryClaim`/`CommitClaim`/`ReleaseClaim`
  Lua 原子操作，替代 `GETDEL`，防止 token 在 Commit 前被消费。
- **AdmissionState 三态**：Session 跟踪显式 `AdmissionState`（Unauthenticated/Promoted/Released），
  防止连接计数泄漏；Resume Commit 使用 `AdmissionPromoted` 显式状态标记。
- **TakeOver 顺序与回滚**：Redis 接管流程先 TakeOver 后关本地旧连接；
  TakeOver 失败时 `RollbackResumeLocalStateAsync` 回滚 `UserSessionRegistry.Add` 与 Presence 发布。
- **DependencyUnavailable 可重试**：`ResumeTokenValidationResult` 新增 `DependencyUnavailable` 状态，
  区分 Redis 失败与无效 token；`RedisResumeTokenStore.TryValidateAsync` Redis 失败时抛
  `RedisException`（非返回 null），确保 fail-closed。
- **同设备 fencing**：`ResumeVerification` 注入确定性 `DeviceIdHash`（按 userId 派生，黄金比例乘子 +1
  保证非零），`TakeOverSameDevice` 按 UserId + DeviceIdHash + 不同 ConnectionLeaseId 匹配
  （忽略 SessionId）。
- **旧 Socket 关闭验证**：`TakeoverCompetitionScenario` 校验旧 Socket `ReadClosed`/`WriteClosed`
  （800ms 传播延迟）+ Redis lease 探针验证 transportId 变更与 sessionId 一致。
- **SessionRevokedPayload 结构化**：`SessionRevoked` 事件改用 `SessionRevokedPayload { transportId }`
  结构化负载，替代裸 `ConnectionLeaseId`。
- **TakeOverUnavailable 指标**：`GatewayMetrics` 新增 `ResumeFailureReason.TakeOverUnavailable`
  （takeover_unavailable）；`SessionLifecycleCoordinator` 捕获 TakeOver 异常并以
  `AuthenticationRejected` 关闭连接，确保 fail-closed。

### 主线一：Push 正式闭环（Gateway 侧已完成）

Gateway 侧 Push 闭环已全部完成，跨仓库待补项见 `roadmap-todo.md` 主线一：

- **配置 Fail-fast**：`Push.Enabled` 默认 false；`Push.ProviderMode` 须为
  `Disabled` / `TestNoop` / `Production`；`Production` 模式下任一平台使用 `NoopPushProvider`
  启动失败；`NoopPushProvider` 仅用于 Development/Test 且返回 retryable 失败；
  `PushOptions` 门控防止静默吞推；启动校验 push 配置。
- **PushDispatchDisposition**：`PushDispatcher` 返回 `PushDispatchDisposition`
  （NoTargets / FullySucceeded / PermanentlyCompleted / Retryable / PartiallyRetryable）
  决定 ACK/NAK 行为；`PushConsumer` 仅对 FullySucceeded/NoTargets/PermanentlyCompleted ACK，
  Retryable NAK，PartiallyRetryable 仅重试失败 Token。
- **Token Retry**：仅重试失败 Token，非整批重投。
- **幂等**：复合键 UserId + CommandKind + RequestId + CanonicalPayloadHash；Redis L2 缓存。
- **DLQ**：永久失败消息进入死信队列。
- **Provider 并发限制**：限制并发调用 Provider client。
- **无效 Token 注销**：Provider 返回无效 Token 时自动注销。
- **PushWorker 拆出**：`PushConsumer` 与 Provider 调用从 TCP Gateway 移至独立 PushWorker 服务，
  隔离网络资源。
- **AES-GCM Token 加密**：Push Token 静态加密。
- **日志降级**：Push delivery payload 不在 Information 级别记录。

### 主线四：附件和 Relationship（Gateway 协议层已完成）

Gateway 侧协议命令、DTO、Handler、端口 + Stub 已全部实现，跨仓库待补项见 `roadmap-todo.md` 主线四：

- **Attachment 协议层**：`IAttachmentBackend` 端口 + `AttachmentCommandHandler`（当前 Stub 返回
  `attachment_service_unavailable`）；Initiate/Finalize C2S 命令路由接入 `CommandDispatcher`。
- **Relationship 协议层**：`IRelationshipBackend` 端口 + `RelationshipCommandHandler`
  （当前 Stub 返回 `relationship_service_unavailable`）；C2S 命令路由接入 `CommandDispatcher`。
- **DI 注册**：`Program.cs` 注册端口接口与 Handler。
- **GatewayLog Stubs**：`GatewayLog.Stubs.cs` 为新命令提供日志存根。
- **测试调整**：3 个测试文件 + `GatewayLogContractTests.cs` 同步调整。
- **枚举对齐**：Gateway `AttachmentWireStatus` 6 状态 vs Realtime 2 状态前 2 值对齐，
  扩展状态（UploadConfirmed/Rejected/Expired/ThumbnailUpdated）仅由
  `AttachmentLifecycleHandler` 下游推送使用，不参与 `AttachmentWireMapper` 映射。

提交 `bc09d1b`：48 文件，+2020/-181 行，366/366 测试通过。
## 2026-07-31

### 八.1 PerSessionDrain 零分配 CAS

- `DrainOperation` 改用 `ManualResetValueTaskSourceCore<bool>` 实现 `IValueTaskSource`，
  单实例跨代次复用，消除每入队 `TaskCompletionSource` 分配。
- 用 packed `long`（`_drainStateGen`）合并 Running 位 + 32-bit generation，
  替代对象引用 CAS，实现零分配原子状态转换。
- `SpinLock` 串行化 `Reset`/`Complete`，防止跨代次竞态（旧代次完成不污染新代次）。
- `DrainOperation.Complete` 增加代次校验 + 幂等保护（`_completed` 标志）。

### 八.4 心跳队列安全门指标

- `HeartbeatRefreshWork` 增加 `EnqueuedAtTimestamp`，跟踪队列项排队年龄。
- 新增 `_oldestEnqueueTimestamp` + `_tickInterval`，队列空→非空时 CAS 记录最老项。
- `RunAsync` 跟踪 tick schedule_lag（实际 vs 计划触发时间）与 full_cycle.duration。
- `WorkerLoopAsync` 检测排队超时（>tickInterval），记录 refresh.overdue 计数。
- 新增指标：`gateway.heartbeat.queue.depth` / `queue.oldest_age`（ObservableGauge）、
  `schedule_lag` / `full_cycle.duration`（Histogram）、`refresh.overdue`（Counter，tag: kind）。
- `TcpGatewayService` 注册 queue.depth/oldest_age 观察者回调。

### 群聊幂等指纹 SHA-256 稳定化 + Redis L2 条件写 + Realtime Consumer 异常传播

- 群组幂等 payload hash 改用 SHA-256（归一化二进制表示），替代 `System.HashCode`
  （进程随机种子，跨进程不稳定）。
- Redis L2 idempotency `TryAdd` 改为条件 Lua 写（仅当 key 缺失或存储 payloadHash 匹配才写），
  防止并发 Miss last-writer-wins 覆写。
- Realtime Consumer Worker 故障须先 `await workersTask` 捕获根因，再取消 mainLoop，
  防止 `OperationCanceledException` 掩盖真实 worker 异常。
- Realtime Consumer Drain 窗口使用独立 `CancellationTokenSource.CancelAfter(DrainTimeout)`，
  不链接已取消的 stoppingToken，保留 30s drain 预算。

### P0 生产路径关键缺陷修复（8 项）

- Redis 熔断器拆分（ResumeToken / DeviceLease / GroupIdempotency / PushToken / RoutingDirectory），
  防止跨域故障耦合。
- Resume Prepare 阶段改用 `TryClaim`/`CommitClaim`/`ReleaseClaim` Lua 原子操作，替代 `GETDEL`，
  防止 token 在 Commit 前被消费。
- Session 跟踪显式 `AdmissionState`（Unauthenticated/Promoted/Released），防止连接计数泄漏。
- `DomainWorkLane` 改用 `Channel<TWork>` 替代 `BoundedMpmcRing` + `SemaphoreSlim` + `_signalState`。
- `DomainWorkLane` worker 按 burst（1-8 项）处理 + 每项 stopToken 检查，防止停机后批量执行。
- `ActorRuntime.TryTellDurable` 区分新 Actor 激活（全局配额 + Shard 限制）与已有 Actor 投递
  （仅 Mailbox 容量约束）。
- `RedisDeviceSessionLeaseStore.TakeOverAsync` Redis 失败时抛 `RedisException`（非返回 null），
  确保 fail-closed。
- `ResumeVerification` 注入确定性 `DeviceIdHash`（按 userId 派生），支持同设备 fencing。

## 2026-07-30

### Resume 闭环验证

- Redis owner 探针 + NATS 故障本机关闭。
- takeover-competition 保留旧 Socket 读写断言（`ReadClosed` / `WriteClosed`，800ms 传播延迟）。
- Resume error code 区分 + 10k reconnect storm 压测。

## 2026-07-29

### Resume 可靠性补强

- Redis 应用层熔断器（`IRedisCircuitBreaker` / `RedisCircuitBreaker`）：
  Closed→Open→HalfOpen 三状态机，连续失败阈值 `CircuitBreakerFailureThreshold`（默认 5），
  开路时长 `CircuitBreakerOpenDuration`（默认 5s）。
- `RevokeAsync` DemandMaster，防止主从切换期间撤销失效。
- Resume 路径广播 `SessionRevoked`，跨 Gateway 旧 session 不继续发送出站帧。
- Resume 可观测性指标：`gateway.resume.attempts/succeeded/failed`（tag: reason）、
  `gateway.redis.circuit_breaker.open`。

### 群聊 RequestId 幂等缓存

- `GroupRequestIdempotencyCache`：有界 TTL+LRU，4096 条/30s，键为 `(ActorUserId, RequestId)`。
- 仅缓存 Realtime 正常返回（含业务失败），异常路径不缓存可重试。
- 指标：`gateway.group.idempotent.hit` / `miss` / `conflict` /
  `redis_hit` / `redis_miss` / `redis_failure`。

### R1 真实 Ephemeral A/B 测试

- 覆盖三条 Ephemeral 管道（Legacy / Actor Generic / Specialized TypingActor）。
- 10+1 场景全覆盖：授权缓存命中/未命中/拒绝、NATS 延迟/断线、同 Key 高频覆盖、
  连接 churn、10k 活跃 Actor、单热点 Actor、预算归零、Legacy vs TypingActor 对比。
- 关键 Bug 修复：`BoundedMpscRing.TryDequeue` 多消费者竞态、
  `ReceiveAuthorizationCompleted` 主动 TryEmit。

## 2026-07-28

### DirectSocket 默认切换

- `InboundTransportMode` 默认从 `Pipelines` 切换为 `DirectSocket`。
- Linux 真实集成短测通过：200 认证 Heartbeat 连接，1,216/1,216 持久 Pipeline 成功，
  40.12/s，p95/p99 253.5/364.5 ms。
- 修复 RealtimeServices `ResolveSyncWatermarksAsync` 在 Npgsql `SequentialAccess` 下
  先读第 6 列再回读第 5 列的错误。

## 早期已完成（按主题归并，日期不详）

### Runtime V2 Phase 1+2

- `TcpClientSession` 拆分为 `Outbound`（Durable FIFO + Ephemeral keyed mailbox + 两种发送驱动）
  与 `Transport`（Close/Dispose、deadline、SendFrameAsync）partial。
- 全局共享执行器替代每连接资源：`DeadlineWheel`、`SessionCommandExecutor`、`OutboundPumpCoordinator`。
- `OutboundSendMode` A/B：`PersistentSendLoop`（默认）vs `OnDemandSendPump`。
- `SessionLifecycleCoordinator` 拆分为 `Presence` 与 `DeviceSession` partial；
  `GroupCommandHandler` 拆分为 `Create`/`Mutate`/`Helpers`；
  `TcpGatewayService` 由 ~1800 行降至当时 669 行；
  `RealtimeEventDispatcher` 由 1316 行降至 190 行。
- `DeadlineWheel` 计数器修复：`_fired` 集合使 `Cancel` 在已触发注册上幂等忽略
  （**注：`_fired` 后续已移除**，改用分桶 + 计数器）。
- `SessionCommandExecutor.StopAsync` 移除 early-return，确保排空队列释放预算与缓冲。

### 轻量 Actor Runtime 接入

- BCL-only `ActorRuntime/`：分片单消费者、有界 MPSC 入口、intrusive ready queue、
  前 4 项内联 + `ArrayPool<T>` 溢出环形邮箱。
- `EphemeralCommandPipeline` 接入 `SessionRuntime`，由 `UseActorRuntimeForEphemeralCommands` 切换。
- 设计与实测见 `docs/actor-runtime.md`。

### P0 热路径性能修复

- 心跳分桶扫描：`HeartbeatBucketRegistry` 按 connectionId/userId 分桶，每 tick 仅枚举一桶，
  不再 `_sessions.Values.ToArray()` 全量扫描。
- 心跳刷新指标正确区分成功/失败（`RefreshLeaseAsync` / `RefreshPresenceAsync` 返回 bool）。
- `CommandContext` 转为 readonly struct，消除每命令堆分配。
- `TypingFanoutHost` 有界发布：keyed pending + 单槽唤醒 + 固定 publisher worker。
- `ResponseByteBudget` 边界修复：`TruncateOutcome` 枚举，item_too_large 错误。
- Presence Redis 热路径：Lua 移除 `ZREMRANGEBYSCORE`，`ZCOUNT` 替代；`RunMaintenanceAsync` 后台清理。
- Realtime JetStream 分区并行消费：按 `TargetUserId % PartitionCount` 路由。

### P1 业务闭环

- Resume 同步水位恢复（`LastConversationSequence`）。
- 群聊廉价结构校验（`GroupCommandHandler.Validation.cs`）。
- Push Token 注册原子化（Lua 合并 HSET+ZADD+淘汰+TTL，Hash tag 对齐 Cluster slot）。
- Relationship 授权缓存主动失效（friendship/blocked-user 双向失效）。
- 协议版本与兼容性基础（`GatewayFeature`、`ProtocolPayloadFormat`、`Deprecated`、
  最低客户端版本、FeatureBits 强制检查、命令级能力协商）。

### 性能基线与故障注入

- 本机 30 分钟基线：1000 TCP 长连接，94,715 持久链路成功，52.61 pipeline/s。
- Outbox 优化 A/B：吞吐 +120.2%，p95/p99 -71.9%/-62.6%。
- History SQL 放大消除：扫描行 1,535→11，缓冲页 1,162→17，执行时间 6.055ms→0.071ms。
- 容量曲线：120/s 目标 5 分钟持续 115.35/s，34,626 成功。
- 故障注入短测：Garnet 568/568、PostgreSQL 575/575、NATS pause/unpause 513/513。
- 多进程编排器统一启动/采样/报告；`ChatApp.Performance.Gate` 失败闭环检查。

### 可观测性部署

- Gateway 与 RealtimeServices 接入 OpenTelemetry Metrics/Tracing + OTLP。
- W3C Trace Context 贯穿全链路。
- Linux 测试机部署 Prometheus + Grafana + 初始告警规则 + 仪表盘（`deploy/observability/`）。
