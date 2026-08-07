# Gateway / Realtime 契约归一边界

本文件记录 Gateway 对 `ChatApp.Realtime.Contracts 2.3.0` 的类型所有权决策。判断标准是：
业务语义、属性集合/类型、枚举数值和 camelCase JSON wire 必须同时一致；仅同名不足以共享。

## 已迁移为 Realtime 包直接类型

| Gateway 历史名称 | 共享类型 | 结论 |
| --- | --- | --- |
| `AttachmentRef` / `AttachmentWireStatus` | `Messaging.AttachmentRef` / `AttachmentWireStatus` | 消息/历史中的附件引用仅表达 Scanning/Available，属性与数值一致；原逐字段 mapper 已移除。 |
| `ConversationListCursor` | `Conversations.ConversationListCursor` | 排序游标字段、顺序和语义一致。 |
| `ConversationListItem` | `Conversations.ConversationListItem` | 全部属性与列表展示语义一致。 |
| `ConversationMemberItem` / `ConversationMemberRole` | `Conversations` 同名类型 | 成员字段及 Owner=1、Admin=2、Member=3 一致。 |
| `ConversationType` | `Conversations.ConversationType` | Direct=1、Group=2 一致。 |
| `MessageReactionSummary` | `Messaging.History.MessageReactionSummary` | Emoji/Count/ReactedByMe 一致。 |
| `RelationshipOperation` / `RelationshipListType` | `Relationships` 同名枚举 | 业务操作及 byte 数值一致。 |
| `RelationshipItem` | `Relationships.RelationshipListItem` | 仅历史命名不同，属性与列表项语义一致。 |
| `RelationshipListResponse` | `Relationships.RelationshipListResult` | 请求结果字段完全一致；Gateway 名称通过 alias 过渡。 |
| `RelationshipChangeOperation` | `Relationships.RelationshipChangeOperation` | Upsert=0、Delete=1 一致。 |
| `RelationshipSyncWatermark` | `Sync.RelationshipSyncWatermark` | ListType/AfterSequence 及增量水位语义一致。 |
| `RelationshipChangeLogEntry` / `RelationshipCatchUp` | `Sync` 同名类型 | tombstone、分页和 retention 字段完全一致。 |
| `SyncCursorResetReason` | `Sync.SyncCursorResetReason` | 五个原因及 byte 数值一致。 |

Core、Gateway、Infrastructure 和测试工程暂用 global aliases 维持源码可读性；编译后的公开属性和 codec 泛型参数均直接指向 `ChatApp.Realtime.Contracts` 类型，Core 程序集中不再定义上述副本。

## 明确保留在 Gateway 的 wire 类型

| Gateway 类型 | Realtime 相似类型 | 不共享原因 |
| --- | --- | --- |
| `AttachmentLifecycleUpdate` | `AttachmentRef.Status` | 生命周期事件允许 0..5；共享附件引用状态只有 Scanning=0、Available=1。事件 `Status` 因此保留 `short`。 |
| `MessageHistoryCursor` | `Messaging.History.MessageHistoryCursor` | v1 TCP 只有 ReceivedAtMs/MessageId；Realtime 还有 ChangedAtMs，并区分 Before/After 水位。 |
| `ConversationSyncWatermark` | `Sync.ConversationSyncWatermark` | v1 JSON 字段是 `afterReceivedAtMs`，Realtime 字段及真实语义是 `afterChangedAtMs`。 |
| `SyncCursorResetRequired` | `Sync.SyncCursorResetRequired` | v1 JSON 保留 Tip/ClientAfter `ReceivedAtMs` 字段名，Realtime 使用 `ChangedAtMs`。仅共享原因枚举。 |
| `MessageHistoryItem` / `ConversationHistoryCatchUp` | `RealtimeHistoryMessage` / Realtime catch-up | Realtime 消息包含 `conversationSequence` 等内部/新版字段；TCP v1 输出面更窄。 |
| `ConversationListResponse` / `MessageHistoryResponse` / `SyncBootstrapResponse` | Realtime page 类型 | Realtime page 含 RetryAfterMs/QueueKind，且内部消息项类型不同；Gateway 还承担单帧字节预算截断。 |
| `RelationshipCommandRequest/Response`、`RelationshipListRequest` | Realtime command/query/result | 内部命令含 ActorUserId/SessionId，command result 还含队列退避字段；TCP C2S/S2C 信封语义不同。 |

边界由 `RealtimeContractConsolidationTests` 固化：反射测试防止本地副本回流，枚举测试锁定数值，JSON golden 锁定 camelCase wire，负向测试保证上述相似类型不会被同名误合并。
