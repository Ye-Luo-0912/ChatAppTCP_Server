# TCP 网关与 RealtimeServices 消息链路

TCP 网关只负责连接、鉴权、协议和本地在线投递。消息数据库、状态推进和 Outbox
由同级 ChatApp.RealtimeServices 完成；网关引用其 ChatApp.Realtime.Integration
模块，不复制 NATS/JetStream 实现。

## 消息与回执数据流

~~~mermaid
sequenceDiagram
    participant S as 发送方客户端
    participant G as TCP Gateway
    participant N as NATS JetStream
    participant R as RealtimeServices
    participant P as PostgreSQL / Outbox
    participant D as 接收方客户端

    S->>G: ChatMessage(ClientMessageId)
    G->>N: IncomingMessageCommand
    N-->>G: Publish ACK
    G-->>S: MessageAcknowledgement(Accepted=true)
    N->>R: chat.incoming-messages
    R->>P: 消息 + MessageReceived Outbox 同事务
    P->>N: MessageReceived
    N->>G: chat.realtime-events
    G-->>D: ChatMessage(Server MessageId)
    D->>G: MessageReceipt(Delivered / Read)
    G->>N: MessageReceiptCommand
    N-->>G: Publish ACK
    G-->>D: MessageReceiptAcknowledgement(Accepted=true)
    N->>R: chat.message-receipts
    R->>P: 状态推进 + MessageReceiptUpdated Outbox 同事务
    P->>N: MessageReceiptUpdated
    N->>G: chat.realtime-events
    G-->>S: MessageReceiptUpdated
~~~

## 历史查询与断线恢复

历史查询是可重试的只读操作，使用 chat.message-history.query 的 Core NATS
request/reply，不进入 JetStream 持久命令流，也不增加消息写入和回执热路径延迟。

- TCP 命令 MessageHistoryRequest(106) 请求一页，MessageHistoryPage(107) 返回结果。
- 可选 `ConversationId`：按会话查询；省略时保持用户级全量历史（旧客户端兼容）。
- 会话历史使用索引 `(conversation_id, received_at_ms DESC, message_id DESC)` 与
  keyset 游标 `(ReceivedAtMs, MessageId)`；非成员返回 forbidden。
- 客户端不发送 UserId；网关始终注入当前已认证会话的 UserId。
- 首次请求不带游标，返回最新一页；NextCursor 由
  (ReceivedAtMs, MessageId) 组成，用于继续向更早消息翻页。
- 重连时先取最新页并按 MessageId 去重；若尚未遇到本地最后已知消息，再使用
  NextCursor 逐页回溯，避免在登录热路径无限自动补发。
- 可选 `AfterReceivedAtMs` / `AfterMessageId`：仅在与 `ConversationId` 同时提供时
  向前（更新）翻页；与 `Before*` 游标互斥。
- 单页默认 50、最多 100 条；服务端限制累计响应大小，TCP 网关查询超时为 3 秒。
- PostgreSQL 使用收件人/发件人复合索引和 keyset pagination，不使用 OFFSET。
- 查询超时或服务不可用时返回失败页，不关闭已认证 TCP 连接。

## 契约和确认语义

- 客户端上行 ChatMessage.MessageId 是 ClientMessageId，最大 128 字符。
- 未提供 ClientMessageId 时网关生成 UUID v7；消息重试必须复用同一 ID。
- 网关按 SHA256(SenderUserId:ClientMessageId) 生成 64 字符消息 CommandId。
- MessageAcknowledgement.Accepted=true 只表示 JetStream 接收了消息命令。
- 客户端收到服务端 MessageId 后，用 MessageReceipt 上报 Delivered 或 Read。
- 网关按 SHA256(ReceiverUserId:MessageId:ReceiptType) 生成回执 CommandId。
- MessageReceiptAcknowledgement.Accepted=true 只表示 JetStream 接收了回执命令。
- MessageReceiptUpdated 才表示 RealtimeServices 已持久化状态并完成 Outbox 发布。
- Read 会隐式补齐 Delivered；每种状态只首次推进，重复或乱序回执不会回退。
- 当前单聊回执是用户级状态，会推送到原发送者的所有在线设备。
- `delivered_at_ms` / `read_at_ms` 是用户级字段：任一在线设备上报 Delivered 或 Read
  即推进该用户对该消息的状态；**没有**按设备维度的离线投递账本。
- 下行采用至少一次语义，客户端必须按 MessageId 和状态去重。

## 数据一致性

- 消息通过 (SenderUserId, ClientMessageId) 唯一约束幂等。
- 消息与 MessageReceived Outbox 在同一 PostgreSQL 事务提交。
- 回执状态更新与 MessageReceiptUpdated Outbox 也在同一事务提交。
- 单聊会话 ID 为确定性字符串 `dm:{minUserId}:{maxUserId}`；写入消息时同事务
  投影 `conversations` / `conversation_members`，并用
  `(last_message_at_ms, last_message_id)` 条件更新，保证重复或乱序投递不回退摘要。
- 会话摘要前进时，向双方用户各写一条 `ConversationListChanged`
  （业务名 ConversationChanged；EventId =
  SHA256(`convchg:{conversationId}:{lastMessageId}:{targetUserId}`)）。
- Gateway 下行 `ChatMessage` 携带 `ConversationId`；`ConversationListChanged`
  映射为 TCP `ConversationChanged`(112)，`UnreadCountChanged` 映射为
  TCP `UnreadCountChanged`(113)。
- 会话列表与已读标记使用 Core NATS request/reply：
  `chat.conversation-list.query` 与 `chat.conversation-mark-read`。
- TCP 命令 ConversationListRequest(108) / ConversationListPage(109) 查询列表；
  ConversationMarkReadRequest(110) / ConversationMarkReadResponse(111) 推进已读。
- 客户端不发送 UserId；网关注入当前会话 UserId。查询超时复用 3 秒。
- 网关完成本地排队后 ACK 事件；异常时 NAK 并请求重投。
- 目标用户不在线时网关 ACK 自己的事件副本，数据库仍是事实来源。

## 多设备同步与重连

重连后客户端应优先使用 **SyncBootstrap**（TCP 114/115，NATS
`chat.sync.bootstrap`）一次拉取会话列表与待补偿历史；也可退化为
ConversationListRequest(108) + MessageHistoryRequest(106) 组合。

- SyncBootstrapRequest(114) 携带各会话本地水位
  `(ConversationId, AfterReceivedAtMs, AfterMessageId)`；网关注入 UserId，
  不接收客户端 UserId。
- SyncBootstrapResponse(115) 返回 `ServerTimeMs`、会话列表（含未读）、按
  未读/水位优先选取的 `CatchUps`，以及可选的 `ResetsRequired`（无效水位需全量恢复：
  `MessageNotFound` / `AheadOfTip` / `MembershipLost` / `GapTooLarge`，附 tip 提示）。
  消息保留 / tombstone horizon 驱动的失效尚未实现。
- 客户端合并下行实时 `ChatMessage` 与补偿历史时，**必须按 MessageId 去重**。
  收到 `ResetsRequired` 时应清除本地该会话游标并全量拉历史，不要当作增量成功。
- 发送方多设备：RealtimeServices 在持久化后向发送者其他在线设备推送
  `MessageReceived` 回声；网关 `RealtimeEventDispatcher` 会跳过与事件
  `SessionId` 相同的发起会话，避免本机重复收到自己刚发的消息。

## 多网关规则

每个网关实例使用独立 durable consumer，因此所有实例都会看到实时事件，只有持有
目标用户连接的实例执行在线投递。

RealtimeIntegration.InstanceId 必须同时满足：

1. 并行实例之间唯一。
2. 同一实例重启后稳定，以继续 durable consumer 进度。
3. 容器部署使用 Pod/实例稳定标识，不能让同主机进程共享默认值。

网关的 ManageStreams 必须为 false。Stream Subjects、容量、副本和保留期只由
RealtimeServices 管理。INCOMING_MESSAGES、MESSAGE_RECEIPTS 和
REALTIME_EVENTS 是独立 Stream；历史读取 subject 使用 Core NATS，不创建 Stream。

## 启动和失败处理

启动顺序：

1. PostgreSQL、NATS JetStream、Redis/Garnet。
2. ChatApp.RealtimeServices，由它校准 Stream、数据库结构和 Outbox。
3. ChatApp TCP Gateway。

失败语义：

- 消息发布失败：返回 message_bus_unavailable，复用 ClientMessageId 重试。
- 回执发布失败：返回 message_bus_unavailable，重试相同 MessageId 和状态。
- 历史查询失败：返回 history_service_unavailable，保持连接并允许稍后重试。
- 非法/越权回执：消息服务写入死信流，不修改消息状态。
- 临时处理或 ACK 异常：NAK 后安全重投。
- 慢 TCP 客户端：发送队列超限后断开，不阻塞消息消费者。
- SessionRevoked：只断开目标用户中 SessionId 精确匹配的连接。

## 实时事件契约

业务名与线协议类型对应关系（Gateway 只消费 Abstractions DTO）：

| 业务名 | Wire `RealtimeEventType` | TCP | Payload | EventId 配方 |
|---|---|---|---|---|
| ConversationChanged | `ConversationListChanged` (4) | 112 | `RealtimeConversationChangedPayload` v2 | SHA256(`convchg:{conversationId}:{lastMessageId}:{targetUserId}`)；偏好变更用 `convprefs:...` |
| UnreadCountChanged | `UnreadCountChanged` (10) | 113 | `RealtimeUnreadCountChangedPayload` v1 | SHA256(`unread:{conversationId}:{targetUserId}:{unreadCount}:{lastReadAtMs}:{lastReadMessageId}`) |
| MessageReceived | `MessageReceived` (5) | 101 | `RealtimeChatMessagePayload` v1 | SHA256(`{senderUserId}:{clientMessageId}`)；发送方回声 SHA256(`msgecho:{messageId}:{senderUserId}`) |
| MessageDelivered / MessageRead | `MessageReceiptUpdated` (7) + `ReceiptType` | 103 | `RealtimeMessageReceiptPayload` v1 | SHA256(`{messageId}:{receiverUserId}:{receiptTypeByte}`) |
| SessionInvalidated | `SessionRevoked` (6) | 断开连接 | （无业务 payload） | 由会话撤销流程生成 |

统一约定：

- `PayloadVersion` 缺省等于各 payload 的 `CurrentPayloadVersion`；未知更高版本字段应忽略。
- Outbox / JetStream 按 `EventId` 幂等；乱序时投影用复合序 max-merge，不回退。
- Gateway 按 wire 枚举分发，不解析 Realtime DB 模型。

## 下一阶段

阶段 1–5（会话模型、列表未读、会话历史、多设备 SyncBootstrap、事件契约对齐）已完成。
下一阶段继续性能门禁：用 `Run-ConversationCombo.ps1` 校准会话阶段延迟阈值，复跑浸泡，
并将版本化门禁接入 Linux 定时 CI；门禁稳定后再推进群聊等扩展能力。
