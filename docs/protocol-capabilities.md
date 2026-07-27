# 协议版本与能力协商

## 握手

客户端先发送 `ClientHello`，服务端校验
`MinimumClientProtocolVersion <= protocolVersion <= CurrentProtocolVersion`，
再返回 `ServerHello`。返回的版本是本次连接实际选定的客户端版本，
`featureBits` 是客户端声明与服务端已实现能力的交集。
运行时关闭 Resume 或 Presence/Typing 时，对应能力也会从服务端可用掩码中移除。

`TcpGateway:MinimumClientProtocolVersion` 默认等于编译期最低版本，可在淘汰旧客户端时
逐步提高，但不能超出 `PacketProtocol` 的编译期支持区间。

## 兼容模式与严格模式

- 未协商 `CommandCapabilities`：保持 v1 行为，已认证客户端可继续使用所有现有命令。
- 协商 `CommandCapabilities`：启用严格门控；扩展命令还必须包含
  `CommandDescriptor.RequiredFeature` 指定的能力。
- 未满足能力的命令返回非致命 `FeatureNotNegotiated`，不进入业务 handler，
  连接保持可用。
- `BinaryPayload`、`Compression`、`StreamingChat` 已保留 wire 位，但尚未实现，
  服务端不会在 `ServerHello` 中回显。

严格模式目前覆盖：

| 能力 | 命令族 |
|---|---|
| `SessionResume` | `ClientHello.resumeToken` |
| `ConversationSync` | `SyncBootstrapRequest` |
| `ConversationPreferences` | `ConversationSetPrefsRequest` |
| `MessageMutation` | 消息编辑、撤回 |
| `PresenceAndTyping` | Typing、Presence query/unwatch |
| `MessageReactions` | Reaction add/remove |
| `GroupManagement` | 群组管理与成员列表 |
| `PushTokenManagement` | Push Token 注册/注销 |

聊天、回执、会话列表、历史查询、MarkRead、Heartbeat 和认证仍是 v1 核心命令，
不要求扩展能力。

## 性能约束

协商结果在握手完成时一次性写入 `TcpClientSession`。帧调度热路径只读取一个
`uint` 位掩码并执行按位比较；同一次 `CommandDescriptor` 查询同时提供速率成本、
弃用状态、能力要求和执行 lane，不引入每帧对象分配或额外 I/O。

## 新增协议功能

1. 在 `GatewayFeature` 分配新的单比特值，尚未实现的位不得加入
   `GatewayFeatureSet.Implemented`。
2. 完成服务端实现后加入 `Implemented`。
3. 在 `CommandCatalog` 为相关客户端命令设置 `RequiredFeature`。
4. 补充 catalog 完整性测试，以及 DirectSocket/Pipelines 的真实 TCP 握手测试。
5. JSON 仍是当前 wire 格式。未来二进制 codec 必须通过 `BinaryPayload` 协商，
   并保留 JSON 回退路径。
