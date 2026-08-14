# 待办路线图

本文件是 TCP Gateway **唯一的详细执行清单**。当前事实见 [`roadmap-current-state.md`](roadmap-current-state.md)，完成记录见 [`roadmap-changelog.md`](roadmap-changelog.md)，跨仓摘要见 [`NEXT-STAGE.md`](NEXT-STAGE.md)。

修改前先读相关实现、调用方、上下游契约和测试，确认命令语义与资源所有权。每个批次只解决一个问题，不复制 Shared DTO，不引入无界队列、连接级后台线程或隐式格式切换。

## 批次顺序

| 顺序 | 批次 | 主要 Owner | 完成证据 |
| --- | --- | --- | --- |
| 1 | `REL-E2E-4` | Realtime → Gateway → Client | list/catch-up 真实联调、HTTP 对照、reset/gap/断线恢复 |
| 2 | `VOICE-MSG-2` | Server → Realtime → Shared → Gateway → Client | 上传到播放的端到端链路与错误恢复 |
| 3 | `CALL-E2E-2` | Server → Realtime → Shared → Gateway → Client | grant、信令状态机、跨 Gateway 与弱网恢复 |
| 支撑 | `BIN-INTEGRATION-3` | Shared → Gateway + Client | 完整命令覆盖、双 codec、JSON fallback、短测 |
| 支撑 | `PERF-SUPPORT-1` | 发生热点的仓库 | profiler 归因、微基准、同构短 A/B |

后续 Agent 一次只接一个批次，并在交接中写明修改范围、未解决风险和验证结果。

## P0：`REL-E2E-4` 关系读取

Server HTTP 与 public `T_*` 表继续作为唯一关系权威；Gateway 只提供读取映射，关系 mutation 不迁入 TCP。

1. 将 Realtime 投影 list/catch-up backend 接入现有 handler，使用 Shared `TcpRelationship*` 与 sync 类型做唯一 wire 输入；禁止从 Realtime 数据库实体或内部 DTO 自动序列化客户端响应。
2. 显式映射 owner/list/resource/version、opaque cursor/watermark、partial/reset 与稳定错误；响应预算裁剪后才能决定下一水位，不能返回伪空成功或 `HasMore` 缺 cursor。
3. 覆盖 unavailable、projection changed、gap、retention exceeded、invalid cursor、重复请求、分页中权限变化、断线续页和 capability 关闭。失败必须保留旧有效状态且不推动 Client 水位。
4. 以同一账户的 Server HTTP 好友、申请和黑名单列表逐项对照；出现差异时只修投影、mapper 或分页语义，禁止恢复 legacy Realtime 关系表或 Gateway 本地权威。
5. 完成 Client 首次加载、增量、reset、账户切换和多设备变化联调；用 5–20 分钟短测覆盖断线与续页，不把性能基准混进正确性结论。

完成标准：Client 仅依赖 TCP read 即可从 snapshot 收敛并持续 catch-up；所有 gap 都显式 reset，mutation 仍由 HTTP 完成，关闭能力后安全 fail-closed。

## P1：`VOICE-MSG-2` 语音消息

1. Shared 固定语音附件的 codec/container、MIME、duration、sample rate、channels、size 与可选 waveform；Gateway 只映射这些有界字段和附件引用。
2. 发送继续复用现有附件 ownership、finalize、扫描、绑定和消息幂等。只有 `Available` 附件可发送；扫描中、拒绝、过期、非本人和已绑定冲突返回稳定错误。
3. 历史、同步、撤回、保留清理和跨 Gateway delivery 与普通附件消息共享语义；音频正文、临时本地路径和下载票不得进入日志、Outbox 或 TCP payload。
4. 增加畸形元数据、超限、重复 client message id、断线重试和旧客户端未知字段测试；再联调录制 → 上传 → 发送 → 接收 → 播放。

完成标准：语音消息不新增独立传输栈，正常与失败路径均可恢复，关闭语音能力不影响普通附件和文本消息。

## P1：`CALL-E2E-2` 1:1 通话控制面

1. Shared 固定 CallInvite/Accept/Reject/Cancel/End/Reconnect 及必要 SDP/ICE envelope，包含 call id、command id、revision、TTL 和明确预算。
2. Gateway 校验 authenticated actor、Server 短期 call grant、好友/拉黑权限、并发通话上限和用户/连接速率；重复 command id 幂等，过期或乱序命令 fail-closed。
3. 复用 Realtime 临时状态机与非持久化 signal 路径完成跨 Gateway 路由；结束、超时和断线后清理 route，不把 SDP/ICE 写入 PostgreSQL、Outbox 或聊天 JetStream。
4. 覆盖双方同时发起/挂断、多设备竞争、Gateway 切换、依赖短暂中断、ICE restart 和旧 capability 客户端。控制面只决定状态，媒体使用 WebRTC/SRTP 与 ICE/STUN/TURN。

完成标准：所有信令序列得到唯一终态，鉴权和预算可测试，TCP Gateway 不承载连续媒体包或自定义 UDP 可靠层。

## 支撑：`BIN-INTEGRATION-3`

1. 仅在 Shared 当前 schema 批次完成且功能命令目录稳定后接入。握手与协商前保持 JSON，首版 Resume 也保持 JSON；协商后 session 固定 exact format，不 sniff、不在线切换。
2. 入站使用 Shared generated decoder，出站使用普通源码 encoder；Gateway 负责 buffer owner、池归还和敏感区清理，不复制 Core 指针实现或 schema。
3. 混合连接按 format 分组，每个事件每种格式最多编码一次并共享只读 frame；不能退化为逐 session 序列化。
4. 用真实 Chat/History/Sync/关系/语音/通话 corpus 比较 payload、CPU、allocation 和 p95/p99；完成 malformed/oversize/fuzz、fallback、GoAway/重连与 80/320/640 msg/s 短测。收益不足时继续使用 JSON。

## 支撑：`PERF-SUPPORT-1`

1. 先读取现有 `TCP-MEM-1`、数据库和 soak 证据，仅为当前功能回归或 profiler Top 热点补采样。
2. 微基准或聚焦计数先证明原因，再一次修改一个因素；优先处理可测量的 per-session retained、allocation/message、CPU sample、DB ops/WAL 或 p99 问题。
3. 原实现与候选至少做两轮交错的 3–5 分钟同构 A/B；保持连接 ramp、负载、配置和依赖一致。只有趋势不清时补一次 10–15 分钟样本。
4. 必须保持零漏投、零重复、零预算泄漏和资源正确释放；复杂度增加但收益不稳定时撤回实现并记录负结论。

## 后续但不在当前阶段

- 群通话/SFU：1:1 通话稳定后再设计独立媒体服务与容量模型。
- QUIC：只在真实移动网络问题无法由 TCP/WebRTC 解决时做可关闭对照；裸 UDP 不进入聊天控制面。
- Push/更深可观测性：按真实客户端需求和故障缺口逐项接入，不先建设空壳平台。

## 验证顺序

聚焦单测/契约测试 → Release 构建 → 3–5 分钟 smoke 或同构 A/B；必要时补一次 10–15 分钟样本。当前阶段到功能联调验收为止。
