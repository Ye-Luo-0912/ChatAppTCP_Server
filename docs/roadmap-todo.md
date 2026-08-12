# 待办路线图

本文件是 TCP Gateway **唯一的详细执行清单**。当前事实见 `roadmap-current-state.md`，已完成事项见 `roadmap-changelog.md`，跨仓顺序摘要见 `NEXT-STAGE.md`。

修改前先读相关实现、调用方、上下游契约和测试，确认命令语义与资源所有权。每个批次只解决一个问题，不复制 Shared DTO，不引入无界队列、连接级后台任务或隐式格式切换。

## 当前批次

| 批次 | Owner / 可并行性 | 前置条件 | 完成证据 |
| --- | --- | --- | --- |
| `TCP-P0-2` | Gateway；立即执行 | 无 | 根因、聚焦回归、Release 全量测试、短时 smoke |
| `REL-READ-3` | Realtime → Gateway → Client；可与 P0 部分并行 | Shared 关系契约已完成；Gateway 接入等待 `TCP-P0-2` | 投影 list/catch-up、显式 mapper、本地投影、HTTP 对照、关闭开关 |
| `BIN-SCHEMA-2` | Shared；可与 P0 并行 | `chatapp-bin-v1` Core 已完成 | 全量真实 schema、encoder/decoder、golden、fuzz、真实 corpus 基准 |
| `TCP-PERF-2` | Gateway；P0 后执行 | 读取既有 `TCP-MEM-1` 证据 | 单热点归因、微基准、短时 A/B、资源与正确性报告 |
| `BIN-INTEGRATION-3` | Gateway + Client | `BIN-SCHEMA-2`、`TCP-P0-2` 完成 | 双 codec、session 协商、降级开关、短时跨 Gateway 报告 |
| `VOICE-SIGNAL-1` | Shared → Realtime → Gateway → Client | `TCP-P0-2` 完成 | 信令契约、状态机、鉴权/限流、跨 Gateway 与故障测试 |

后续 Agent 一次只接一个批次，并在交接中写明修改范围、未解决风险和验证结果。关系、二进制与语音 capability 分开接入、分开关闭。

## P0：`TCP-P0-2` 正确性收口

1. 为 Windows 默认并行全套中的 DirectSocket/Persistent 握手 abort 建立稳定复现，记录客户端阶段、服务端 session 状态、socket 关闭方向、异常类型和预算计数；先确定是测试隔离、端口复用还是生命周期竞态。
2. 按真实资源所有权修复根因。禁止仅增加等待时间、吞掉异常或串行化整个测试集；关闭路径必须停止新准入，等待已准入 writer 发布完成，再释放 socket、session、lease 和全局预算。
3. 增加确定性竞态测试，覆盖握手中断、鉴权与关闭并发、connection-id 复用、Persistent send loop 退出、重复 Dispose、服务停止和 10k shutdown。断言无悬挂任务、无双重清理、预算归零。
4. 复核所有可关联响应和错误保持 `ConversationId` / `RequestId`；History/Sync 超过硬预算时返回明确可重试失败，不允许静默丢段或发送半帧。
5. 复核 Resume claim/commit/abort、旧 session fencing 和回滚顺序；依赖不可用时不得把未完成会话提升为 authenticated。
6. 验证顺序为聚焦测试连续重复 → Release 全量测试 → 3–5 分钟连接/鉴权/关闭 smoke。任何偶现失败都保留诊断证据，不以重试掩盖。

## P0：关系只读 `REL-READ-3`

Server HTTP 与 public `T_*` 表继续作为唯一关系权威；TCP relation list/sync/mutation 在本批次完成前保持 fail-closed，旧 Realtime 关系表不得恢复为在线权威。

1. 以 Shared 已有 list/catch-up/reset DTO、字段预算、reserved 字段和错误语义为唯一 wire 输入；只有真实消费测试复现契约缺口时才回到 Shared 修正，不在 Gateway 创建本地同义 DTO。
2. Realtime 完成 snapshot-gated list 与投影 change history/catch-up 同源读取：无 checkpoint 返回 unavailable；分页期间 version 变化要求重启；重复/乱序 delta、gap 修复和故障恢复后结果仍与 Server HTTP 一致。
3. Gateway 增加 Realtime owner → TCP wire 的显式 mapper 和默认关闭 capability，不得从 Realtime 内部 DTO 或数据库形状推导 wire。
4. 覆盖响应字节预算、断线续页、重复请求、旧 cursor、权限变化和 HTTP 权威逐项对照。任一差异关闭 capability，读取继续 fail-closed，已协商连接不在线改变语义。
5. Client 可先用 fixture 完成本地投影与恢复，真实链路完成后再接 list/sync；mutation 永久走 Server HTTP。多设备合并、reset 可重入和离线恢复必须使用同一组 Shared 语义。

## P1：`TCP-PERF-2` 短测驱动的 CPU/内存优化

既有 `TCP-MEM-1` 已覆盖 10k 静默、heartbeat-only、active/slow-reader，并保存 gcdump、PSS、fd 与 socket 归因；不要重复整批测量。

1. 先读取现有证据，按 retained bytes、allocation/message、CPU sample 或 kernel socket 占用选出一个 Top 热点；若证据不足，只补针对该热点的采样。
2. 建立可重复的微基准或聚焦计数测试，再一次只修改一个因素。优先审查 per-session CTS/waiter、send-loop 状态、deadline callback、缓冲区保留和连接级集合；不得为了理论收益增加协议复杂度。
3. 对原实现与候选执行至少两轮交错的 3–5 分钟同构 A/B，固定连接 ramp、负载、配置和依赖。已胜出的候选最多增加一次 10–15 分钟 Linux 样本确认 PSS/retained 趋势。
4. 同时记录吞吐、ACK/跨 Gateway delivery、p95/p99、CPU、GC、allocation、PSS、fd、socket 和所有预算；必须零漏帧、零重复、零预算泄漏，p95/p99 不回退超过 5%。
5. 只有归因证据显示约 15% 以上热点改善，或单个开关稳定节省约 0.5 KiB/连接，才保留实现；否则撤回复杂度并记录负结果。

## P1：消息链路与数据库短测

1. 用 trace、`pg_stat_statements`、WAL/message、DB ops/message 和 allocation/message 确认 Top SQL 或调度路径；每轮只改变一个 SQL、索引、批处理或调度因素。
2. Outbox hint 合并默认保持 `0 ms`；`2 ms` 仅作为显式资源优先模式，未证明尾延迟风险可接受前不改变默认值。
3. 以 80/320/640 msg/s 每档 2–3 分钟做 admission/capacity 快筛，验证逐消息 ACK、跨 Gateway 投递、重复/漏投、Outbox/JetStream/死信、CPU、GC、SQL、WAL 与 p95/p99；较低档失败立即停止升档。
4. 获胜候选只补一次 10–15 分钟同构样本。每轮使用独立目录并记录配置、commit、负载和资源采样，禁止拼接历史运行得出结论。
5. 补慢速 header/payload、全局入站预算耗尽、连接风暴、依赖断开/恢复和跨 Gateway 重连的短时故障测试。

## P1：Shared 真实 DTO `BIN-SCHEMA-2`

唯一 wire 规范由 `ChatApp.Shared/docs/BINARY-PROTOCOL.md` 维护。Gateway 不重新定义字段编码、公共原语、reader/writer 或兼容 facade。

1. 盘点所有握手后带 payload 的命令，至少覆盖 ChatMessage、ACK、History、Sync、Receipt、Edit/Revoke、Presence、Typing、Group、Attachment 和错误响应；当前帧头没有逐帧格式位，因此不能只迁移部分命令。
2. 为真实 DTO 固定字段号、wire type、required/optional、默认值、最大长度、集合上限、reserved 范围与未知字段行为。嵌套与集合规则先在 Shared 固定后再生成代码。
3. encoder 保持普通源码单遍写入连续 `Span<byte>`；decoder 使用生成的连续/分段实现。运行时不使用反射、动态注册、旧两遍 measured adapter 或逐字段中间对象。
4. 增加同语义 golden、截断、非 canonical varint、非法 UTF-8、重复/乱序字段、未知字段、深度/长度预算和 segmented input fuzz。失败必须 fail-closed，borrowed view 不得逃逸 owner 生命周期。
5. 用真实消息 corpus 对 JSON 与二进制比较 payload bytes、encode/decode ns、allocation/message 和必要 DTO 所有权分配；单字段微探针不能替代真实结果。

## P1：Gateway 双 codec `BIN-INTEGRATION-3`

1. `ClientHello` / `ServerHello` 与协商完成前的数据始终使用 JSON；完整握手后把 payload format 固化到 session。首版 Resume 强制 JSON，入站不 sniff，连接中途不切换格式。
2. Gateway 从 frame owner 获取连续可提交 `Span<byte>` 调用 Shared encoder；入站按连续或分段 buffer 调用生成 decoder。成功、异常、扩容和引用归零路径都必须正确释放 owner，敏感 buffer 必须清零。
3. 混合连接按 format 分组，每个事件每种格式最多编码一次并复用共享帧；格式数量设硬上限，禁止逐 session 重复序列化。
4. capability 默认关闭并提供 JSON 降级开关。关闭选择后由旧连接自然排空或明确重连，不在线改变已有 session 的格式。
5. 在真实 corpus 达到 payload 至少下降 30%，序列化 CPU 或分配至少下降 20%，且 80/320/640 msg/s 短测保持 ACK/delivery 零漏零重、p95/p99 无显著回退后，才允许小范围接入；否则保持 JSON 默认。

## P2：`VOICE-SIGNAL-1` 语音通话信令

1. Shared 定义版本化 CallInvite/Offer/Answer/IceCandidate/Hangup/Reject 信令，字段至少包含 `CallId`、`RequestId`、参与者、序号、过期时间和 capability；为 SDP/ICE 数量、单项长度和整帧大小设置硬预算。
2. Gateway 只允许 authenticated session 发送信令，复用关系/拉黑授权，增加用户级与连接级速率限制、并发通话上限、过期拒绝、幂等和重放保护。
3. Realtime 负责跨 Gateway 在线路由和短 TTL 状态，不把音频、视频或连续媒体包写入 PostgreSQL、Outbox 或聊天 JetStream；离线与依赖失败返回明确状态。
4. Client 实现显式状态机，覆盖重复/乱序信令、双方同时发起、拒绝、超时、断线、网络切换、多设备竞争和旧 capability 客户端。
5. 语音消息复用附件 ownership、finalize、扫描和下载授权；TCP frame 只传附件引用。1:1 通话媒体使用 WebRTC/SRTP 与 ICE/STUN/TURN，裸 UDP 不进入聊天、鉴权、同步或 ACK。
6. 先通过契约与状态机测试，再做 3–5 分钟跨 Gateway 信令 smoke。群通话 SFU 与 QUIC transport 留作后续独立评估，本阶段不执行。

## 可维护性与验证

1. 只在本批次触达时拆分 oversized 类型；新 handler、mapper 和状态机保持单一职责，并为资源关闭和竞态补确定性测试。
2. 日志只记录结构化故障信息，不记录 token、SDP、ICE 明文、消息正文或附件内容；新增指标保持低基数。
3. 通用验证顺序：聚焦单测/契约测试 → Release 构建 → 3–5 分钟 smoke 或同构 A/B；只有已胜出的性能改动需要补充归因时，才增加一次 10–15 分钟 Linux 样本。
4. 任一正确性、资源所有权、ACK/delivery 或 p95/p99 门禁失败都先修复或撤回候选，不靠延长测试掩盖问题。
