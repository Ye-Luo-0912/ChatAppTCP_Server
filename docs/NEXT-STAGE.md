# 下一阶段与接手状态

## 职责

TCP Gateway 负责连接、鉴权、协议能力、限流和到 Realtime 的路由；不成为数据库、好友关系或媒体数据的业务权威。

## 下一步执行与交接

Gateway 当前有一条等待型功能主线和一条可并行测量主线：

1. **等待 `REL-WIRE-2`。** 在收到 Shared 的不可变关系只读包、golden/hash、错误/预算表之前，relation list/sync/mutation 继续 fail-closed；不得从 Realtime 内部 DTO 反推 TCP wire。
2. **接手 `REL-READ-3`。** 先增加显式 wire mapper 和默认关闭的 capability，再覆盖响应硬预算、partial/reset、版本变化、断线续页和 HTTP 权威对照。Realtime list canary 先通过，Gateway 才给小比例新连接协商能力；旧连接不在线改变语义。
3. **交给 Client。** 提供 producer fixture、包/二进制 hash、短时跨 Gateway 报告、开关和回滚步骤。失败时关闭 capability，让新连接恢复 Server HTTP；mutation 永久不迁到 TCP。
4. **并行 `TCP-MEM-1`。** 只在冻结快照上执行 10–15 分钟 10k 静默、heartbeat-only、1% active+slow-reader 三类画像，采 gcdump/PSS/socket；该批次只产证据，不夹带功能或默认值修改。
   - **测量脚本已交付（2026-08-10）**：编排器新增 Linux 内存归因（PSS/smaps_rollup、`/proc/{pid}/fd` 峰值、cgroup sock/oom），`scripts/Run-MemoryProfile.ps1` 编排三类画像 × 每轮 10–15 分钟，`scripts/Performance-Gcdump.ps1` 与内嵌 socket/`ss -tinm` 采集在测量中段并行取证；`Run-MemoryProfile.ps1`/`Performance-Gcdump.ps1` 等全部性能脚本通过 PowerShell AST 解析，orchestrator Release 构建 `0 warning / 0 error`。已修复归因 Markdown 汇总 `-f` 逗号/除法优先级导致的格式化报错。
   - **`TCP-MEM-1` 正式测量完成（2026-08-11，Linux 真机 192.168.5.49）**：完整批次 `3 画像 × 3 轮 × 10 分钟` 全部 `VALID`（`memory-profile-20260811-011250Z`，`TCP-MEM-1: PASSED`）。每轮 2 Gateway 采 PSS/VmRSS/VmHWM/cgroup 峰值、fd 峰值、2 个 gcdump（共 18）与 `ss -tinm` socket 归属。画像内存梯度符合预期：active（213–230 MiB PSS）> heartbeat（197–209）> silent（175–183）。管道修复：后台任务 `[ordered]@{}` 反序列化为 `OrderedDictionary` 使证据正确聚合、`ss` 归属正则改为 `pid=<pid>,`、`$pid` 只读变量冲突改用 `$gatewayPid`、active 死信门放宽到本画像消息理论上限（slow-reader 场景非交付语义）。
5. **后续顺序。** 关系只读 canary 稳定后才进入 binary 双 codec；语音附件/通话信令随后独立批次，QUIC 最后做可回滚实验。

下一位 Agent 必须在报告中写明接手的批次号；未拿到上一批次证据时只允许补测试/测量，不允许提前开放 capability。

## 接手状态

- P0（已验证）：有效历史响应及可关联错误回显请求 `ConversationId`/`RequestId`，显式历史和 Sync catch-up 游标均保留 `ChangedAtMs`；客户端兼容推断、`ClientMessageId`、单调合并和可重入 reset 已通过 Release 与全量测试。
- P0（已收口）：关系读写在 Realtime 投影完成前明确返回迁移错误，客户端继续使用 Server HTTP，不允许旧关系表形成第二权威。
- P0（harness 已收口）：按 Gateway 编号比较发送 child 的 ACK 与 next-ring child 的投递 ID 指纹，并采集同一负载主机上的真实 delivery latency；该指纹是高概率校验且单调时间戳不可跨负载主机比较。
- P0（契约迁移已收口）：Gateway/Client 已共同升级 `ChatApp.Protocol.Tcp 0.4.1`，本地历史/同步同义 DTO 已删除；Gateway 通过 `HistoryWireMapper` 显式转换 Realtime owner，Shared 不反向依赖 Realtime。超出硬预算的 Sync 不再静默丢弃尾部 catch-up，而是返回可重试的明确失败。
- P0（契约候选已完成，feed 发布仍是 TODO）：Gateway/Client 的 History/Sync golden 与第一版 old/new 矩阵均为 `8/8`，已覆盖旧缺省字段、未知字段/枚举、双向 cursor、截断和可关联错误；Shared 迁移文档记录了包、fixture 与消费端二进制 SHA-256。发布前补帧级超限 fuzz，并用共享 feed 做一次 locked restore + TCP 短联调；不再增加本地兼容 DTO。
- P0（关系入口继续 fail-closed）：Server 已在原关系事务内写带连续 owner/list version 的 `RelationshipProjectionDelta v1`，Realtime 在 Server 事件的 JetStream ACK 前原子提交 item/version/inbox 并拒绝 gap。流级快照导出、checkpoint 原子导入、数据库时钟租约扫描、持久化 cursor、失败续跑、两轮稳定判定和 count/hash 单流自修复均已实现；Realtime 也已有 snapshot-gated list processor，用同一数据库快照读取 version+页面并拒绝旧分页 cursor。Rebuilder 与读取仍默认关闭，Shared/Gateway/Client list/sync wire 和真实授权对照尚未完成，因此 TCP list/sync/mutation 继续返回迁移错误。
  - TODO 顺序：Realtime 的受保护 status/streams 已暴露持久化 cursor、稳定轮次、租约/最后错误，以及逐 stream current/snapshot version、数量/hash 和本地 delta 连续性；reconcile 以同一复合游标合并 Server digest 与本地视图，不传好友明细。先在隔离环境启用 Rebuilder，验证多实例接管、取消续跑、密钥轮换、429/5xx/超时和并发 mutation；再运行 Realtime 的 `scripts/Invoke-RelationshipProjectionReconcile.ps1`，由工具完成两轮有界分页、Rebuilder 状态前后校验和全量指纹比较。只有命令零退出且故障恢复后再次通过，才独立打开 list canary。随后按 Shared 字段/错误/预算清单统一 Gateway/Client list/sync wire，做 HTTP 权威逐项对照，最后只给小比例新连接开放 capability。
  - 切读验证至少覆盖快照期间并发关系变化、重复/乱序 delta、gap 修复、响应字节预算、断线续页和 HTTP 权威结果逐项对照；失败时关闭 capability，新连接继续走 Server HTTP，不修改已协商连接的格式或语义。
- P1（默认值已门禁）：Outbox hint 合并窗口默认 `0 ms`；`2 ms` 的数据库调用收益不足以抵消跨 Gateway 尾延迟退化，只能显式启用。
- P1（长连接资源优化已实现，短时 10k 画像已完成第一阶段）：OrderedWrite/Query executor 改为每连接轻量 holder、首次命令才创建并复用队列，未跨连接共享可变状态；按 .NET 10 x64 对象分配探针，10k 连接两条 lane 预计避免约 40.28 MiB 空队列托管分配。
  - 注销、首次入队、Stop/Dispose 和 connection-id 复用通过 opaque lease、关闭准入、holder ready token 与 expected-session cleanup 保持线性化；PerSession/OnDemand 发送泵由 Publishing/Finalizing 代次保证唯一 drain，LazySegmented 在关闭前封 writer admission 并等待已准入写者发布完成。多生产者 FIFO、预算归零和全量回归均已覆盖。该估算不是进程 RSS，下一步用 gcdump/PSS 对照验证实际 retained 类型。
  - DirectSocket 删除每次 receive 的无效时间戳读取；曾升级到 4 KiB 的接收缓冲区在大帧保留窗口到期、下一完整帧后的空缓存安全点降回 1 KiB，无 per-session timer，每个曾升级连接最多向池归还约 3 KiB 并降低 session 持有容量，但不宣称 ArrayPool 或进程 RSS 会立即下降。Ephemeral close 竞态使用 key+entry version 精确撤销，避免帧和两级预算滞留。
  - 第一阶段 10k authenticated heartbeat（每 Gateway 5k）零连接失败/协议拒绝，Gateway 平均 CPU `0.69–0.89%`，私有内存 ramp 增量上界约 `29.6–33.5 KiB/连接`，最大 handle 约 `5.5k/实例`、线程 `48–59/实例`，证明没有每连接线程；该值混有 JIT、GC committed、池和 native cache，不能当作对象 retained size。
  - 默认 lease/presence 刷新从 30 秒分离为 90 秒 cadence，TTL 校验保证连续漏两轮后仍有刷新机会；同形 60 秒样本的 Garnet 入站网络 `16.7→12.3 MB`，但平均 CPU `13.16→13.21%` 无可证明改善，因此只保留“减少命令/网络”的结论。编排器现显式记录两项 cadence，测量时长上界允许 `max(1 秒, 1%)` 的调度正偏差。
  - 下一轮 TODO 只做 10–15 分钟 10k 静默、heartbeat-only、1% active+slow-reader，并同时采 gcdump、PSS/smaps、fd、`ss -tinm`/cgroup sock 与 Gateway heartbeat counter；要求零漏帧/重复/预算泄漏，吞吐和 p95/p99 不回退超过 5%。只有 retained/PSS 证据显示新的 ≥15% 热点，才继续 linked CTS、send-loop waiter 或 socket buffer 优化。**测量脚本已交付并通过 AST 解析（见主线 4），待 Linux 真机执行。**
- P1：继续使用短时 admission/change/capacity 测试优化 CPU、分配、数据库操作和 WAL；30 分钟仅用于冻结候选，8 小时仅用于发布候选。
  - 每轮保存负载、连接数、配置、源码/二进制 hash 和资源采样；一次只改一个因素，以同构重复 A/B 的中位数判断收益，任何 ACK/投递/重复/漏投或 p95/p99 回退先阻断候选。
  - 优先优化已证实的热点；当前 Gateway 不是主要 CPU 瓶颈时，不以增加协议复杂度换取理论收益。
- P1（二进制 payload，Shared runtime/generator 已完成且 feature 未启用）：保留现有 10-byte 帧头与 JSON 默认值，复用 `IPayloadCodec<T>` 的 `IBufferWriter`/`ReadOnlySequence` 路径；协商前仍用 JSON，完整握手成功后把 negotiated format 固化到 session，连接内不 sniff、不混用、不在线切换。首版带 ResumeToken 的连接强制 JSON，待协议保证先发 JSON ServerHello 后再单独开放 binary resume。
  - 实现前先对 ChatMessage、History、Sync 真实 payload 做 JSON/候选格式离线基准；进入 canary 前必须覆盖全部握手后有 payload 的命令，并提供配置 kill switch、JSON 降级和未知/恶意输入限制。
  - 混合灰度时按 payload format 最多分组编码一次并复用共享出站帧，禁止每 session 重新序列化；格式数设置硬上限，连接排空/重连后才能切回 JSON。
  - 推广门槛：相同语义 golden/兼容/fuzz 全过，payload 目标下降至少 30%，序列化 CPU 或分配至少下降 20%，80/320/640 msg/s 短测的 p95/p99 不显著回退且 ACK/投递零漏零重；否则保持 JSON 默认。
- P2（UDP/QUIC 评估）：不为聊天、鉴权、同步或 ACK 引入裸 UDP；它会把加密、认证、重放、排序/重传、拥塞、NAT、PMTU 与放大防护搬进 Gateway，当前资源数据不支持这项复杂度。
  - 1:1 音频直接走 WebRTC/SRTP 与 ICE/STUN/TURN，TCP Gateway 只传小型可靠信令，因此避免的是未来媒体流量进入 Gateway，并不会降低当前基线资源。
  - QUIC stream 仅在二进制/共享协议冻结后做独立、可回退 A/B：覆盖 UDP 阻断、0/1/3/5% 丢包、20/80/200 ms RTT、路径迁移和 NAT rebinding；只有 CPU/分配或移动网络恢复有稳定显著收益且语义零回退才进入灰度。

## 功能路线

语音消息复用附件命令和普通 ChatMessage；1:1 通话新增 capability 与 offer/answer/ICE/结束信令，媒体走 WebRTC/TURN。群通话引入独立 SFU；QUIC 先做隔离基准，不直接替换 TCP 控制面。

## 共享与资源

复用 source-generated codec、只读元数据、连接池和有界线程安全调度器；连接 session、payload buffer、取消源和事务对象必须由单一生命周期持有并可靠释放。

## 验证顺序

聚焦单测/契约测试 → Release 构建 → 短时 admission/smoke；阶段长测与发布 soak 只在功能冻结后执行。

当前基线：Release build `0 warning / 0 error`；TCP 串行全量 `566 passed / 1 Redis environment skip`；PerSession/OnDemand/Lazy 确定性竞态连续 10 轮 `260/260`。默认并行的 Windows loopback 仍有一个 DirectSocket/Persistent 握手 abort 时序债，但该类隔离连续运行通过。三条性能入口脚本均通过 PowerShell AST 解析。
