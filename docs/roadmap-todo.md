# 待办路线图

本文件只保留尚未完成的工作。当前状态见 `roadmap-current-state.md`，已完成事项见
`roadmap-changelog.md`；跨仓统一顺序和验收口径见 `docs/NEXT-STAGE.md`。

## 当前执行批次与交接顺序

| 批次 | Owner / 可并行性 | 前置条件 | 必须交付的接手证据 |
| --- | --- | --- | --- |
| `PROTO-FEED-1` | Shared；代码与测试已收口，feed 发布外放 | 六包候选源码和版本冻结 | 不可变 nupkg/hash、locked restore、Gateway/Client 短联调报告 |
| `REL-GATE-1` | ✅ 已完成（2026-08-11）；Server + Realtime | Contracts `2.5.2`、Integration `3.1.3` 与 Migration 060–062 冻结 | `run-manifest.json`、reconcile report、故障矩阵；密钥不得入档 |
| `REL-WIRE-2` | Shared；必须等待 `REL-GATE-1` 通过 | `REL-GATE-1` 已通过（manifest、reconcile report、故障矩阵） | list/catch-up/reset schema、reserved 字段、old/new golden、包/hash |
| `REL-READ-3` | Realtime → Gateway → Client | `REL-WIRE-2` 的不可变包已发布 | 默认关闭的 capability、HTTP 权威对照、短时 canary/回滚报告 |

后续 Agent 只接一个批次，并先读取上一批次证据；没有证据时不得按“代码已存在”推定门禁通过。
`REL-READ-3` 完成前，关系 mutation 永久走 Server HTTP，TCP 入口继续 fail-closed。二进制生产协商、
语音/通话和 QUIC 不与关系只读首轮同时灰度。

## P0：共享契约候选发布

1. 在 Shared 补帧级超限/畸形输入 fuzz，以及旧附件、关系字段组合的 old/new fixture。
2. 在干净 CI 中重新打包六个 `0.4.1` 候选包并核对已记录 SHA-256；任何源码或包元数据变化都升新版本，禁止覆盖同版本。
3. 发布不可变 feed 后，让 Gateway 与 Client 只从 feed 做 locked restore，并运行一次短时 TCP 联调，覆盖 request-id 错配、JSON 降级、超限和截断。
4. 回滚只切回上一不可变包；不恢复本地重复 DTO，也不回滚已经成功的客户端 SQLite migration。

## P0：关系只读投影

当前 Server HTTP 和 public `T_*` 表是唯一关系权威。Server 已在关系事务内写 Realtime Outbox
连续版本 delta；Realtime 已在 JetStream ACK 前原子提交 inbox/item/version，并能通过权威 stream
快照回填、重复扫描和 count/hash 对账修复单流。Rebuilder 与 snapshot-gated list processor 均默认关闭，
TCP relation list/sync/mutation 继续 fail-closed，旧 Realtime 关系表不得恢复为在线权威。

1. ✅ **`REL-GATE-1` 已完成（2026-08-11，Linux 隔离环境 192.168.5.49）**：以 secret store 注入服务密钥启用
   Rebuilder，双实例共享同库抢租/过期接管零冲突，验证多实例交替持租、整页提交、失败续跑、429/5xx/超时退避
   与错误分类；取消重启、密钥轮换、并发 mutation 和扫描期间新增较小 owner id 已由 Rebuilder 编排测试兜底
   （`RelationshipProjectionRebuildWorkerTests`）。密钥、列表正文或响应体不写日志。
2. ✅ **`REL-GATE-1` reconcile 门禁通过（2026-08-11）**：受 Ops API key 保护的 status/streams 返回持久化
   cursor、稳定轮次、租约/最后错误，以及逐 stream current/snapshot version、数量/hash、快照后 inbox
   count/max version 和本地连续性。`Invoke-RelationshipProjectionReconcile.ps1` 从空 cursor 分页、校验每轮
   前后状态 token，连续两轮全量 SHA-256 指纹一致 `CF08320F…9CB3E9`，2 clean pass 全 27/27 匹配、0 差异、
   无 gap/503、全页 200，`gatePassed=true`。报告不含服务密钥、好友明细或响应正文。修复了 Server 导出端点
   camelCase 与 Realtime PascalCase 反序列化的契约 bug（新增 `ServerJsonOptions`）。
3. 只有连续两轮 stable 且两轮所有 reconcile 页面均为 200、总差异为零，故障注入恢复后再次全量对账
   仍一致，才独立开启 Realtime list canary；无 snapshot checkpoint 返回 unavailable，分页 version 变化要求
   从第一页重启。canary 期间继续抽样与 Server HTTP 权威列表逐项对照，任一失败立即关闭读取开关。
4. Shared/Gateway/Client 再统一 list/catch-up/reset wire，固定 version/watermark、partial/reset、
   unavailable/version-changed/gap 和游标失效语义；Sync 超限不得静默丢段，mutation 永久走 Server HTTP。

## P1：TCP 长连接 CPU/内存

已完成 executor 惰性队列、发送泵唯一清理、LazySegmented writer admission、接收缓冲安全降级和
90 秒 lease/presence 刷新。下一步不再凭对象估算继续改代码，先做 10–15 分钟可归因画像：

1. 分别运行 10k authenticated 静默、heartbeat-only、1% active + 1% slow-reader；固定源码、
   Release 二进制、连接 ramp、配置和运行目录，每个候选至少三轮短样本。
2. 同时采集 gcdump 类型计数、PSS/smaps、fd、`ss -tinm`、cgroup sock、GC/分配、线程、句柄、
   heartbeat counter 与 p95/p99；区分 managed retained、GC committed、ArrayPool、native cache 和内核 socket。
3. 只有新热点能带来至少约 15% PSS/retained 改善，或单个已有开关能稳定节省至少约 0.5 KiB/连接，
   才继续 linked CTS、send waiter、deadline callback 或 socket buffer 优化。
4. 验收必须保持零漏帧、零重复、零预算/retained 泄漏，吞吐和 p95/p99 不回退超过 5%，并覆盖
   connection-id 复用、Stop/Dispose、慢读者公平、resume/fencing 和 10k shutdown。

## P1：消息链路与数据库性能

1. 用 trace、`pg_stat_statements`、WAL/消息、DB ops/消息和 allocation/消息先确认 Top 路径；
   每轮只改一个 SQL、批处理、索引或调度因素，以同构重复 A/B 中位数判断。
2. Outbox hint 合并默认保持 `0 ms`。`2 ms` 只作为显式资源优先模式；未证明尾延迟风险可接受前，
   不改变默认值。
3. 短时 80/320/640 msg/s admission/capacity 必须验证 ACK/跨 Gateway 投递、重复/漏投、
   Outbox/JetStream/死信、CPU、GC、SQL、WAL 和 p95/p99；30 分钟仅冻结候选，8 小时仅用于发布门禁。
4. Linux 仍需补慢速 header/payload、全局入站预算、连接风暴、依赖断开/恢复和跨 Gateway 重连；
   每轮使用独立目录、manifest、源码/二进制 hash，禁止从历史运行拼接结论。

## P1：Push 与附件闭环

### Push

1. 实现真实 FCM/APNs/WebPush provider、凭据轮换和 provider 级限流；Gateway 继续只负责路由，
   网络资源由 PushWorker 隔离。
2. Realtime publisher 在可靠判断 `UserOffline` 后入队；区分用户离线和目录查询失败，后者必须重试，
   不能误判为离线。
3. Client 完成 token 注册、轮换、撤销、退出登录和通知偏好；覆盖多设备、无效 token、部分成功、
   DLQ 重放和幂等。

### 附件

1. 补所有权校验、扫描/审核触发、过期 sweep、下载授权和保留策略；Gateway 不签发下载 token，
   只转发稳定 wire 状态。
2. 用新 migration 对齐数据库状态约束与 wire/domain 枚举，保留显式 mapper，禁止跨层强制转换未知值。
3. 覆盖重复 finalize、消息绑定竞态、扫描失败、过期、下载越权和恢复；语音消息复用同一附件生命周期，
   音频内容不进入 TCP frame、Postgres Outbox 或 JetStream。

## P1：二进制 payload 接入

Shared 已废弃从未上线的实验格式，并完成默认不可协商的首个候选 `chatapp-bin-v1` 底座；生产 JSON 和 10-byte 帧头不变。

wire、公共 Core、无生成器单遍 encoder、decode-only generator、原生指针边界和分配口径统一见
[`ChatApp.Shared/docs/BINARY-PROTOCOL.md`](https://github.com/Ye-Luo-0912/ChatApp.Shared/blob/main/docs/BINARY-PROTOCOL.md)。
本节只列 Gateway 接入任务，不在本仓重新定义字段编码，也不恢复旧 reader/writer 或兼容 facade。

1. 执行 Shared `BIN-SCHEMA-2`：全部握手后 payload 命令具备手写单遍 encoder、生成 decoder、reserved
   manifest、当前格式 golden 与 fuzz；不能只给 ChatMessage 开 binary，因为当前帧头没有逐帧格式位。
2. `ClientHello/ServerHello` 始终 JSON；完整握手后把格式固化到 session，首版 Resume 强制 JSON。
   入站不 sniff，连接中途不切换；回滚关闭选择，让已协商连接排空或 GoAway 重连。
3. 混合灰度按格式分组，每个事件每种格式最多编码一次并复用共享帧；不得退化为逐 session 序列化。
   鉴权、resume 等敏感 buffer 在成功、异常、扩容和引用归零路径都由所有者清零。
4. 执行 Shared `BIN-INTEGRATION-3` 门禁：encoder/borrowed decode 分配口径、真实 payload 体积、CPU/message、
   allocation/message 和 ACK/delivery p99 均达标，且 80/320/640 msg/s 短测零漏零重，才进入 canary；
   否则继续 JSON 默认。

## P2：语音、媒体与 QUIC

1. 语音消息复用附件上传；1:1 通话使用 WebRTC/SRTP + ICE/STUN/TURN，TCP 只承载可靠信令。
   裸 UDP 不进入聊天、鉴权、同步、ACK 或媒体业务层。
2. 群通话在 1:1 稳定后使用独立 SFU；TURN/SFU 的 CPU、带宽和成本进入独立容量模型，
   不能把“媒体未进入 Gateway”写成当前 TCP 资源下降。
3. QUIC stream 仅在共享契约和 binary 灰度稳定后做可关闭 A/B，覆盖 UDP 阻断、0/1/3/5% 丢包、
   20/80/200 ms RTT、网络切换和 NAT rebinding；TCP 始终保留回退。

## 工程治理

1. 注册 Linux 自托管 runner，接入 locked restore、Release build、完整测试、数据库契约、真实依赖探针、
   定时短门禁和发布 soak；保存报告并与冻结基线比较。
2. 配置 OTLP Collector 与 Alertmanager 实际通知通道，校准告警阈值；日志只保留结构化故障信息，
   不在热路径恢复高频 Information，也不记录 token 或消息正文。
3. 继续拆分 oversized 类型并补确定性竞态测试；当前 Windows 默认并行全套仍有 DirectSocket/Persistent
   loopback 握手 abort 的测试隔离债，串行全量与隔离重复通过，但 CI 应消除该时序噪声。

## 验证顺序

聚焦单测/契约测试 → Release 构建 → 5–20 分钟短时 A/B → 必要时 30 分钟冻结候选 →
仅发布候选执行 8 小时或更长 soak。任何正确性、兼容性、资源所有权或 p95/p99 门禁失败都先回退候选，
不靠延长测试时间掩盖问题。
