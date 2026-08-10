# R3 TCP + Realtime 8 小时浸泡复跑记录

> 本文是按轮次追加的审计记录；早期“下一步”不代表当前计划。当前代码/配置结论见第 18–19 节、
> [`perf-optimization-rerun-20260807.md`](perf-optimization-rerun-20260807.md) 第 19–20 节和
> [`NEXT-STAGE`](NEXT-STAGE.md)。

> 新轮启动时间：2026-08-06 02:16:49（UTC+08:00）  
> 当前状态：**FORMAL 8H PASSED**；00:30 首轮已主动中止，绝不计入本 verdict  
> 新轮运行目录：`/home/yeluo/chatapp-perf/runs/codex-tcp-soak-20260805T180247Z`  
> launcher PID / PGID：`1457261 / 1457261`；完成时间：2026-08-06 10:22:36（UTC+08:00）

## 1. 为什么旧报告不能作为 8 小时结论

2026-08-04 的运行虽然持续了约 8 小时，但认证连接大量失败，负载又把消息发给自己，
产生 `invalid_self_chat` 死信；因此业务链路并未按目标执行，内存结论也不具备可比性。

原计划的 `10,000 × 5 msg/s × 8h` 同样不是有效的单机稳态模型：默认业务限流是每用户
30 秒 30 条，5 msg/s 会快速进入 `rate_limited`；50,000 msg/s 持续 8 小时还会产生
14.4 亿条 durable 消息，仅 512 B 正文约 737 GB，尚未包含 JSON、索引、WAL 和 Outbox。

本次把“认证长连接规模”和“durable 写入速率”分开：10,000 个连接全部认证并接收，
其中 100 个连接以每连接 0.8 msg/s 发送，总目标 80 msg/s；其余连接只做 30 秒一次的
heartbeat keepalive，heartbeat 不进入 durable 消息、ACK、投递或吞吐计数。

## 2. 本轮修复和有效性门禁

- 每个连接使用唯一用户和令牌，chat 使用确定性非 self peer ring。
- 连接爬坡、稳定期、正式测量和 delivery drain 分开计时。
- `--active-senders` 与小数 `--messages-per-second` 将连接规模和写入量解耦。
- 正式窗口停止发送后继续接收，直至全部已发送消息同时得到 MQ ACK 和 peer delivery，
  或达到有界 drain 超时。
- 非主动 chat 连接在稳定期、测量期和 drain 期错峰发送 heartbeat，避免约 90 秒 idle timeout。
- 外层 99.9% ACK/投递门禁下推给每个负载子进程；连接关闭、拒绝、跟踪溢出或 TTL
  过期会立即终止，不再等到 8 小时结束后才判无效。
- 每条消息用 `ClientMessageId` 关联 MQ ACK 和全部预期可读设备投递；重复、错配或漏投
  均单独计数并立即使本轮无效，delivery 先于 ACK 的正常乱序仍能正确闭环。
- slow consumer 使用同用户“健康可读设备 + 慢读设备”模型：慢设备会收到真实 chat fan-out，
  健康设备仍提供可验证的投递观测；`SlowReaders=0` 的正式模型保持一连接一身份。
- 依赖启动前验证 NATS/JetStream、Garnet PING/读写/Lua 与 PostgreSQL；Linux 文件句柄门禁
  要求覆盖全部连接和安全余量。
- 容量/soak 的资源采样覆盖率只统计协调 measurement 窗口，并按每条 process/container
  series 取最小值；不再把 ramp、warmup 或 drain 样本混入分母而产生超过 100% 的结果。
- 临时依赖容器改为 `create -> 登记本轮标签 -> start`，finally 只按精确名称和本轮标签
  删除并携带 `-v`；端口冲突造成的半创建容器也不会遗留。
- 正式运行强制绑定隔离运行目录、源码归档、规范包源归档和实际 dotnet 文件；编排器会
  自行重算 SHA-256，并让 build 与全部子进程使用同一个已验证的绝对 dotnet 路径。
- Realtime 并发、队列、JetStream prefetch 和 max-ack-pending 被显式配置并写入报告。
- 单聊分区键由低位分布不佳的简单异或改为稳定 64-bit avalanche hash；实际 1,000 用户
  peer ring 在 4 分区上的分布由 `[0,500,0,500]` 改为 `[262,236,240,262]`。
- JetStream 配置 `BackOff` 时以首值作为实际 ACK timeout，并按实际 timeout 的一半发送
  progress ACK；这与 [NATS JetStream consumer 文档](https://docs.nats.io/nats-concepts/jetstream/consumers)
  中“BackOff 覆盖 AckWait”的语义一致。

## 3. 正式运行前 canary

| 场景 | 结果 | 连接 | durable sent / ACK / delivered | 目标达成 | ACK p95 / p99 | delivery p95 / p99 | 测量采样 / 尾部积压 |
|---|---|---:|---:|---:|---:|---:|---:|
| 新冻结版：1,000 sender × 0.8 msg/s，60s | PASS | 1,000/1,000 | 47,985 / 47,985 / 47,985 | 99.929% | ≤0.928 / ≤2.816 ms | 65.536 / 180.224 ms | 100% / JS 0 / Outbox 0 |
| 新冻结版：10,000 连接、100 sender × 0.8 msg/s，120s | PASS | 10,000/10,000 | 9,599 / 9,599 / 9,599 | 99.975% | ≤1.344 / ≤1.664 ms | 32.768 / ≤45.056 ms | 98.333% / JS 0 / Outbox 0 |

两轮均逐 child 验证 `Sent == Acknowledged`、`ExpectedDeliveries == Received`、ACK/投递
延迟样本数相等、drain 完成，并且 duplicate、rejected、expired、dropped、outstanding、
runtime failure 和 dead letter 全为 0。正式 8 小时运行才负责给出内存平台结论。

## 4. 正式 8 小时配置

| 项 | 值 |
|---|---:|
| Measurement / warmup | 28,800s / 300s |
| TCP connections / active senders | 10,000 / 100 |
| Durable rate / payload | 80 msg/s / 512 B |
| Inactive heartbeat / delivery drain | 30s / 60s |
| Realtime processing concurrency | 16 |
| Queue / prefetch / max ACK pending | 512 / 64 / 256 |
| Connection ramp | 500/s |
| ACK / delivery minimum | 99.9% / 99.9% |
| Dead letters | 0 |
| Memory growth / final slope maximum | 20% / 30 MiB/h |
| Transport | DirectSocket + PersistentSendLoop + BoundedChannel |

首轮 launcher PID 为 `1438931`，日志位于
`reports/soak-8h-formal/soak-run.log`。代码复审随后发现旧 load generator 只用全局
ACK/delivery 总数完成 drain：重复终态可能掩盖另一条消息缺失，不能严格证明“同一条消息
同时收到 ACK 和 peer delivery”；稳定期 runtime failure 也未绑定生命周期取消，可能直到
8 小时结束才失败。因此该轮在约 16 分钟时主动停止，保留日志但不生成、不接受正式 verdict。
修复要求是逐 message 双状态、重复终态门禁、稳定期有界 heartbeat 与跨 child fail-fast；
聚焦测试和 canary 通过后才会重新计时 8 小时。

2026-08-06 00:36:38（UTC+08:00）的首个 measurement 检查点显示：launcher、编排器、
Realtime、两个 Gateway 和两个 load generator 全部存活；TCP 状态对应约 10,000 条已建立
连接。56 秒内 `INCOMING_MESSAGES` 写入 4,492 条（约 80.2 msg/s），consumer ACK floor
与 delivered sequence 同为 4,492，pending/ack-pending/redelivery 均为 0；两个 Gateway 的
Realtime consumer 也均无 pending 或 redelivery，死信为 0。该检查点只证明被中止轮当时
已正确进入稳态；因测量器语义缺陷，它不替代也不组成新的 8 小时最终 verdict。

新正式轮在 2026-08-06 02:23:20（UTC+08:00）的 measurement 检查点显示：10,000 条
TCP 连接全部 established；约 61.2 秒内 `INCOMING_MESSAGES` 写入 4,904 条（约
80.1 msg/s）。Realtime consumer 的 delivered/ACK floor 均为 4,904，两个 Gateway
consumer 的 delivered/ACK floor 均为 9,804；三者 pending、ack-pending、redelivery
全部为 0，死信为 0，Outbox 最老年龄为 0，主日志错误计数为 0。该检查点仅表示新轮
已健康进入 8 小时测量，最终结论仍必须等待完整 verdict。

## 5. 可追溯快照

- 新轮 .NET SDK：10.0.301（隔离安装，不修改系统 SDK）；实际 `dotnet` SHA-256：
  `763bfd4dbb1bb3a3b5257c6800eef77bb4abe2127e6ff9c33e2a56e2e814aedf`。
- 新轮 TCP + Realtime 冻结源码归档：`codex-tcp-soak-20260805T180247Z-source.tar.gz`，
  SHA-256 `513519173fdc3c91ab23f3a49d8143b7d8fea5f0038f53ab63fa0f863d6d62c0`。
- 新轮规范包源归档：`codex-tcp-soak-20260805T180247Z-canonical-feed.tar.gz`，
  SHA-256 `00823022224bc833ba1644d74a72b3e4a39ff6ab267c979f3b009fc76ddc6e4d`。
- 新轮报告内 `SnapshotBinding.Required/Complete` 均为 `true`；编排器在启动服务前重算
  三个文件摘要，所有 build/子进程使用上述已验证 `dotnet` 绝对路径。
- 以下为已中止首轮的历史快照，仅供诊断追溯：
- TCP/Gateway 快照：`ChatAppTCP_Server-v2.tar.gz`，SHA-256
  `d57e21af62ee0bb4637a7903b544831f2ec7960fc124a6c3d4fa89e23069674e`。
- Realtime 快照：`ChatApp.RealtimeServices.tar.gz`，SHA-256
  `70cb6167efc1f75bfe2b368741609ce6befe6727bd9aa5b528a5feba75d7efaf`。
- 两套解决方案在 Linux 锁定还原后 Release 构建均为 0 warning / 0 error。
- 当前冻结前本地验证：TCP 517 项中 516 passed、1 Redis 环境 skipped、0 failed；
  Realtime 285/285；两套解决方案 Release build 均为 0 warning / 0 error。

## 6. 正式 verdict（2026-08-06）

正式报告目录为 `reports/soak-8h-formal-v2`，本轮退出码为 `0`，最终 verdict 为
`PASSED`。报告独立判定 `RunValid=true`、`MemoryConclusive=true`、`MemoryStable=true`；
首轮 cwd 失败 canary 和此前主动中止轮次均未混入本结论。

### 6.1 运行窗口、连接与逐消息正确性

- 实际测量 `28,800.0414148s`（两 child 分别 `28,800.0412692s`、`28,800.0414148s`），
  8 小时窗口门禁通过；连接成功率和峰值连接率均为 `100%`（10,000/10,000），100 个
  active sender 全部出现。
- 总计发送 `2,304,000` 条，预期投递 `2,304,000` 条，MQ ACK `2,304,000` 条，实际
  收到 `2,304,000` 条；rejected、duplicate ACK、duplicate delivery、outstanding、
  tracking expired、tracking dropped 和 runtime failure 全部为 `0`，两个 child 的 drain
  均完成。每个 child 各自验证本 Gateway 内 `peer-ring`，不覆盖跨 Gateway 投递。
- 目标吞吐 `80.0 msg/s`，实际 `79.999884959 msg/s`，达成率 `99.999856%`。

### 6.2 延迟、JetStream、Outbox 与死信

| Gateway | ACK 平均 / P50 / P95 / P99 | peer-delivery 平均 / P50 / P95 / P99 |
|---|---:|---:|
| gateway-1 | 1.107 / 0.992 / 1.280 / 3.968 ms | 28.910 / 22.528 / 53.248 / 204.800 ms |
| gateway-2 | 1.135 / 1.024 / 1.344 / 3.840 ms | 30.953 / 24.576 / 57.344 / 204.800 ms |

完整消息延迟 P50/P95/P99 为 `24.576 / 57.344 / 204.800 ms`。JetStream measurement
窗口内 deliveries 与 ACK 均为 `2,304,000`，pending final 为 `0`；Realtime persisted
为 `2,304,000`，DEAD_LETTERS 为 `0`。Outbox published 为 `4,608,000`，最终 pending/dead
均为 `0`，最大尝试次数为 `0`；报告中的 oldest-age gauge 尾值 `22.588s` 对应无 pending
记录，不构成积压。

### 6.3 采样覆盖率、资源与内存趋势

- 资源观测覆盖 8 条 series（5 个进程 + 3 个依赖容器）；每条在协调 measurement 窗口的
  最小覆盖率为 `99.243%`（14,291/14,400），Prometheus 覆盖率为 `100%`，通过 `>=90%`
  门禁。
- Gateway-1：基线/最终窗口 RSS 中位数 `238.53 → 181.11 MiB`，增长 `-24.1%`，最终
  斜率 `-5.11 MiB/h`；Gateway-2：`235.70 → 181.71 MiB`，增长 `-22.9%`，最终斜率
  `-2.48 MiB/h`。两者均 `STABLE`，无重启；资源时间线峰值工作集约为 Gateway-1
  `286.7 MiB`、Gateway-2 `286.3 MiB`。
- Realtime 平均 CPU `6.51%`、最大工作集 `215.0 MiB`；两个 load child 平均 CPU
  `1.22%/1.23%`、最大工作集 `123.8/124.9 MiB`。NATS、PostgreSQL、Garnet 三个本轮
  临时容器均完成全窗口采样；其最大内存分别约 `373.0/3,942.4/236.9 MiB`。

### 6.4 报告与可复核文件

- 本地完整报告：[`.artifacts/remote-reports/soak-8h-formal-v2`](../.artifacts/remote-reports/soak-8h-formal-v2)
- Verdict：[soak-verdict-20260806-022238Z.json](../.artifacts/remote-reports/soak-8h-formal-v2/soak-verdict-20260806-022238Z.json)
- Benchmark：[benchmark-report.json](../.artifacts/remote-reports/soak-8h-formal-v2/capacity-curve-20260805-181649Z/rate-1/benchmark-20260805-181655Z/benchmark-report.json)
- 资源时间线：[process-resource-timeline.csv](../.artifacts/remote-reports/soak-8h-formal-v2/capacity-curve-20260805-181649Z/rate-1/benchmark-20260805-181655Z/process-resource-timeline.csv)

## 7. 2026-08-07 优化复测说明

本报告的 `605.6 GB / 262,842 B/msg`、每消息两个 Outbox 事件和同 Gateway peer-ring
均是 2026-08-06 冻结源码的历史结果，不应继续当作当前实现现状。后续已完成聚合授权、
单条多目标 Outbox、跨 Gateway 固定速率容量曲线，以及 SQL/Outbox allocation 热点优化。

最新可审计结果见 [TCP / Realtime 性能优化与跨 Gateway 容量复测](perf-optimization-rerun-20260807.md)
和 [版本化容量摘要](performance-baselines/2026-08-07-linux-cross-gateway-capacity.json)。
同负载 A/B 的 Realtime allocation 由 `127,756.48` 降至 `99,122.23 B/msg`，下降
`22.41%`；80/160/320/640 msg/s 四档跨 Gateway 曲线均通过，当前环境的持续建议值为
320 msg/s。最终提交快照仍需以新的正式 8 小时跨 Gateway soak 验证长期内存稳定性。

该正式轮已于 `2026-08-07T13:57:37Z` 启动，独立运行根目录为
`/home/yeluo/chatapp-perf/runs/codex-tcp-soak-opt-20260807T135316Z`，报告目录为
`reports/soak-8h-cross-gateway-v3`，主 PID/PGID 为 `1701156/1701156`。最终组合源码
SHA-256 为 `92c650d8fd2add8eca82000411bd2ab00eb8cc93bb5a15d6ebb28e564887a1ac`；
本历史报告的 PASSED verdict 不与新轮混合。

## 8. 优化后跨 Gateway 正式 8 小时 verdict（2026-08-08）

新轮以退出码 `0` 完成，最终 verdict 为 **PASSED**：`RunValid=true`、
`MemoryConclusive=true`、`MemoryStable=true`。实际 measurement 为
`28,800.0165649s`；10,000/10,000 连接成功，总计发送、ACK、跨 Gateway 预期投递和
实际收到均为 `2,304,000`，重复、漏投、拒绝、outstanding、tracking 丢失、runtime
failure 和死信全部为 `0`，实际吞吐为 `79.999954 msg/s`。

优化后 Realtime allocation 为 `225,766,484,480` bytes，即 `97,988.93 B/msg`，较本
历史轮的 `262,842 B/msg` 下降 `62.72%`；数据库操作由 `14.27` 降至
`10.1964 ops/msg`。Outbox 行由每消息两条降为一条，published 为 `2,304,000`；实际
Sharded NATS target publish 仍为 `4,608,000`，因为 sender ACK 与跨 Gateway recipient
delivery 需要两个目标 Gateway，不能将其误报为 NATS publish 减半。

Gateway-1 基线/最终 RSS 中位数为 `305.92 → 264.48 MiB`，最终斜率
`1.16 MiB/h`；Gateway-2 为 `309.70 → 265.13 MiB`、`1.36 MiB/h`，两者均独立
判定 `STABLE`。8 条资源 series 的最小 measurement 覆盖率为 `99.243%`，Prometheus
覆盖率为 `100%`。完整新轮分析、ACK 延迟和跨 child delivery latency 限制见
[优化复测报告](perf-optimization-rerun-20260807.md)；本地完整报告位于
[`.artifacts/remote-reports/soak-8h-cross-gateway-v3`](../.artifacts/remote-reports/soak-8h-cross-gateway-v3)。

2026-08-08 已继续针对该轮的异常式控制流、数据库准入往返和 UTF-8 临时分配完成第二轮
代码优化；具体共享层、安全边界、本地测试与微基准见优化复测报告第 8 节。该批改动尚未
执行新的正式 Linux 8 小时运行，因此本节历史 verdict 只证明 `d5e886a` 冻结版本，不能
作为第二轮改动的性能或长期内存结论。

## 9. 第二轮共享层复测进度（2026-08-08）

第二轮组合源码 SHA-256
`0e132e482e252156ac92baf3bd88a7ab0934a9906076ce622d69f828f23923cd`
已先通过 10,000 连接、80 msg/s、600 秒 measurement 的短时跨 Gateway 门禁：
`PASSED / VALID`，48,000 条发送、ACK、预期投递和实际投递完全一致，重复、漏投、
死信与运行错误为 0。数据库操作降至 `8.9546 ops/msg`，managed allocation 降至
`94,933.63 B/msg`；完整数据见优化复测报告第 9 节。

同一冻结源码的正式 8 小时轮已于 `2026-08-08T08:04:08Z` 启动：

- 运行目录：`/home/yeluo/chatapp-perf/runs/codex-tcp-soak-shared-20260808T080154Z`；
- 报告目录：`reports/soak-8h-cross-gateway-shared-v1`；
- 主 PID/PGID：`1905586/1905586`；
- 容器标签：`20260808080428z-1`；
- 规范包源 SHA-256：
  `00823022224bc833ba1644d74a72b3e4a39ff6ab267c979f3b009fc76ddc6e4d`；
- .NET host SHA-256：
  `763bfd4dbb1bb3a3b5257c6800eef77bb4abe2127e6ff9c33e2a56e2e814aedf`。

该轮与 2026-08-05/07 历史轮及短时 canary 完全隔离；最终只有在
`RunValid=true`、`MemoryConclusive=true`、`MemoryStable=true` 且 ACK/跨 Gateway
投递、重复/漏投、JetStream/Outbox/死信和异常式控制流全部通过后，才能替换本节的
“运行中”状态。

## 10. 第二轮共享层正式 8 小时最终结论（2026-08-09）

上一节的“运行中”状态现已完成，且不改变 2026-08-05 历史轮结论。本轮组合源码快照
`0E132E482E252156AC92BAF3BD88A7AB0934A9906076CE622D69F828F23923CD` 在
`/home/yeluo/chatapp-perf/runs/codex-tcp-soak-shared-20260808T080154Z` 运行，退出码
`0`，verdict 为 **PASSED**：`RunValid=true`、`MemoryConclusive=true`、
`MemoryStable=true`。

- 10,000/10,000 连接成功；两 child 合计发送、ACK、跨 Gateway 预期投递和实际收到
  `2,304,000`，重复、漏投、拒绝、outstanding、tracking 丢失、runtime failure、死信
  均为 `0`；总吞吐约 `80 msg/s`。
- ACK P50/P95/P99：gateway-1 `1.024/1.280/1.600 ms`，gateway-2
  `0.960/1.280/1.536 ms`。Delivery latency histogram 未由 harness 跨 child 汇总，
  `DeliveryLatency.Count=0` 代表未采集；不能据此得出零延迟或同 ID 相关性结论。
- JetStream pending `0`、redelivery 增量 `1`；Outbox persisted/published
  `2,304,000`，最终 pending/dead/max-attempts `0/0/0`，清理发布历史增量 `576,000`。
- 数据库操作 `8.203062 ops/msg`，managed allocation `89,889.49 B/msg`；
  `TaskCanceledException`、`SemaphoreFullException` 未出现，唯一 exception series
  `ArgumentException` 增量 `0`。
- 8 条资源 series 覆盖率 `99.243%`（Prometheus `100%`）。Gateway-1 RSS 中位数
  `296.93 → 272.16 MiB`、斜率 `-1.95 MiB/h`；Gateway-2 `304.11 → 267.67 MiB`、
  斜率 `-0.52 MiB/h`，均稳定且无 OOM/重启。

完整报告已归档至
[`.artifacts/remote-reports/soak-8h-cross-gateway-shared-v1`](../.artifacts/remote-reports/soak-8h-cross-gateway-shared-v1)，
详细分析见[优化复测报告](perf-optimization-rerun-20260807.md)。

## 11. 后续数据库归因说明（2026-08-09）

本历史报告中的 Docker PostgreSQL block I/O 不能解释为业务数据或 WAL。最新代码已在容量
报告中加入 PostgreSQL 原生 WAL、checkpoint、表/索引、tuple churn 和 SQL 级 `wal_bytes`
采集，并完成事务授权合并、Outbox HOT claim 与发布成功紧凑完成。实现、安全语义和短时
指示性 A/B 见[优化复测报告第 11 节](perf-optimization-rerun-20260807.md)。在新的正式轮完成前，
不得用短时结果回写本节历史 8 小时 verdict，也不得把旧 `644 GB` 继续当作真实数据量。

## 12. 第三轮数据库容量门禁（2026-08-09）

新的 PostgreSQL 原生诊断已用 1,000 连接、1,000 active sender、跨 Gateway 负载完成
并发 4/8/16 对照。固定 100 active sender 的首轮曲线会在 160 msg/s 起触发
生产单用户 `30 秒 / 30 条` 限流，已明确排除，没有把反滥用上限误报为数据库容量。

正确负载下，并发 4 可完整通过 320 msg/s，640 msg/s 时 JetStream 末段积压约
`22.9k`，仅投递 `54,417/76,810`；并发 8 将投递提升到 `71,490/76,818`，
仍低于 95% 门禁。并发 16 的 640 msg/s 单档为 **PASSED / VALID**：
发送、ACK、跨 Gateway 实际投递均为 `76,813`，JetStream pending 首值/尾值/峰值均为
`0`，Outbox pending 峰值 `144`、尾值 `0`，无 checkpoint、temp file、deadlock。

该通过档 WAL 为 `536,066,702 bytes = 6,978.9 B/msg`，`52,468` 次 WAL sync 平均
`1.334 ms`；PostgreSQL 平均/峰值 CPU `87.39%/196.40%`，数据库操作
`5.269 ops/msg`，managed allocation `74,356 B/msg`。详细 SQL WAL 归因、失败档的
fsync 证据、一次无效重复投递轮与 `644 GB` 的 block I/O / allocation 区分，见
[优化复测报告第 12 节](perf-optimization-rerun-20260807.md)。下一个正式 8 小时轮将以
可配置并发 16 运行，不通过关闭 `synchronous_commit` 换取性能。

## 13. 第三轮数据库优化后正式 soak 最终结论（2026-08-09）

第三轮组合源码归档
`13787B6A3B10E87CB6B3DD1BD5BBA67C65C7861914B503796F275915D1F33B89`
已在 `/home/yeluo/chatapp-perf/runs/codex-tcp-soak-dbopt-20260808T193632Z` 完成。退出码为
`0`，最终 verdict 为 **PASSED**：`RunValid=true`、`MemoryConclusive=true`、
`MemoryStable=true`。

- 10,000/10,000 连接成功；两个 child 合计发送、ACK、跨 Gateway 预期投递和实际收到
  均为 `2,304,000`，拒绝、重复 ACK/投递、漏投、outstanding、tracking 丢失、runtime
  failure 和死信全部为 `0`；总吞吐约 `80 msg/s`。
- ACK P50/P95/P99：gateway-1 `1.024/1.280/1.600 ms`，gateway-2
  `0.992/1.280/1.536 ms`。跨 child delivery latency histogram/同 ID 相关性仍未采集，
  `DeliveryLatency.Count=0` 不代表零延迟。
- JetStream delivery/ACK 均为 `2,304,000`，pending 尾值 `0`、峰值 `1`；Outbox
  persisted/published 均为 `2,304,000`，pending 尾值 `0`、峰值 `25`，dead 为 `0`。
  五个 stderr 为空，`TaskCanceledException`、`SemaphoreFullException` 均未出现。
- 8 条资源 series 覆盖率均为 `99.243%`，Prometheus 为 `100%`。Gateway-1 RSS
  中位数 `313.58 → 273.60 MiB`、末段斜率 `-5.96 MiB/h`；Gateway-2
  `307.49 → 279.95 MiB`、`-3.05 MiB/h`，均稳定且无 OOM/重启。
- 数据库操作为 `7.253158 ops/msg`，较第二轮正式轮再降 `11.58%`；managed allocation
  为 `90,269.52 B/msg`，较第二轮波动 `+0.42%`、基本持平，较最初 `262,842 B/msg`
  已下降 `65.66%`。

PostgreSQL Docker Block I/O 仍显示 `2.09 GB / 606 GB`，但原生统计已完成归因：实际 WAL
为 `21,976,258,655 bytes = 9,538.3 B/msg`（约 `20.47 GiB`），checkpoint/bgwriter/backend
逻辑页写合计约 `29.27 GiB`，核心表实际增长约 `6.09 GiB`；`temp_bytes=0`、deadlock=0。
因此 `606 GB` 不是业务数据、WAL 或 .NET 常驻内存。PGDATA 实际使用 Docker local volume，
差额应归为 cgroup/块设备聚合口径、文件系统 journal、WAL 段初始化和高频 fsync 的存储栈
写放大，不能把 OverlayFS 当作已证实主因。其数值较旧 `644 GB` 仅下降约 `5.9%`，后续
不能再把它当容量指标，并应补采原始 `io.stat`、进程 I/O 与宿主设备扇区作闭环。

SQL WAL 的主要来源是 messages INSERT `62.64%`、Outbox INSERT `16.28%`、会话投影
`9.33%`、幂等账本 `6.80%`、Outbox claim `2.50%`。当前 Outbox 已只写 `payload_utf8`，
`payload_json` 为 `NULL`，不存在双份 payload 落盘。下一步按“修复两个 Pending 索引合计
约 `16.936` 亿 tuple 的全局热扫，并增加提交后 event-id 有界队列快路径 → 合并 claim/完成
批次与 worker-local 连接复用 → 验证 conversations 全局列表索引并恢复 HOT →
审计 messages 索引/幂等账本生命周期 →
`wal_compression` 和 checkpoint 周期 A/B”推进；继续保持
`synchronous_commit=on`、`full_page_writes=on`，不以降低可靠性换性能。

完整报告已复制到
[`soak-8h-cross-gateway-dbopt-v1`](../.artifacts/remote-reports/soak-8h-cross-gateway-dbopt-v1)，
详细原生 WAL、fsync、Top SQL、表/索引和后续安全边界见
[优化复测报告第 13 节](perf-optimization-rerun-20260807.md)。

## 14. 第四阶段优化已实现、等待正式测量（2026-08-09）

针对第三轮的 Pending 索引热扫、`2.75 wal_sync/msg`、Conversation 非 HOT 和 FPI/checkpoint
写放大，代码已加入事务提交后有界 event-id 提示、按 ID 精确认领、worker-local 串行预编译
Npgsql session、100 行/100 ms 发布完成批处理、5 秒可靠恢复扫描，以及 Migration 058/059
的 HOT/冗余索引治理。正式脚本同时记录并默认使用 `wal_compression=lz4`、
`checkpoint_timeout=900s`、`max_wal_size=4096MB`，继续保持 `synchronous_commit=on`、
`full_page_writes=on`。

安全边界没有改变：数据库 Outbox、租约和 claim token 仍是权威；有界队列只用于加速，丢失
提示由恢复扫描接管；连接仅由单个 worker 串行拥有；NATS 发布成功后才进入数据库完成批次，
失败或崩溃仍以相同 EventId 重试。保留 retry/recovery、Dead、Published 清理和业务查询所需
索引，没有按写入型报告的零扫描盲删 messages 可靠性索引。

本地门禁：Realtime 构建 0 warning/0 error、单元 `296/296`、PostgreSQL 集成 `42/42`；
TCP 构建 0 warning/0 error、测试 `519 passed / 1 environment-skipped`。上述结果只证明代码与
可靠性契约通过，真实 WAL、fsync、DB ops、allocation、CPU、块写和 8 小时内存趋势仍需新的
冻结快照正式 soak 给出。详细实现和复测指标见
[优化复测报告第 14 节](perf-optimization-rerun-20260807.md)。

## 15. 后续验证改为分层执行（2026-08-09）

第四阶段不再为每个 SQL/资源改动重复运行 8 小时。新的统一入口按
`Smoke → Change → Capacity → Candidate → Formal` 分层：日常反馈约 1–5 分钟，容量筛查约
6–10 分钟，30 分钟 Candidate 只在准备合并/发布时执行，8 小时 Formal 仅用于最终长期内存、
WAL/checkpoint 和发布证据。Candidate/Formal 必须显式 `-ConfirmLongRun`。

最终分钟级 Smoke 已验证精确认领按主键锁定、物化资格校验的实现：1,600 条消息 ACK 与跨
Gateway 投递均为 100%，P95/P99 `2.688/2.944 ms`，无重复、漏投、死信、deadlock 或 temp
落盘；Pending index tuple read 从低速 Change 基线的约 `1,135.3/msg` 降到 `6.067/msg`
（约 `99.47%`），Conversation HOT 为 `100%`。完整单元/集成/TCP 回归也已通过。该短测只
作为修改反馈，不改写本文件已有三轮正式 8 小时 verdict，也不产生新的 `MemoryStable` 结论。
详细 A/B 与停止边界见[优化复测报告第 16 节](perf-optimization-rerun-20260807.md)。

## 16. Outbox 预领取轻量验证（2026-08-09）

在第 15 节分层反馈流程下，单聊 Outbox 热路径进一步改为业务事务内预写租约，提交后仅发送
owner/token 提示，Publisher 用主键只读校验后发布。队列满、回滚、冲突、提示丢失、进程崩溃
和租约过期仍由数据库 Outbox 与恢复扫描处理；共享层不缓存 payload、不跨 worker 共享数据库
连接，并复用 publisher generation token，避免每消息创建 GUID/string。

最终 Smoke 为 **PASSED / VALID**：1,000 连接、两个 Gateway、发送/ACK/跨 Gateway 投递
`1,599/1,599/1,599`，P95/P99 `2.944/5.632 ms`，最终 JetStream/Outbox pending 和 dead
均为 `0`。WAL 为 `6,078.4 B/msg`、WAL sync `1.117/msg`、DB ops `6.115/msg`、Pending
索引读 `6.375/msg`；Outbox exact/preclaimed/recovery/complete 调用为
`0/1,578/6/169`。相对上一版精确认领，额外 claim UPDATE 已完全消失，WAL 约降 `3.9%`、
sync 约降 `46.6%`。详细实现、安全边界和报告见
[优化复测报告第 17 节](perf-optimization-rerun-20260807.md)。该结果仍是分钟级反馈，不改写历史
8 小时 verdict，也不声明新的 `MemoryStable`。

## 17. Realtime 分配热点轻量闭环（2026-08-09）

当前实现使用 20 秒 `gc-verbose` trace 定位到 admission、单聊序号分配、messages INSERT 和
幂等账本的固定 SQL 被每消息重复插值。新增按 `RealtimeDatabaseSchema` 实例隔离的不可变
`Lazy<string>` 命令文本缓存；只共享 SQL，不共享 command、parameter、connection 或 transaction。
幂等账本成功路径也由无用的 `INSERT ... RETURNING` DataReader 改为受影响行数判定，冲突时
仍读取 canonical，可靠性语义不变。

同配置 Change 的 allocation 在 80/320 msg/s 从 `82,533.3/81,489.9 B/msg` 降至
`70,357.0/68,972.3 B/msg`（`-14.75%/-15.36%`），WAL 基本不变；trace 的
allocation-tick `1,247 → 1,026`、`String.Ctor` `173 → 14`，两个目标读取路径的重复 SQL
字符串样本 `160 → 0`。最终 Smoke 发送/ACK/跨 Gateway 投递 `1,600/1,600/1,600`，P99
`3.20 ms`，allocation `70,185.6 B/msg`、WAL `6,068.4 B/msg`、sync `1.119/msg`，所有
pending/dead/DLQ/重复/漏投为 `0`。全量单元 `300/300`、PostgreSQL 集成 `43/43`。

详细 trace、A/B、并发安全边界和停止条件见
[优化复测报告第 18 节](perf-optimization-rerun-20260807.md)。该结果仍是分钟级反馈，不声明
长期内存稳定性；下一次长测只在发布候选冻结后执行。

## 18. 单聊数据库往返合并轻量验证（2026-08-09）

根据 Top SQL 归因，无附件单聊的 messages、Outbox 和可选幂等账本已合并为同一事务内的一条
数据修改 CTE；生命周期/授权/幂等 admission 与 Conversation 序号分配再合并为另一条命令。
共享层仅缓存不可变、按 schema
实例隔离的 SQL，不共享 Npgsql connection、transaction、command 或 parameter。附件消息继续
走原有分步绑定路径，冲突、回滚和幂等 canonical 语义不变。

相同 Change A/B 中，80/320 msg/s 的 DB ops 从 `6.101/5.557` 降至 `4.090/3.773`
（约 `-33.0%/-32.1%`），managed allocation 从 `70,357.0/68,972.3 B/msg` 降至
`64,575.7/63,316.3 B/msg`（均约 `-8.2%`）；320 档 ACK P99 从 `17.408 ms` 降至
`4.352 ms`，WAL 基本持平。两个档位 ACK/跨 Gateway 投递均为 `100%`，最终 pending、dead、
DLQ、重复、漏投、deadlock 和 temp bytes 均为 `0`。

预领取 Outbox 的 `next_attempt_at_ms` 同时改为 lease expiry，减少有效租约行参与 ownerless
recovery 热扫；预领取主键读取仍校验 owner、token、Pending 和有效 lease，租约到期恢复契约由
集成测试覆盖。最终 320 msg/s Smoke 发送/ACK/投递 `6,395/6,395/6,395`，DB ops
`3.710/msg`、allocation `63,920.3 B/msg`、WAL `5,688.7 B/msg`、sync `0.775/msg`，
Pending index read `55.664/msg`，最终队列清空。全量单元 `300/300`、PostgreSQL 集成
`44/44`。

最终 admission+sequence 合并后的 320 msg/s Smoke 继续 **PASSED / VALID**：发送/ACK/跨
Gateway 投递 `6,400/6,400/6,400`。DB ops 由上一轮 `3.710` 再降至 `2.622/msg`
（`-29.3%`），allocation 降至 `62,571.2 B/msg`，WAL/sync 为 `5,680.5 B/msg` 和
`0.688/msg`，Pending index read 为 `55.015/msg`。相对第 17 节 SQL cache 后的 320 Change
基线 `5.557 DB ops/msg`，累计下降 `52.8%`。Top SQL 只剩 admission+sequence 与
message+Outbox+ledger 两条固定业务命令；没有跨并发消息共享数据库连接，也没有降低 PostgreSQL
durability。

详细实现、无效诊断轮排除、安全边界和报告见
[优化复测报告第 19 节](perf-optimization-rerun-20260807.md)。本阶段继续执行分钟级反馈；只有发布
候选冻结后才运行 30 分钟 Candidate，最终发布前再运行一次 8 小时 Formal。

## 19. Outbox hint 窗口默认值门禁（2026-08-09）

`0 ms` 与 `2 ms` 各三轮同构轻量跨 Gateway A/B 已完成。`2 ms` 可减少约 `20%` DB ops/msg，
但 Delivery P95/P99 中位数和尖峰频率均更差，因此默认保持 `0 ms`；`2 ms` 仅保留为显式资源优先
配置。本结论只决定配置默认值，不改写前三轮正式 8 小时 verdict，也不声明新的长期内存稳定性。
数据、排除样本和报告见[优化复测报告第 20 节](perf-optimization-rerun-20260807.md)。
