# R3 TCP + Realtime 8 小时浸泡复跑记录

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
