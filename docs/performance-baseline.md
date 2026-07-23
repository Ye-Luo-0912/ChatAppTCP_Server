# 性能基线执行说明

## 当前状态

持久消息链路与多进程组合负载已经可重复执行，并输出 JSON 与 Markdown 报告。
2026-07-20 已完成首轮本机 30 分钟固定基线，并将脱敏机器摘要保存到
performance-baselines 目录。该数据用于同机同配置版本比较，不是生产容量承诺。
此前的 5 秒运行仍只用于验证测试工具、持久化闭环和容量指标。

本次烟测配置：

- Windows x64、16 个逻辑处理器、约 31.3 GiB 可用内存；
- .NET 11.0.0-preview.6.26359.118；
- 本机三节点 NATS、临时 PostgreSQL、单个 RealtimeServices；
- 4 路并发、目标 4 pipeline/s、512 字节正文、1 秒预热、5 秒测量。

结果：24/24 条完整链路成功，错误率 0%，实际完成 4.39 pipeline/s；完整链路
p50 362.5 ms、p95/p99 428.0 ms。运行结束后 32 条消息全部已读，History 失败、
History queue depth、History in-flight 和 Outbox pending 均为 0，Outbox 最大尝试
次数为 1。原始结果位于 `.artifacts/performance/pipeline-load-20260719-214629Z.*`。

## 多进程组合基准

`ChatApp.Performance.Orchestrator` 已能一条命令启动并采样多个 Gateway、
RealtimeServices、TCP/持久链路负载进程，并按显式容器名采集 NATS、PostgreSQL 和
Garnet。统一报告包含子负载吞吐、错误率、p50/p95/p99、进程资源、Docker 资源及
Prometheus 前后增量。

数据库密钥通过环境变量传给服务，报告不保存变量值。正式 30 分钟基准入口：

```powershell
$env:CHATAPP_BENCHMARK_DB = "Host=127.0.0.1;Port=5432;Database=ChatAppDatabase;Username=postgres;Password=..."
$env:CHATAPP_BENCHMARK_GARNET = "127.0.0.1:6379,abortConnect=false"

dotnet run --project .\tools\ChatApp.Performance.Orchestrator -c Release -- `
  --gateway-count 2 `
  --warmup-seconds 30 `
  --duration-seconds 1800 `
  --sample-interval-ms 5000 `
  --tcp-mode connection `
  --tcp-connections 1000 `
  --pipeline-concurrency 8 `
  --pipeline-operations-per-second 0 `
  --pipeline-payload-bytes 512 `
  --realtime-database-environment CHATAPP_BENCHMARK_DB `
  --garnet-environment CHATAPP_BENCHMARK_GARNET `
  --docker-container chatapp_nats `
  --docker-container chatapp_postgres `
  --docker-container chatapp_garnet `
  --report-directory .artifacts\performance
```

2026-07-20 使用隔离的单节点 JetStream、临时 PostgreSQL 和现有 Garnet 完成 3 秒
组合烟测：持久链路 8/8 成功、错误率 0%、约 2.37 pipeline/s、完整链路 p99 440 ms；
TCP 连接 12/12 成功。该结果只验证编排、采样和报告，不作为容量基线。完整参数和
chat/慢消费者组合方式见[编排器说明](../tools/ChatApp.Performance.Orchestrator/README.md)。

## 2026-07-20 本机 30 分钟优化前基线

环境和固定参数：

- MECHREVO WUJIE15XA，AMD Ryzen 7 8845HS，8 核/16 线程，31.29 GiB 内存；
- Windows 10.0.26300 x64，.NET SDK/Runtime 11 Preview 6；
- Release 构建；2 个 Gateway；1000 个 TCP 长连接，每个实例 500 个；
- 30 秒预热、1800 秒负载、5 秒资源采样；
- 8 路持久链路最大吞吐、512 字节 JSON 正文；
- 隔离的单节点 NATS JetStream、PostgreSQL 16.8 和 Garnet 1.0.84。

负载结果：

| 场景 | 成功 | 失败 | 吞吐 | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 完整持久链路 | 94,715 | 0 | 52.61 pipeline/s | 50.5 ms | 437.5 ms | 441.5 ms |
| Gateway 1 长连接 | 500 | 0 | 保持 1800 秒 | 不适用 | 不适用 | 不适用 |
| Gateway 2 长连接 | 500 | 0 | 保持 1800 秒 | 不适用 | 不适用 | 不适用 |

持久链路分段延迟：

| 阶段 | 平均 | p50 | p95 | p99 | 最大 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 消息发布 ACK | 1.00 ms | 1.0 ms | 1.5 ms | 2.5 ms | 22.39 ms |
| 消息持久化至 Outbox 发布 | 67.40 ms | 19.5 ms | 207.5 ms | 214.0 ms | 244.19 ms |
| 回执发布 ACK | 1.08 ms | 1.5 ms | 2.0 ms | 2.5 ms | 16.14 ms |
| 回执持久化至 Outbox 发布 | 71.77 ms | 20.5 ms | 219.5 ms | 221.0 ms | 334.26 ms |
| History 查询 | 12.87 ms | 13.0 ms | 21.0 ms | 25.5 ms | 114.57 ms |

稳定性和容量信号：

- 消息、回执和 History 各完成 94,715 次；Outbox 发布 189,430 次；
- 两个 JetStream consumer 最终 pending 均为 0，连接状态为 1；
- 失败、重投、死信、客户端丢弃和慢消费者计数均无增长；
- 最终 Outbox pending 为 8，等于负载并发窗口；最老记录 3.605 秒；
- 两个 Gateway 的最大工作集分别为 97.88/96.86 MiB，结束时为
  70.88/70.50 MiB；RealtimeServices 最大 147.69 MiB，结束时 136.84 MiB；
- NATS 最大 180.20 MiB、结束 94.83 MiB；PostgreSQL 最大/结束 486.80 MiB；
  Garnet 最大 44.54 MiB。运行中快照显示托管进程和 NATS 已回落，PostgreSQL
  增长速度在后半程明显放缓，30 分钟内未见无界增长；
- 进程 CPU 是按 16 个逻辑处理器归一化的主机占比；Docker CPU 以单核 100% 计，
  两者不能直接横向比较。

完整链路 p95/p99 的主要等待集中在两个 Outbox 阶段，并与
Outbox:PollIntervalMs=200 的固定空闲轮询周期吻合。该基线保留为优化前对照；事务提交后
进程内唤醒 OutboxPublisherWorker、同时保留 200 ms 兜底轮询的实现及复测结果见下一节。

同一机器、拓扑和参数下，先采用 10% 人工复核线：

- 吞吐低于 47.35 pipeline/s；
- 完整链路 p95 高于 481.25 ms 或 p99 高于 485.65 ms；
- 任一操作失败、1000 个 TCP 连接未全部成功；
- 任一 JetStream consumer 最终 pending 非 0，或失败/重投/丢弃计数增长；
- 最终 Outbox pending 超过 16，或最老年龄超过 15 秒。

资源数据受 Docker Desktop 和宿主页缓存影响，首轮只做 10% 人工复核，不作为硬失败。
30 分钟也不能替代 8–24 小时浸泡。脱敏的版本化数据见
[机器可读摘要](performance-baselines/2026-07-20-local-single-node-30m.json)，完整原始
报告位于 .artifacts/performance/benchmark-20260720-102632Z/。

## Outbox 主动唤醒优化后 30 分钟基线

实现保持原有可靠性边界：容量 1 的进程内信号只负责合并本实例事务提交通知；
OutboxPublisherWorker 被通知后立即抢占批次。跨实例写入、通知竞态、进程重启和历史
遗留记录仍由 200 ms 轮询兜底，因此没有把数据库 Outbox 降级成仅依赖内存通知。

相同硬件、依赖、2 个 Gateway、1000 个 TCP 长连接、8 路并发、512 字节 JSON 正文和
1800 秒负载的 A/B 结果：

| 指标 | 优化前 | 主动唤醒后 | 变化 |
| --- | ---: | ---: | ---: |
| 成功完整链路 | 94,715 | 208,523 | +120.2% |
| 失败 | 0 | 0 | 不变 |
| 吞吐 | 52.61/s | 115.84/s | +120.2% |
| 完整链路 p50 | 50.5 ms | 62.0 ms | +22.8% |
| 完整链路 p95 | 437.5 ms | 123.0 ms | -71.9% |
| 完整链路 p99 | 441.5 ms | 165.0 ms | -62.6% |
| 消息 persisted_outbox p95 | 207.5 ms | 35.5 ms | -82.9% |
| 回执 persisted_outbox p95 | 219.5 ms | 34.5 ms | -84.3% |
| History p95 | 21.0 ms | 61.0 ms | +190.5% |

稳定性和资源结论：

- 消息、回执、History、两类 JetStream delivery/ACK 均为 208,523；Outbox 发布
  417,046 次，计数完全闭合；
- 0 失败、0 重投、0 死信、0 丢弃；两个 consumer 最终 pending 均为 0；
- 最终 Outbox pending 为 3、最老年龄 2.852 秒，处于 5 秒指标采集周期内；
- 两个 Gateway 最大工作集约 100 MiB，RealtimeServices 最大 145.66 MiB，均未因
  2.2 倍持续吞吐出现工作集失控；
- PostgreSQL 平均 CPU 为 572%（Docker 单核 100% 口径）、峰值 926.9%，最大内存
  1,079.3 MiB；NATS 平均 CPU 27.55%，最大内存 278.5 MiB；
- 120 秒短测曾达到 189.79 pipeline/s；30 分钟持续值降至 115.84/s，同时 History
  p95 升至 61 ms，证明新容量瓶颈已下移到 PostgreSQL/History 查询和磁盘，而不是
  Outbox、Gateway 或 JSON 编解码。

优化后同机 10% 人工复核线更新为：

- 吞吐低于 104.26 pipeline/s；
- 完整链路 p95 高于 135.3 ms 或 p99 高于 181.5 ms；
- 任一操作失败、1000 个 TCP 连接未全部成功；
- 任一 JetStream consumer 最终 pending 非 0，或失败/重投/丢弃计数增长；
- 最终 Outbox pending 超过 16，或最老年龄超过 15 秒。

History 的收件人/发件人 UNION 两个分支有界 Top-N 已完成，验证结果见下一节。
随后做固定速率容量曲线和依赖故障注入。优化后的脱敏数据见
[主动唤醒机器基线](performance-baselines/2026-07-20-local-single-node-30m-outbox-signal.json)，
完整报告位于 .artifacts/performance/benchmark-20260720-111938Z/。

## History 分支 Top-N 优化验证

查询保持原有 `(received_at_ms DESC, message_id DESC)` 稳定排序和游标条件。收件人与
发件人两个索引分支现在分别排序并限制为 `take`，再由外层合并最终 Top-N；发件人分支
继续排除自发自收消息，避免同一消息重复出现。

PostgreSQL 16 `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)` 结果：

| 数据 | 指标 | 优化前 | 分支 Top-N 后 | 变化 |
| --- | --- | ---: | ---: | ---: |
| 40 万合成历史 | 实际扫描行 | 400,000 | 12 | -99.997% |
| 40 万合成历史 | 缓冲页 | 8,320 | 约 8 | -99.9% |
| 40 万合成历史 | 执行时间 | 82.267 ms | 0.158 ms | -99.8% |
| 真实短测数据、单用户 1,535 条 | 实际扫描行 | 1,535 | 11 | -99.3% |
| 真实短测数据、单用户 1,535 条 | 缓冲页 | 1,162 | 17 | -98.5% |
| 真实短测数据、单用户 1,535 条 | 执行时间 | 6.055 ms | 0.071 ms | -98.8% |

第一页、同毫秒 `message_id` 次序、游标下一页和自发自收只出现一次均在 PostgreSQL
临时表上通过。隔离 120 秒全链路验证完成 12,247/12,247、0 失败，报告位于
`.artifacts/performance/benchmark-20260720-122554Z/`。该短测使用了与上一轮不同的
临时容器镜像和宿主状态，整体吞吐不能与上一轮 120 秒结果直接 A/B；这里只将同一
PostgreSQL 实例、同一数据集的执行计划作为可归因结论。固定输入速率验证见下一节。

## 固定速率容量曲线

新增 `Run-CapacityCurve.ps1`，每个速率档使用全新临时 NATS、PostgreSQL 和 Garnet，
在 finally 中清理容器。负载为 2 个 Gateway、1000 个 TCP 连接、32 路 Pipeline 并发、
512 字节 JSON 正文；每档预热 10 秒、测量 60 秒。

| 目标速率 | 实际速率 | 达成率 | p95 | p99 | 失败 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 40/s | 40.16/s | 100.4% | 151.5 ms | 199.5 ms | 0 |
| 80/s | 79.05/s | 98.8% | 144.0 ms | 255.5 ms | 0 |
| 120/s | 115.79/s | 96.5% | 190.5 ms | 214.5 ms | 0 |
| 160/s | 126.63/s | 79.1% | 402.5 ms | 1,245.5 ms | 0 |
| 200/s | 186.61/s | 93.3% | 159.5 ms | 211.0 ms | 0 |
| 240/s | 213.67/s | 89.0% | 185.5 ms | 205.0 ms | 0 |
| 280/s | 190.50/s | 68.0% | 208.0 ms | 317.0 ms | 0 |
| 320/s | 252.08/s | 78.8% | 142.0 ms | 167.0 ms | 0 |
| 400/s | 204.70/s | 51.2% | 205.5 ms | 392.0 ms | 0 |

高档位结果非单调，说明 252.08/s 只能视为短时峰值，不能作为持续容量。负载生成器采用
有界闭环节流，worker 变慢时会降低实际注入速率，因此目标未达成不等价于开放环形负载
在服务端形成等量积压。

目标 120/s 又执行了 30 秒预热、300 秒持续确认：34,626/34,626 成功、实际
115.35/s（96.1%）、p50/p95/p99 为 91.5/174/214 ms、History p95 为 4 ms；
JetStream 最终 pending 为 0，Outbox pending 为 7、最老 2.957 秒；PostgreSQL
平均/峰值 CPU 为 27.2%/61.43%。

短期同机门禁设为：目标 120/s 时实际不低于 110/s、0 错误、p95 不高于 200 ms、
p99 不高于 250 ms、JetStream 最终 pending 为 0、Outbox pending 不超过 16 且最老
不超过 15 秒。故障注入门禁已完成；在完成 8–24 小时浸泡前，单节点业务运行预算仍保守按不高于
80 pipeline/s。完整曲线位于 `.artifacts/performance/capacity-curve-20260720-145218Z/`
和 `.artifacts/performance/capacity-curve-20260720-145936Z/`，5 分钟确认位于
`.artifacts/performance/capacity-curve-20260720-150540Z/`；脱敏摘要见
[容量曲线机器基线](performance-baselines/2026-07-20-local-single-node-capacity-curve.json)。

## 依赖故障注入短测

新增 `Run-FaultInjection.ps1`，在负载真正启动后停止并重启指定容器，记录 readiness
依赖状态、恢复时间、积压收敛以及完整 Pipeline 的成功/失败。2026-07-20 使用 20/s、
8 路并发、100 个 TCP 连接和 3 秒故障窗口完成首轮工具短测：

| 依赖 | 结果 | 健康恢复 | 积压收敛 | Pipeline | 结论 |
| --- | --- | ---: | ---: | ---: | --- |
| Garnet | 通过 | 3.42 s | 3.55 s | 568/568 | 自动恢复，0 失败 |
| PostgreSQL（修复前） | 失败 | 未恢复 | 未收敛 | 96 成功/16 失败 | Outbox Worker 未捕获 `57P01`，宿主退出 |
| PostgreSQL（修复后） | 通过 | 1.66 s | 1.68 s | 575/575 | 宿主保持运行，0 失败，JS pending=0 |
| NATS（pause/unpause） | 通过 | 0.12 s | 0.15 s | 513/513 | 短断线自动恢复，0 失败，JS/Outbox pending=0 |

PostgreSQL 修复是在 `OutboxPublisherWorker` 内捕获非取消异常、标记暂时故障并指数退避；
下一次成功循环恢复 heartbeat，不再让 `BackgroundServiceExceptionBehavior=StopHost` 终止
整个进程。NATS 短断线使用 pause/unpause，513/513 成功，p95 91.5 ms，恢复后 JS 与
Outbox pending 均为 0。单副本容器 `stop --timeout 1` 是 crash 耐久性场景，曾出现已确认
事件在重启后不可见；这不能由单节点本机拓扑提供零丢失承诺，必须在生产 3 副本
JetStream 场景单独验收，不能与普通断线恢复混为一类。

短测报告位于 `.artifacts/performance/fault-injection-20260720-154550Z/`、
`.artifacts/performance/fault-injection-20260720-154743Z/` 和
`.artifacts/performance/fault-injection-20260720-155127Z/`。这些是工具与故障边界验证，
不替代 10 秒故障、60 秒恢复窗口的正式稳定性基线。
## 长时间浸泡执行入口

浸泡测试沿用 80 pipeline/s、32 并发、1000 TCP 连接的已验证负载模型，默认运行 8 小时并每 2 秒保存一次运行时趋势汇总。它会创建且最终清理独立的 NATS、PostgreSQL、Garnet 容器。

```powershell
pwsh .\tools\ChatApp.Performance.Orchestrator\scripts\Run-Soak.ps1
```

短时验证可使用至少 5 分钟的窗口：`pwsh .\tools\ChatApp.Performance.Orchestrator\scripts\Run-Soak.ps1 -DurationSeconds 300 -WarmupSeconds 30 -SkipBuild`。每次运行的 `benchmark-report.md` 中新增 `Soak metric trends`，包含 .NET GC/分配/堆、Npgsql 连接池、JetStream 与 Outbox 的首尾、变化、最小值和峰值；进程和 Docker 的 CPU、内存、线程/句柄趋势保留在同一报告。
## Linux 远端正式浸泡基线

2026-07-21/22 已将测试迁移到 CachyOS Linux 主机执行，使用 .NET 11 Preview、单节点 JetStream、PostgreSQL 16.8 和 Garnet 1.0.84。8 小时窗口（预热 300 秒、pipeline 目标 80/s）共完成 2,303,575 次 pipeline，失败 0，实际吞吐 79.98/s，p95 200.5 ms、p99 270 ms；JetStream 最终 pending=0，Outbox 最终 pending=0，最老 Outbox 项最大 4.605 秒。运行时趋势中 Gen2 GC 增量 59，LOH 最终约 10.9 MiB，Npgsql 使用连接峰值 14、空闲连接峰值 18；Realtime 工作集结束约 203.5 MiB，私有内存峰值约 410.6 MiB。

随后在同一 Linux 主机创建 3 副本 JetStream 集群，负载中硬停止/重启一个 NATS 节点。pipeline 4,816/4,816 成功，吞吐 40.08/s，p95 111 ms；NATS disconnected=1、reconnection=1，最终 connected=1，JetStream pending=0。该结果确认单节点硬重启不会造成消息丢失或静默积压。

详细归档：[2026-07-22-linux-remote-resilience.json](performance-baselines/2026-07-22-linux-remote-resilience.json)。

## 自动化性能门禁

`ChatApp.Performance.Gate` 会验证编排器生成的 `benchmark-report.json`，并在报告异常、任一
必需指标缺失、pipeline 出错、p95 超过 300 ms、JetStream 最终 pending 非 0、Outbox
pending 超过 16 或最老消息超过 5 秒时以非零退出。2026-07-22 已在 Linux 主机直接对上述
8 小时原始报告复验通过：错误率 0%、p95 200.50 ms、JetStream/Outbox 最终 pending 均为 0。

同日已完成双 Gateway、RealtimeServices、NATS、PostgreSQL 和 Garnet 的安全组合回归：
45 秒内 pipeline 1,368/1,368 成功、p95 101 ms；TCP chat 鉴权连接 40/40 成功、0
失败。自动化临时鉴权采用随机令牌、仅用户可读写的令牌文件和结束删除，不进入报告或
进程命令行。门禁现同时要求已配置的 TCP 连接全部成功且失败数为 0。

该工具应在 Linux 自托管的定时任务中紧接浸泡测试执行，并保存 `benchmark-report.json`、
`benchmark-report.md` 和 `gate-result.json` 三项产物。具体命令与阈值覆盖方式见
[性能门禁说明](../tools/ChatApp.Performance.Gate/README.md)。在 Git 服务地址、仓库和
自托管 runner 注册完成前，不宣称 CI 已部署。
## 三依赖正式故障恢复基线

正式参数为目标 80 pipeline/s、32 路并发、1000 个 TCP 连接、512 字节正文；负载开始
20 秒后注入 10 秒故障，恢复观察窗口 60 秒。每个场景使用全新依赖容器，结果如下：

| 依赖 | 动作 | 健康恢复 | 积压收敛 | Pipeline | 实际吞吐 | p95 / p99 | 最终积压 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| NATS | pause/unpause | 0.095 s | 0.121 s | 6,336/6,336 | 70.16/s | 162.5 / 200.5 ms | JS=0，Outbox=0 |
| PostgreSQL | stop/start | 3.586 s | 6.682 s | 6,080/6,080 | 67.36/s | 149.5 / 190.5 ms | JS=0，Outbox=4 |
| Garnet | stop/start | 3.321 s | 3.341 s | 7,173/7,173 | 79.35/s | 118.5 / 136.0 ms | JS=0，Outbox=6 |

三项均为 0 明确失败，依赖恢复后 readiness 自动恢复，JetStream pending 归零，Outbox
保持在门禁上限 16 以内。为此补齐了 Outbox 数据库异常退避、AccountCleanupWorker 空闲
心跳、同 MsgId JetStream 重连发布、历史瞬时失败重试以及阶段化负载错误诊断。

单副本 NATS 强制终止不属于本表的短断线场景；硬重启耐久性必须在生产等价的 3 副本
JetStream 上单独验证。脱敏机器基线见
[三依赖故障恢复机器基线](performance-baselines/2026-07-20-local-single-node-fault-injection.json)。
## 持久链路单工具基准

先启动 NATS、PostgreSQL 和 RealtimeServices，再从 TCP 网关仓库根目录执行：

```powershell
dotnet run --project .\tools\ChatApp.Realtime.PipelineLoadGenerator -c Release -- `
  --warmup-seconds 30 `
  --duration-seconds 1800 `
  --concurrency 8 `
  --operations-per-second 0 `
  --payload-bytes 512 `
  --report-directory .artifacts\performance
```

`--operations-per-second 0` 用于探测最大吞吐；建立服务等级基线时还应分别执行固定
目标速率，观察延迟是否随队列积压持续上升。同一主机和 NATS 环境只运行一个持久链路
生成器实例，其 durable consumer 会在重复运行间复用。

## 正式验收矩阵

1. 30 分钟稳定基准：固定硬件、容器资源、数据规模、并发和正文大小。
2. 8–24 小时浸泡：检查工作集、Gen2/LOH、连接、JetStream pending/redelivery、
   Outbox pending 和 History queue depth 是否随时间无界增长。
3. TCP 组合场景：`Run-ConversationCombo.ps1` 并行跑持久链路（含会话历史/列表/
   mark-read/SyncBootstrap）与 TCP chat 扇出/慢消费者，并以
   `--require-conversation-stages` 过门禁。
4. 故障恢复：依次短断 NATS、PostgreSQL 和 Garnet，验证恢复后积压归零且无静默丢失。
5. 版本比较：保存 JSON 报告；p95/p99 或吞吐回退超过 10% 时人工复核。

正式报告必须同时记录服务进程 CPU、工作集、GC 分配、数据库连接池、TCP 排队条数/
字节、JetStream pending/redelivery、Outbox pending/age、History queue depth/in-flight
以及错误样本。OpenTelemetry/OTLP、RealtimeServices Prometheus 和 W3C 跨进程追踪已经
接入；统一多进程编排器、NATS 重连/redelivery 指标、首轮基线、Outbox 主动唤醒 A/B、
History 有界 Top-N、固定速率容量曲线、故障注入与 8 小时浸泡均已完成。下一步用
会话组合场景校准阶段阈值，并把版本比较纳入定时 CI 门禁。

运行时检查：

```powershell
Invoke-WebRequest http://127.0.0.1:8080/metrics
Invoke-RestMethod http://127.0.0.1:8080/diagnostics/runtime
```

生产环境由 OTLP Collector 汇聚两个服务；Gateway 的独立 HttpListener exporter 是
预发布开发组件，默认关闭，不作为生产采集链路。

NATS 生命周期与 JetStream pending/redelivery/ACK 指标已经接入。2026-07-20 的隔离
恢复演练确认 NATS 暂停后 `connected` 从 1 变为 0，重启后恢复为 1，且
`reconnections_total` 增加 1。正式基准使用的首轮阈值和仪表盘要求见
[可观测性与告警基线](observability-alerts.md)。
