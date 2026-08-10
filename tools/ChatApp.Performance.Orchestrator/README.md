# 多进程性能基准编排器

该工具在一次运行中启动一个 RealtimeServices、一个或多个 TCP Gateway，并行执行 TCP
负载与持久消息负载。它按固定间隔采样子进程和显式指定的 Docker 容器，抓取
RealtimeServices 的 Prometheus 前后快照，最后生成统一 JSON/Markdown 报告。

编排器只终止自己启动的 .NET 子进程。`--docker-container` 只启用 `docker stats` 采样，
不会启动、停止或删除已有容器。

## 冻结快照绑定

正式基准或用于发布结论的 canary 应启用严格快照绑定。编排器会把运行目录、源码归档、
规范包源归档和实际 `dotnet` 可执行文件的 SHA-256 同时写入 JSON/Markdown 报告；缺少
任一项或摘要格式错误时，在启动服务和负载前直接失败：

```powershell
$env:CHATAPP_BENCHMARK_REQUIRE_SNAPSHOT_BINDING = "true"
$env:CHATAPP_BENCHMARK_RUN_ID = "codex-tcp-soak-20260805T200000Z"
$env:CHATAPP_BENCHMARK_RUN_ROOT = "/home/user/chatapp-perf/runs/codex-tcp-soak-20260805T200000Z"
$env:CHATAPP_BENCHMARK_SOURCE_ARCHIVE_PATH = "$env:CHATAPP_BENCHMARK_RUN_ROOT/source-archives/source.tar.gz"
$env:CHATAPP_BENCHMARK_SOURCE_ARCHIVE_SHA256 = "<64-hex>"
$env:CHATAPP_BENCHMARK_CANONICAL_FEED_ARCHIVE_PATH = "$env:CHATAPP_BENCHMARK_RUN_ROOT/source-archives/canonical-feed.tar.gz"
$env:CHATAPP_BENCHMARK_CANONICAL_FEED_ARCHIVE_SHA256 = "<64-hex>"
$env:CHATAPP_BENCHMARK_DOTNET_PATH = "/opt/dotnet/dotnet"
$env:CHATAPP_BENCHMARK_DOTNET_SHA256 = "<64-hex>"
```

严格模式会在启动服务前读取三个文件并重新计算摘要，拒绝摘要不匹配、相对/不存在路径、
归档不在本轮 `source-archives` 下，或仓库不在本轮 `source` 下的运行。构建和所有受管
子进程也统一使用校验后的 `CHATAPP_BENCHMARK_DOTNET_PATH`，不再重新从 `PATH` 解析。
临时开发基准可以不设置这些变量；报告会明确显示绑定未启用且不完整，不能作为正式
性能结论的可复现来源。`Run-Soak.ps1` 属于正式入口，会强制要求上述变量；缺失时甚至
不会创建临时依赖容器。

## 正式持久链路基准

数据库连接等密钥通过“环境变量名”传入，报告只记录变量名，不保存变量值：

```powershell
$env:CHATAPP_BENCHMARK_DB = "Host=127.0.0.1;Port=5432;Database=ChatAppDatabase;Username=postgres;Password=..."
$env:CHATAPP_BENCHMARK_GARNET = "127.0.0.1:6379,abortConnect=false"

dotnet run --project .\tools\ChatApp.Performance.Orchestrator -c Release -- `
  --gateway-count 2 `
  --gateway-base-port 18888 `
  --realtime-port 18080 `
  --nats-url nats://127.0.0.1:4222 `
  --warmup-seconds 30 `
  --duration-seconds 1800 `
  --sample-interval-ms 1000 `
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

正式模式要求 `/ready` 返回成功，即 Worker、NATS、PostgreSQL 和 Garnet 全部健康。
实际容器名称以 `docker ps` 为准。默认先构建所有目标；已完成 Release 构建时可传
`--no-build`。

## 会话组合场景

持久链路现已在单次 pipeline 内覆盖：消息写入、回执、会话历史（含翻页）、会话列表、
已读标记与 SyncBootstrap。要与 TCP chat 扇出/慢消费者并行，使用：

```powershell
pwsh .\tools\ChatApp.Performance.Orchestrator\scripts\Run-ConversationCombo.ps1 `
  -DurationSeconds 120 -WarmupSeconds 15 -SkipBuild
```

脚本会启动临时依赖容器，以 `tcp-mode chat` + `--tcp-bootstrap-auth` + 慢消费者并行跑
pipeline，并默认执行带 `--require-conversation-stages` 的性能门禁。

TCP chat/heartbeat 模式需要认证令牌。隔离的性能场景应优先使用`--tcp-bootstrap-auth`：编排器会生成 256-bit 随机令牌、写入指定 Garnet 的临时缓存键，
再写入仅当前用户可读写的临时令牌文件。TCP 负载子进程只接收文件路径；令牌不会进入进程
参数、报告或日志，并在结束后删除缓存键和令牌文件。此模式要求 `--garnet-environment`，且不能与 `--tcp-token` 混用：

```powershell
--tcp-mode chat --tcp-bootstrap-auth --tcp-bootstrap-user-id 9300000000 `
--tcp-active-senders 100 --tcp-messages-per-second 0.8 `
--tcp-inactive-heartbeat-seconds 30 --tcp-delivery-drain-seconds 30 `
--tcp-slow-readers 5
```

在 bootstrap chat 场景中，`--tcp-slow-readers` 不再创建只能由慢连接代表的独立用户。
编排器保留每个 Gateway 分区前部的健康连接及主动发送者索引，并让分区末尾的 slow-reader
槽复用会被该分区 peer-ring 主动发送者命中的健康用户令牌。这样同一用户始终有一个健康、
可观测投递的连接，同时其额外 slow 连接也会收到真实 chat fan-out 并形成出站背压；总连接数
不变，唯一认证用户数为 `tcp-connections - tcp-slow-readers`。每个 Gateway 分区必须至少保留
两个健康用户，否则编排器会在启动依赖前拒绝配置。手工传入 `--tcp-token` 时不会自动重排
身份，调用方需要自行保证末尾 slow 槽复用一个有健康连接且会被主动流量命中的用户。

需要复用现有用户令牌时仍可传 `--tcp-token`；编排器同样会将它写入临时令牌文件，
不会作为子进程参数传递。编排器会把指定的 Garnet 连接字符串同时映射为 Gateway 的 `Redis__ConnectionString`，避免认证流量错误落到开发机的默认缓存。

`--tcp-messages-per-second` 是每个主动发送连接的速率；`--tcp-active-senders`
把已认证长连接规模与 durable chat 写入量解耦。例如 10,000 个连接、100 个主动发送者、
每发送者 0.8 msg/s 的目标总速率是 80 msg/s。正式 chat 基准还应显式记录
`--realtime-processing-concurrency`，并用 `--tcp-min-ack-ratio`、
`--tcp-min-delivery-ratio` 将脚本门禁下推给负载子进程，以便长测在语义退化时立即失败。
测量结束后 `--tcp-delivery-drain-seconds` 只停止新发送、继续接收末尾 ACK/投递；该时间不计入
吞吐分母。非主动发送连接由 `--tcp-inactive-heartbeat-seconds` 维持认证会话；这些 heartbeat
不进入 durable chat 消息、ACK、投递或吞吐计数。

长连接资源 A/B 可用 `--gateway-device-lease-refresh-seconds` 和
`--gateway-global-presence-refresh-seconds` 显式覆盖 Gateway 的 Redis 刷新 cadence；两项值会写入
`BenchmarkOptions`/最终 JSON，不能只靠目录名推断配置。它们不改变客户端 heartbeat 周期，且仍受
Gateway TTL 安全校验约束。

不要把“10,000 长连接”直接解释为“10,000 个用户各 5 msg/s 持续写入”。默认业务限流为
每用户 30 秒 30 条，且 50,000 msg/s 持续 8 小时会产生 14.4 亿条 durable 消息；这既不是
有效业务负载，也会把容量、磁盘和内存稳定性混成一个不可解释的失败。

## 固定速率容量曲线

脚本会为每个速率档创建全新的 NATS、PostgreSQL 和 Garnet 容器，运行编排器、汇总
JSON/Markdown，并在 finally 中只删除本次创建的容器。容器创建与启动分成两个阶段；
创建成功后会先登记名称和本轮唯一标签，因此端口绑定等启动失败仍会精确清理，已不存在或
标签不属于本轮的同名容器不会被删除。默认使用 32 路 Pipeline 并发，
避免先撞到负载端 8 路闭环并发上限：

```powershell
.\tools\ChatApp.Performance.Orchestrator\scripts\Run-CapacityCurve.ps1 `
  -Rates 40,80,120,160,200 `
  -DurationSeconds 60 -WarmupSeconds 10 `
  -PipelineConcurrency 32 -TcpConnections 1000 -SkipBuild
```

负载模型是有界闭环节流：worker 变慢后实际注入速率会下降。报告同时保留目标速率、
实际速率和达成率；高档位的目标未达成不能解释为开放环形负载的服务端排队量。每档
持久链路基准会显式把 RealtimeServices 配置为 JetStream，确保全新 NATS 能创建 streams。
容量报告和 `run-manifest.json` 会记录实际生效的
`OutboxHintCoalescingWindowMs`，避免把不同合并窗口的结果误作同构 A/B。

## 分层性能验证（默认入口）

日常修改不要直接运行 8 小时 soak。统一入口把相同的逐消息正确性、跨 Gateway、资源覆盖率、
Outbox/JetStream 和 PostgreSQL 诊断门禁组合成固定档位：

| Profile | 典型用时 | 用途 | 是否可作为发布结论 |
|---|---:|---|---|
| `Smoke` | 1–2 分钟 | 脚本、依赖、基本 ACK/投递闭环 | 否 |
| `Change` | 3–5 分钟 | 每轮热路径修改，80/320 msg/s | 否 |
| `Capacity` | 6–10 分钟 | 合并前容量筛查，80/320/640 msg/s | 否 |
| `Candidate` | 约 30 分钟 | 发布候选资源趋势和退化筛查 | 仅候选筛查 |
| `Formal` | 8 小时 | 最终长期内存/WAL/稳定性证据 | 是 |

```powershell
# 日常默认；已完成 Release 构建时使用 SkipBuild
.\tools\ChatApp.Performance.Orchestrator\scripts\Run-PerformanceValidation.ps1 `
  -Profile Change -SkipBuild

# 合并前容量筛查
.\tools\ChatApp.Performance.Orchestrator\scripts\Run-PerformanceValidation.ps1 `
  -Profile Capacity -SkipBuild

# 只查看将要执行的精确配置，不启动容器
.\tools\ChatApp.Performance.Orchestrator\scripts\Run-PerformanceValidation.ps1 `
  -Profile Candidate -ConfirmLongRun -DryRun
```

`Candidate` 和 `Formal` 必须显式提供 `-ConfirmLongRun`。`Formal` 继续委托
`Run-Soak.ps1`，因此还必须满足冻结源码/规范包/.NET host 的 SHA-256 绑定；轻量档不能产生
`MemoryStable` 的长期结论，也不能替代正式发布证据。每轮根目录的
`validation-profile.json` 和容量报告中的 `ValidationProfile` 可防止后续汇总混用不同档位。
容量汇总会直接列出 `WAL/msg`、`WAL sync/msg`、`DB ops/msg`、managed allocation/msg、
Pending 索引读取/msg、Outbox 精确/恢复/完成调用数和 Conversation HOT 比例；日常 A/B 无需
再手工遍历每个子报告。短窗口的 FPI/JIT/启动成本占比更高，跨 Profile 比较时仍应以相同
连接数、速率、warmup 和 measurement 时长为前提。

`resource-sample-coverage` 按 process/container 的每条 series 分别计算，只统计协调后的
measurement phase（不含连接爬坡、稳定期和 chat drain），并将每条覆盖率限制在
0–100%。报告字段 `MinimumMeasurementResourceSampleCoveragePercent` 是所有关键 series
的测量期最小覆盖率；它不是“整个进程生命周期样本数 / 测量期预期样本数”。Soak 的
内存数据完整性门禁复用同一组逐 series 测量期覆盖率。

## 入站解析短时 A/B

`Run-InboundTransportAB.ps1` 使用相同配置依次运行 `Pipelines` 与 `DirectSocket`。默认负载为
1000 个认证连接、每连接 20 heartbeat/s；该短测禁用持久 Pipeline 负载，隔离入站解析差异。
门禁要求两轮均成功、零连接失败，且 DirectSocket 吞吐下降不超过 5%、p95/p99 不超过
允许的短测波动：

```powershell
.\tools\ChatApp.Performance.Orchestrator\scripts\Run-InboundTransportAB.ps1 `
  -DurationSeconds 60 -WarmupSeconds 10 `
  -TcpConnections 1000 -TcpMessagesPerSecond 20 -SkipBuild
```

结果写入独立的 `inbound-transport-ab-*` 目录。短测适合决定是否启用可回退的默认值；
长期稳定性和容量结论仍需运行 soak/capacity 测试。

## 依赖故障注入

脚本为每个场景创建全新的 NATS、PostgreSQL 和 Garnet 容器，在持久链路负载已经启动后
停止指定依赖，再重启并采样 `/ready`、JetStream pending 和 Outbox pending。JSON 报告
保留健康时间线、恢复/收敛时间、吞吐、延迟和明确失败数；finally 只清理本次创建的容器：

```powershell
.\tools\ChatApp.Performance.Orchestrator\scripts\Run-FaultInjection.ps1 `
  -Targets Nats,Postgres,Garnet `
  -FaultAfterSeconds 20 -FaultDurationSeconds 10 `
  -RecoveryWindowSeconds 60 `
  -PipelineOperationsPerSecond 80 -PipelineConcurrency 32 `
  -TcpConnections 1000 -SkipBuild
```

NATS 短断线使用 pause/unpause；PostgreSQL 和 Garnet 使用 stop/start。场景只有在
服务恢复、积压回落且持久链路零失败时才返回成功。短窗口可用于验证工具，
正式结论应使用固定镜像、足够长恢复窗口并保存版本化报告。
## 无数据库密钥的基础烟测

此模式只验证服务编排、NATS 连接、TCP 连接负载、资源采样和报告闭环，不测持久化性能：

```powershell
dotnet run --project .\tools\ChatApp.Performance.Orchestrator -c Release -- `
  --no-pipeline `
  --smoke-noop-storage `
  --warmup-seconds 1 `
  --duration-seconds 5 `
  --tcp-mode connection `
  --tcp-connections 20
```

烟测模式要求 `/live` 成功并确认 `chatapp_nats_connection_connected=1`；它不会降低正式
模式的 readiness 标准。

## 报告与失败判定

每次运行创建独立的 `benchmark-yyyyMMdd-HHmmssZ` 目录，其中包含：

- `benchmark-report.json` 与 `benchmark-report.md`；
- 各子进程 UTF-8 stdout/stderr 日志；
- 每个 Gateway 的 TCP JSON/Markdown 报告；
- 持久链路 JSON/Markdown 报告；
- 进程 CPU、工作集、私有内存、线程、句柄；
- Docker CPU、内存、网络和块 I/O；
- Prometheus 前后值及增量、统一吞吐/错误率/p50/p95/p99。

任一负载非零退出、服务提前退出、子报告缺失、资源采样失败或启动依赖不健康，统一
报告都会标记为 FAILED。5 秒烟测只能验证流程，不能作为容量或版本回退结论。
