# 多进程性能基准编排器

该工具在一次运行中启动一个 RealtimeServices、一个或多个 TCP Gateway，并行执行 TCP
负载与持久消息负载。它按固定间隔采样子进程和显式指定的 Docker 容器，抓取
RealtimeServices 的 Prometheus 前后快照，最后生成统一 JSON/Markdown 报告。

编排器只终止自己启动的 .NET 子进程。`--docker-container` 只启用 `docker stats` 采样，
不会启动、停止或删除已有容器。

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
--tcp-messages-per-second 10 --tcp-slow-readers 5
```

需要复用现有用户令牌时仍可传 `--tcp-token`；编排器同样会将它写入临时令牌文件，
不会作为子进程参数传递。编排器会把指定的 Garnet 连接字符串同时映射为 Gateway 的 `，避免认证流量错误落到开发机的默认缓存。

## 固定速率容量曲线

脚本会为每个速率档创建全新的 NATS、PostgreSQL 和 Garnet 容器，运行编排器、汇总
JSON/Markdown，并在 finally 中只删除本次创建的容器。默认使用 32 路 Pipeline 并发，
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
