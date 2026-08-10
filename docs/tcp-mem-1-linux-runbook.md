# TCP-MEM-1 Linux 真机执行清单与就绪自检

> 本清单只描述在 **Linux 真机**上运行 `scripts/Run-MemoryProfile.ps1` 的依赖、步骤与验收标准，
> 不夹带任何功能或默认值改动。执行前请先按「就绪自检」逐项勾选。

测量脚本链路：`Run-MemoryProfile.ps1` → 委托 `Run-CapacityCurve.ps1`（真实负载）→ 后台
`Performance-Gcdump.ps1` + socket 采集（测量中段并行取证）→ 汇总 `memory-profile-report.{json,md}`。

## 依赖概览（脚本真实引用）

| 依赖 | 用途 | 关键点 |
| --- | --- | --- |
| Linux x64 | 证据采集 | `$IsLinux` 分支才做 gcdump/`ss`/`/proc` |
| PowerShell 7+（`pwsh`） | 执行 | 使用 `$IsLinux`、`Start-Job`（PS7 特性） |
| .NET 10 SDK | 构建 | `global.json` 锁定 `10.0.301`，`allowPrerelease:false` |
| Docker CLI + daemon | NATS/PG/Garnet | 容器带 run-id 标签，脚本负责创建/清理 |
| `dotnet-gcdump` 全局工具 | gcdump | `dotnet tool install -g dotnet-gcdump` |
| `ss`、`pgrep`、`/proc` | socket/进程/PSS/fd | 需要可读 `/proc/{pid}/smaps_rollup`、`/proc/{pid}/fd` |
| 兄弟仓库 | Realtime | `Run-CapacityCurve.ps1` 要求 `<repo>/../ChatApp.RealtimeServices` 存在 |

## 一、就绪自检（逐项确认）

### 1. 主机基础
- [ ] Linux x64，内核支持 cgroup（检查 `/proc/net/sockstat`、`/proc/self/limits` 可读）。
- [ ] `pwsh --version` ≥ 7（不能用 Windows PowerShell 5.x）。
- [ ] `dotnet --version` 命中 `10.0.301`（`global.json` 锁定）。
- [ ] `docker info` 正常（daemon 运行中）。
- [ ] 文件句柄上限足够：**`ulimit -n 65535`**（软/硬都设）。脚本预检要求
      soft ≥ `ceil(10000/2)+1024 = 6024`，不满足会提前失败。
- [ ] 端口空闲（`ss -ltn` 确认无占用）：`18888`(Gateway)、`18080`(Realtime)、
      `4222`(NATS)、`18222`(NATS monitor)、`15432`(Postgres)、`16379`(Garnet)。
      若与开发环境冲突，用 `-NatsPort`/`-PostgresPort`/`-GarnetPort` 等改端口。

### 2. 工具与镜像
- [ ] `dotnet tool install -g dotnet-gcdump`（`Test-PerformanceGcdumpTool` 会检查）。
- [ ] 预拉取镜像（避免测量中途拉取失败）：
      `docker pull nats:2.10.26-alpine`
      `docker pull postgres:16.8`
      `docker pull ghcr.io/microsoft/garnet:1.0.84`

### 3. 源码与快照固定
- [ ] 本仓库已检出目标提交（`ChatAppTCP_Server`），记录源码 SHA-256。
- [ ] 兄弟仓库存在：`<repo>/../ChatApp.RealtimeServices`（`Run-CapacityCurve.ps1` 硬性要求）。
      记录其提交/clone 的 SHA-256。
- [ ] 已做一次 `dotnet restore --locked-mode`（各项目有 lock 文件；脚本用 `--no-restore` 构建）。

### 4. 可选严格快照绑定（推荐）
- [ ] 若启用：`CHATAPP_BENCHMARK_REQUIRE_SNAPSHOT_BINDING=1`，
      `CHATAPP_BENCHMARK_DOTNET_PATH=<绝对路径>`，
      `CHATAPP_BENCHMARK_DOTNET_SHA256=<该 dotnet 的 SHA-256>`。
      三者必须同时设置，否则脚本抛错。

## 二、执行步骤

### 第 0 步：先跑冒烟（验证链路，不产正式证据）
```bash
cd <repo>/tools/ChatApp.Performance.Orchestrator/scripts
pwsh ./Run-MemoryProfile.ps1 -Smoke
```
- 冒烟固定 500 连接、60s、单轮；应产出 `memory-profile-report.json/.md` 且 overall=PASSED。
- 冒烟会跳过 Linux 证据采集能力校验吗？不会——`-Smoke` 只改负载参数，`$IsLinux` 分支照常取证。
  确认 gcdump、`ss-tinm.txt`、`proc-net-sockstat.txt` 已落盘。

### 第 1 步：正式测量（默认：3 画像 × 3 轮 × 10 分钟）
```bash
pwsh ./Run-MemoryProfile.ps1
```
- 默认参数：`-Profiles silent,heartbeat,active -Repeats 3 -DurationSeconds 600 -TcpConnections 10000`。
- 每轮约 `ramp(10s) + warmup(30s) + measure(600s) ≈ 11 分钟`，9 轮 + 构建/依赖启动 ≈ **约 2 小时**。
- 若已用冻结源码预构建 Release 二进制，可加 `-SkipBuild` 缩短构建时间。

### 第 1a 步（可选）：只跑个别画像
```bash
pwsh ./Run-MemoryProfile.ps1 -Profiles silent -Repeats 3 -DurationSeconds 600
```

## 三、预期产物与位置

输出根：`<repo>/ChatAppTCP_Server/.artifacts/performance/memory-profile-<时间戳>/`

```
memory-profile-<stamp>/
├── memory-profile-report.json        # 汇总：OverallSucceeded、每轮 RunValid、Gateway 内存归因
├── memory-profile-report.md          # 人类可读归因表（PSS/RSS/HWM/cgroup peak/fd）
├── silent-1/  silent-2/  silent-3/   # 每画像每轮
│   ├── invocation-N.json             # 本轮调用 manifest（含 RunDirectory）
│   ├── benchmark-report.json         # capacity-curve 报告（含 ProcessResources→gateway-*）
│   └── evidence/
│       ├── gateway-1-pid<pid>.gcdump # 每 Gateway 一次 gcdump
│       ├── gateway-2-pid<pid>.gcdump
│       ├── ss-tinm.txt               # ss -tinm -p 原始输出
│       └── proc-net-sockstat.txt     # /proc/net/sockstat{,6}
├── heartbeat-1/ ...
└── active-1/ ...
```

## 四、验收标准

- [ ] `memory-profile-report.json`：`OverallSucceeded = true`；每轮 `RunValid = true`、
      `CapacityExitCode = 0`、`InvocationError = null`。
- [ ] 三类画像各 3 轮均出现在汇总（`Profiles` 含 silent/heartbeat/active，`Results` 9 行）。
- [ ] 每行之 `GatewayResources` 含 `gateway-*`，且 `MaximumPssBytes`、`MaximumVmRssBytes`、
      `MaximumVmHwmBytes`、`MaximumCgroupMemoryPeakBytes`、`MaximumFileDescriptorCount` 均非 0。
- [ ] 每轮 `Gcdumps` 数量 = GatewayCount（默认 2），`SocketSnapshot` 非空。
- [ ] 零连接失败/协议拒绝；吞吐与 p95/p99 不回退（对照 `roadmap-todo.md` TCP 长连接验收）。

## 五、常见问题排查

| 现象 | 处理 |
| --- | --- |
| 启动即报 open-file 不足 | 在启动 shell 里 `ulimit -n 65535` 后再跑 |
| `dotnet-gcdump 未安装` | 先 `dotnet tool install -g dotnet-gcdump` |
| `Realtime repository was not found` | 把 `ChatApp.RealtimeServices` clone 到本仓库同级目录 |
| NATS/PG/Garnet 预检失败 | 确认 4222/18222/15432/16379 未被占用；需 JetStream 启用 |
| 端口冲突 | 加 `-NatsPort`/`-PostgresPort`/`-GarnetPort` 等改用备用端口 |
| 未发现 Gateway 进程 | 确认 `pgrep -f 'ChatApp.TcpGateway.dll'` 能命中；进程须同用户可 attach |
| docker 拉取失败 | 先按第 2 节预拉三个镜像 |
| 汇总 md 无 gateway 行 | 检查 benchmark-report.json 的 `ProcessResources[].Label` 是否含 `gateway-*` |

## 六、完成后回填

把 `memory-profile-report.json`、`.md` 与各轮 `benchmark-report.json` 的路径、关键数字
（PSS/retained、fd 峰值、cgroup peak）回填到 `docs/roadmap-current-state.md` 的
TCP-MEM-1 条目，并在 `docs/roadmap-todo.md` 勾掉对应项。