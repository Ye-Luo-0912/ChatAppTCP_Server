# TCP / Realtime 性能优化与跨 Gateway 容量复测（2026-08-07）

> 本文按时间追加：第 1–7 节是 2026-08-07 阶段快照，不代表后续默认值；当前实现与配置决策
> 以第 19–20 节和 [`NEXT-STAGE`](NEXT-STAGE.md) 为准。

## 1. 结论

截图中列出的六项工作已完成代码修复或形成可执行结论。2026-08-07 阶段实现已启用跨 Gateway
路由验证、聚合授权查询、单条多目标 Outbox 事件，并曾使用 6 小时已发布事件保留；本轮进一步
通过 Npgsql 原生 positional parameters、缓存 schema SQL 和减少 Outbox 临时对象，降低
Realtime 热路径分配。Linux 复测在 10,000 条 TCP 连接下通过 80/160/320/640 msg/s
四档跨 Gateway 曲线，未发现正确性或吞吐拐点，该阶段实测容量下界为 `>=640 msg/s`。

考虑 PostgreSQL 在 640 msg/s 时平均 CPU 已达到 `91.84%`，持续运行建议值为
`320 msg/s`，640 msg/s 只作为本环境短时能力上界，不能当作长期生产配置。

## 2. 原问题关闭状态

| 优先级 | 原问题 | 阶段状态 | 证据 / 结论 |
|---|---|---|---|
| P0 | 460.8 万次事件全部走 `broadcast/no_pattern` | 已修复并复测 | Sharded Routing 已启用；本轮两个 load child 分别接入两个 Gateway，并在另一 Gateway 统计外部投递。四档跨 Gateway 曲线均为有效运行。 |
| P0 | 每条单聊消息约 5 次串行授权查询 | 已修复 | 直接消息授权合并为一次连接、一次 SQL 的授权快照读取；正式 8 小时旧轮的约 1,152 万次查询不再代表当前实现。 |
| P1 | 每条消息生成两个 Outbox/NATS 事件 | 已修复 | 语义允许时使用一条多目标 Outbox 事件，当前 5 分钟 A/B 每消息数据库操作由 `10.70` 降至 `10.29`。 |
| P1 | 已发布 Outbox 默认保留 7 天 | 已修复阶段压力点 | 该阶段保留期降为 6 小时；当前默认已改为成功后 claim-token 保护的即时删除（`0` 小时），pending/dead 仍保留并可诊断。 |
| P2 | Realtime 总分配约 605.6 GB / 263 KB 每消息 | 已定位并优化 | 同负载 A/B 从 `127,756.48` 降至 `99,122.23 B/msg`，下降 `22.41%`；相对旧 8 小时报告的 `262,842 B/msg` 约下降 `62.3%`，但后者实现版本和运行窗口不同，仅作趋势参考。 |
| P2 | 尚未测出容量拐点 | 已完成该阶段范围 | 80/160/320/640 msg/s 全部通过，该阶段实测下界 `>=640 msg/s`；资源拐点出现在 640 档的 PostgreSQL，持续建议 320 msg/s。 |

## 3. 热点修复

本轮 allocation trace 显示 PostgreSQL SQL placeholder 解析、重复 schema SQL 拼接、
参数集合构造和 Outbox 单事件临时集合是主要可控热点。修复包括：

- 为固定 schema 缓存所有高频 SQL，避免每次访问属性都重新插值和分配字符串；
- 消息写入、直接会话序号、授权读取、幂等账本和 Outbox 写入改用 Npgsql 原生
  `$1..$n` positional parameters，减少命名参数解析；
- 缓存 Outbox 数组插入 SQL，并为单事件路径移除临时列表和循环；
- 修正单事件 wire copy，完整保留 `ProtocolVersion`、`AudienceVersion` 和
  `MinProtocolVersion`；
- 修复容量脚本：TCP-only chat/heartbeat 曲线现在把 `Rates` 作为聚合目标速率分配给
  active senders；正式 soak 通过显式开关继续使用“每个 sender 的速率”，避免语义串扰。

trace 中 SQL parser 相关采样从约 679 降到 110，约下降 `83.8%`；String 构造热点
采样从 578 降到 263。优化后仍可见 Npgsql 参数对象成本，若以后继续压缩分配，应优先
评估预编译命令或批量写入，但需先以正确性和实际端到端收益为门槛。

## 4. 同负载 A/B

环境为 Linux、10,000 条连接、100 个 active senders、80 msg/s 聚合目标、512-byte
payload、30 秒 warmup、300 秒 measurement、跨 Gateway 投递。每轮使用独立的
NATS/PostgreSQL/Garnet，报告门禁均通过。

| 指标 | 优化前 | 优化后 | 变化 |
|---|---:|---:|---:|
| 发送 / ACK / 投递 | 24,000 / 24,000 / 24,000 | 23,996 / 23,996 / 23,996 | 均为 100%，无重复或漏投 |
| Realtime allocation / msg | 127,756.48 B | 99,122.23 B | **-22.41%** |
| Realtime 平均 CPU | 4.563% | 3.963% | **-13.15%** |
| GC pause | 0.880852 s | 0.726756 s | **-17.50%** |
| 数据库操作 / msg | 10.70 | 10.29 | -3.83% |
| Realtime 最大 CPU | 9.700% | 7.899% | -18.57% |

工作集峰值受 5 分钟窗口内 GC 时点影响，本轮没有证据支持“工作集下降”，因此不将其
列为收益。优化后 trace 的启动阶段也包含 EventPipe 元数据噪声，结论以相同采集方式下的
分配总量、CPU、GC pause 和热栈变化为准。

## 5. 跨 Gateway 容量曲线

每档均为新依赖实例、10,000 条连接、1,000 个 active senders、30 秒 warmup、300 秒
measurement、60 秒 drain、512-byte payload、Realtime concurrency 16。所有档位连接
成功率和峰值连接率均为 100%，ACK/投递均为 100%，重复、漏投、死信和运行错误均为 0，
资源采样覆盖率均为 99.3%。

| 目标 msg/s | 实际 msg/s | 消息数 | ACK P99 | Realtime 平均 CPU | allocation / msg | DB ops / msg | PostgreSQL 平均 / 峰值 CPU | 结论 |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 80 | 79.98 | 23,998 | 1.728 ms | 4.29% | 103,281 B | 10.85 | 37.42% / 77.08% | PASS |
| 160 | 159.96 | 47,992 | 2.048 ms | 6.23% | 88,968 B | 8.96 | 51.76% / 108.93% | PASS |
| 320 | 319.92 | 95,984 | 3.328 ms | 8.60% | 85,391 B | 8.44 | 73.80% / 116.81% | PASS，持续建议值 |
| 640 | 640.14 | 192,058 | 3.968 ms | 9.99% | 61,542 B | 8.21 | 91.84% / 143.84% | PASS，依赖资源接近饱和 |

本轮没有在 640 msg/s 以内观察到应用吞吐或正确性拐点，但 PostgreSQL 平均 CPU 在
640 档已接近满载。下一轮若要寻找真实极限，应先扩展 PostgreSQL 资源或减少持久化往返，
再测试 800/960/1280 msg/s；否则只是测数据库排队，不代表 Gateway 自身容量。

## 6. 验证和可追溯性

- Realtime Release solution build：0 warning / 0 error；单元测试 `286/286`；集成测试
  `36/36`。
- TCP Release solution build：0 warning / 0 error；测试 `519 passed + 1 external Redis
  skipped`，0 failed。
- 优化后远端冻结目录：
  `/home/yeluo/chatapp-perf/runs/codex-tcp-opt-post-20260807T125816Z`。
- 当次 TCP 源码归档 SHA-256：
  `380df302b8ee4146090de6e810ab330eb6badea10b923d92665601c49f70c08a`；Realtime
  源码归档 SHA-256：
  `0a9eec4509c6de4f474a77efb7668e3cab1660cf2b112fc9b163b12bb7fcdf9d`。
- 隔离 .NET SDK 10.0.301 SHA-256：
  `763bfd4dbb1bb3a3b5257c6800eef77bb4abe2127e6ff9c33e2a56e2e814aedf`。
- 本地原始 A/B 报告：`.artifacts/trace-profile/baseline-80` 和
  `.artifacts/trace-profile/post-80`；优化后 nettrace 与 speedscope 文件保存在
  `.artifacts/trace-profile/post-80`。
- 本地完整容量报告：
  `.artifacts/remote-reports/capacity-curve-20260807/capacity-curve-20260807-130939Z`；
  可提交的脱敏摘要见
  `docs/performance-baselines/2026-08-07-linux-cross-gateway-capacity.json`。

## 7. 解释边界与后续正式稳定性验证

跨 Gateway 模式由两个 load child 分别连接一个 Gateway，外部投递由另一个 child 计数和
去重。因此全局发送/ACK/投递数量、重复和漏投判定有效；但跨 child 目前不汇总逐消息
delivery latency histogram，也不在一个 child 内做 ACK-ID 与 delivery-ID 集合相等校验。
本报告可以证明跨 Gateway 数量正确性和 ACK 尾延迟，不能把 delivery latency `0` 解读为
零延迟。

短时 A/B 和容量曲线不替代 8 小时内存稳定性验证。最终提交快照的跨 Gateway 正式 8 小时
soak 保持 80 msg/s，以便与 2026-08-06 的旧正式轮做稳定性对比；320 msg/s 是容量建议，
不是本次稳定性报告的负载参数。

新正式轮已于 `2026-08-07T13:57:37Z` 启动：

- 远端运行根目录：
  `/home/yeluo/chatapp-perf/runs/codex-tcp-soak-opt-20260807T135316Z`；
- 报告目录：`reports/soak-8h-cross-gateway-v3`；日志：
  `logs/soak-8h-cross-gateway-v3.log`；
- 主启动 PID/PGID：`1701156/1701156`；
- TCP 提交：`600141888c7a61b5f65866c6e9b6a80345a30391`；Realtime 提交：
  `d5e886a80f160b9a02c312a3383a705aa931097a`；
- 最终组合源码快照 SHA-256：
  `92c650d8fd2add8eca82000411bd2ab00eb8cc93bb5a15d6ebb28e564887a1ac`；规范包源
  SHA-256：`00823022224bc833ba1644d74a72b3e4a39ff6ab267c979f3b009fc76ddc6e4d`；
  .NET 10.0.301 host SHA-256：
  `763bfd4dbb1bb3a3b5257c6800eef77bb4abe2127e6ff9c33e2a56e2e814aedf`。

该轮已通过锁定还原和 Linux Release 构建（两套解决方案均 0 warning / 0 error），并使用
独立 NATS/PostgreSQL/Garnet、Realtime、两个 Gateway 和两个 load child。正式轮于
`2026-08-08T06:04:09+08:00` 退出，退出码为 `0`，最终 verdict 为 **PASSED**：
`RunValid=true`、`MemoryConclusive=true`、`MemoryStable=true`。

### 7.1 8 小时正确性与延迟

- 两个 child 的实际 measurement 分别为 `28,800.0164561s` 和 `28,800.0165649s`；
  10,000/10,000 连接成功并达到峰值，100 个 active sender 全部出现。
- 总计发送、MQ ACK、预期跨 Gateway 投递和实际收到均为 `2,304,000`；rejected、
  duplicate ACK、duplicate delivery、outstanding、tracking expired、tracking dropped、
  runtime failure 和漏投全部为 `0`，两个 60 秒 drain 均完成。
- 实际吞吐 `79.999954 msg/s`，目标达成率 `99.999942%`。
- Gateway-1 ACK 平均/P50/P95/P99/最大为
  `1.059/1.024/1.280/1.600/192.185 ms`；Gateway-2 为
  `1.019/0.992/1.280/1.600/193.669 ms`。
- JetStream measurement deliveries 和 ACK 均为 `2,304,000`，最终 pending 为 `0`，
  redelivery 为 `0`；Realtime persisted 为 `2,304,000`，死信为 `0`。

跨 child 的限制仍然存在：每个 child 在另一 Gateway 收到外部投递并做计数/去重，因而
全局数量和重复/漏投判定有效；但当前没有跨 child 合并 delivery latency histogram，也
没有在同一 child 内相关 ACK-ID 与 delivery-ID 集合。报告里的 `DeliveryLatency.Count=0`
表示“未采集该直方图”，不是零延迟。

### 7.2 Outbox、数据库与分配

- Outbox completed/published 为 `2,304,000`，即一条 Outbox 行/消息；相对旧轮
  `4,608,000` 行下降 `50%`。最终 pending/dead/max-attempts 为 `0/0/0`，全程 pending
  峰值为 `19`、attempts 峰值为 `1`，未形成积压；已清理发布历史 `579,021` 行。
- Sharded Routing 实际 target publish 为 `4,608,000`，即两次 NATS shard publish/消息。
  这是 sender ACK 与跨 Gateway recipient delivery 分属两个目标 Gateway 的结果。因此本轮
  完成的是“一条多目标 Outbox 行”，不能宣称实际 NATS target publish 也下降了 50%；若要
  再减半，必须重新设计可靠 ACK/投递协议语义。
- 数据库操作总数为 `23,492,608`，即 `10.1964 ops/msg`；相对旧轮 `14.27 ops/msg`
  下降约 `28.55%`，与将约 5 次串行授权读取聚合为一次 SQL 的预期相符。
- Realtime 总 managed allocation 为 `225,766,484,480` bytes，即
  `97,988.93 B/msg`；相对旧正式轮 `262,842 B/msg` 下降 `62.72%`，也略优于 5 分钟
  优化后 A/B 的 `99,122.23 B/msg`。GC pause 累计 `70.261s`。

### 7.3 采样、资源与内存稳定性

- 资源覆盖 8 条 series（5 个进程 + 3 个本轮依赖容器），每条 measurement 采样
  `14,291/14,400`，覆盖率 `99.243%`；Prometheus 覆盖率 `100%`。
- Gateway-1 基线/最终窗口 RSS 中位数 `305.92 → 264.48 MiB`，增长 `-13.55%`，
  最终斜率 `1.16 MiB/h`；Gateway-2 为 `309.70 → 265.13 MiB`、`-14.39%`、
  `1.36 MiB/h`。两者均低于 `20%` 增长和 `30 MiB/h` 斜率门限，独立判定
  `STABLE`，无重启、OOM 或 OOM kill。
- Realtime 平均/最大 CPU `4.92%/8.25%`，最大工作集 `217.29 MiB`。两个 Gateway
  平均 CPU `1.96%/1.94%`，最大工作集 `315.01/320.54 MiB`；两个 load child 平均
  CPU `1.20%/1.19%`，最大工作集 `121.19/125.92 MiB`。
- NATS/PostgreSQL/Garnet 平均 CPU 分别为 `14.07%/46.85%/5.12%`，最大内存分别为
  `316.0/953.8/844.5 MiB`。Garnet 峰值高于旧正式轮，但后半程回落到约 400 MiB，
  未显示持续增长；仍建议在更高连接数测试中单独观察其启动峰值。

### 7.4 残余优化信号与报告

本轮运行错误为 0，但 runtime 指标记录了 `2,034,057` 次已处理的
`TaskCanceledException` 和 `68` 次 `SemaphoreFullException`。它们没有导致消息失败、
重投或积压，也不影响本轮 PASS；但异常式控制流很可能仍贡献 allocation/GC，下一轮优化
应使用 trace 定位其调用栈，再决定是否改为无异常的取消/竞争路径。

- 本地完整报告：[`.artifacts/remote-reports/soak-8h-cross-gateway-v3`](../.artifacts/remote-reports/soak-8h-cross-gateway-v3)
- Verdict：[soak-verdict-20260807-220430Z.json](../.artifacts/remote-reports/soak-8h-cross-gateway-v3/soak-verdict-20260807-220430Z.json)，
  SHA-256 `00f65bd7cd9044584bb3c3581087ff5291794e499ef9b75828de96000fe75e1d`
- Benchmark：[benchmark-report.json](../.artifacts/remote-reports/soak-8h-cross-gateway-v3/capacity-curve-20260807-135738Z/rate-1/benchmark-20260807-135744Z/benchmark-report.json)，
  SHA-256 `cd746e07011066a71750882f598f94696323d578dc141a9b2dffc30a23da1f81`
- 可提交的脱敏摘要：
  `docs/performance-baselines/2026-08-08-linux-cross-gateway-soak-8h.json`

## 8. 报告后第二轮资源优化（2026-08-08，待 Linux 复测）

针对 8 小时报告残留的 `2,034,057` 次 `TaskCanceledException`、68 次
`SemaphoreFullException`、`97,988.93 B/msg` 和 `10.1964 DB ops/msg`，Realtime
源码已完成第二轮本地优化：

- 将 ACK 与 Outbox 的逐操作续租任务统一下沉到 `Infrastructure.Core` 共享租约层；每个
  worker runtime 只保留一个定时器和一个可索引最小堆，快速完成时立即移除并复用节点，
  不再为每条消息/每批 Outbox 创建 linked CTS、`Task.Delay` 或独立续租 Task；对象池上限
  为 4,096，版本令牌和 in-flight 状态阻止旧句柄影响复用节点。
- Outbox 唤醒改为原子单槽合并信号，删除 `CurrentCount -> Release -> catch
  SemaphoreFullException` 竞争路径。
- 消息写入的生命周期锁、tombstone 状态与幂等 canonical 合并为一条事务内 admission
  SQL，并移除每消息用户 ID 临时数组；直接消息授权的三次用户表探测合并为一次聚合扫描。
- Outbox 常见单行发布走精确 `claim_token` 更新快路径；多行路径按需分配结果集合；租约
  丢失后停止无效状态 SQL，由新所有者重试。
- typed chat payload 通过版本化、线程内两槽、64 KiB 上限的共享 UTF-8 缓冲写入 wire，
  不再创建 PayloadJson UTF-16 中间字符串。版本 token 使用单原子 CAS，旧租约不能释放新租约。
- Published 清理从 500×50 调整为 2,000×30，每分钟仍为有界 60,000 行，足以覆盖
  640 msg/s（38,400 行/分钟）并保留约 56% 余量，同时减少 DELETE 命令次数；6 小时保留
  和 pending/dead 可靠性语义不变。

本地 Release 构建为 0 warning / 0 error；Realtime 单元测试 `293` 项、集成测试 `37`
项（含 lifecycle race、消息幂等事务、Outbox claim-token/lease）将在本轮最终验证处记录。
新增 wire `ShortRun` 微基准在 512-byte content 下把 managed allocation 从 `3.88 KB`
降到 `2.09 KB/event`（约 `-46%`），耗时从 `2.331 us` 增至 `2.699 us`（约 `+16%`）。
因此该项定位为降低 GC 的明确取舍；异常消除、SQL 合并和清理批次调整能否使端到端 CPU、
GC、WAL/磁盘同时下降，必须由新的 Linux 容量曲线和 8 小时 soak 验证，不能沿用本报告
上一轮 PASSED 数据代替。

## 9. 第二轮 Linux 跨 Gateway 门禁（2026-08-08）

第二轮工作树以组合源码归档 SHA-256
`0e132e482e252156ac92baf3bd88a7ab0934a9906076ce622d69f828f23923cd`
冻结到 `/home/yeluo/chatapp-perf/runs/codex-tcp-shared-canary-20260808T073952Z`。规范包源与
.NET host SHA-256 分别保持
`00823022224bc833ba1644d74a72b3e4a39ff6ab267c979f3b009fc76ddc6e4d` 和
`763bfd4dbb1bb3a3b5257c6800eef77bb4abe2127e6ff9c33e2a56e2e814aedf`。SDK、NuGet 缓存与
规范包使用只共享文件内容的硬链接，源码、构建输出、报告和临时容器仍按本轮隔离，避免
为一次验证重复占用数 GB 磁盘。

10,000 连接、100 active sender、80 msg/s、120 秒稳定期、600 秒 measurement、60 秒
drain 的短时跨 Gateway 门禁退出码为 `0`，结果为 **PASSED / VALID**：

- 两个 Gateway 各成功建立 5,000 条连接；总计发送、MQ ACK、预期跨 Gateway 投递和实际
  收到均为 `48,000`，拒绝、重复 ACK、重复投递、漏投、outstanding、TTL-expired、
  tracking-dropped、死信和 runtime failure 全部为 `0`。
- 实际吞吐 `80.00 msg/s`，达成率 `100%`；两个 child 的 ACK P50/P95/P99 分别为
  `1.088/1.280/1.600 ms` 与 `0.992/1.280/1.472 ms`。
- 资源 measurement 覆盖率 `99.3%`，Prometheus 覆盖率 `100%`；JetStream pending
  始终为 `0`，Outbox pending 峰值仅 `2`、最终为 `0`，published/persisted 均为
  `48,000`。
- 上一轮遗留的 `TaskCanceledException` 与 `SemaphoreFullException` 指标不再出现；
  本轮唯一导出的异常 series 为 `ArgumentException`，增量为 `0`。五个受管进程 stderr
  均为空。
- 数据库操作从上一轮正式报告的 `10.1964` 降到 `8.9546 ops/msg`（`-12.18%`）；
  PostgreSQL 平均 CPU 从 `46.85%` 降到 `34.29%`。Realtime 平均 CPU 从 `4.92%`
  降到 `3.75%`。由于两个窗口长度不同，CPU 只作为回归趋势，最终以同快照 8 小时轮为准。
- Realtime managed allocation 为 `4,556,814,152` bytes，即 `94,933.63 B/msg`；
  相对上一轮正式报告的 `97,988.93 B/msg` 再下降 `3.12%`。GC pause 为 `3.60s`，短窗口
  包含启动/JIT，不能与 8 小时累计值直接同比。
- 末段 Realtime 工作集约 `168 MiB`，两个 Gateway 约 `299/302 MiB`；短测没有失控增长，
  但不据此宣称 `MemoryStable`，内存稳定性仍由正式 8 小时分窗门禁判定。

完整本地报告：
[`canary-cross-gateway-shared-v1`](../.artifacts/remote-reports/canary-cross-gateway-shared-v1)。
跨 child 的既有限制不变：全局 ACK/投递数量和重复/漏投判定有效，但 delivery latency
histogram 未跨 child 汇总，报告中的 `0 ms` 表示未采集，不表示零延迟。

## 10. 第二轮共享层正式 8 小时跨 Gateway verdict（2026-08-09）

本轮使用组合源码快照 `0E132E482E252156AC92BAF3BD88A7AB0934A9906076CE622D69F828F23923CD`，
运行根目录为 `/home/yeluo/chatapp-perf/runs/codex-tcp-soak-shared-20260808T080154Z`，
报告为 `soak-8h-cross-gateway-shared-v1`，容器标签为 `20260808080428z-1`。退出码为
`0`，正式 verdict 为 **PASSED**：`RunValid=true`、`MemoryConclusive=true`、
`MemoryStable=true`；两个 child 的 measurement 均为约 `28,800.03s`，10,000/10,000
连接成功，8 条资源 series 的 measurement 覆盖率均为 `14,291/14,400=99.243%`，
Prometheus 覆盖率为 `100%`。

### 10.1 ACK、跨 Gateway 投递与延迟

- 两个 child 各发送/ACK/预期投递/实际收到 `1,152,000`，合计 `2,304,000`；拒绝、
  重复 ACK、重复投递、漏投、outstanding、tracking 丢失、runtime failure 和死信均为
  `0`。child 吞吐分别为 `39.999953` 与 `39.999951 msg/s`，合计约 `80 msg/s`。
- ACK 延迟：gateway-1 平均/P50/P95/P99/最大为
  `1.0546/1.024/1.280/1.600/203.160 ms`；gateway-2 为
  `1.0133/0.960/1.280/1.536/195.314 ms`。
- JetStream measurement pending 为 `0`，ACK 为约 `2,304,001`，redelivery 增量为 `1`，
  但 child 级消息语义仍为全量成功；Outbox persisted/published 均为 `2,304,000`，
  pending/dead/max-attempts 为 `0/0/0`，清理历史发布行增量 `576,000`。
- 当前 harness 没有跨 child 合并 delivery latency histogram，也没有同 ID 的
  ACK-ID 与 delivery-ID 集合相关性；因此两个 child 的 `DeliveryLatency.Count=0` 表示
  **未采集**，不能解释为零延迟。全局数量、重复/漏投门禁仍然有效。

### 10.2 数据库、分配与异常式控制流

- Npgsql 操作计数增量 `18,899,854`，折合 `8.203062 ops/msg`；managed allocation
  增量 `207,105,393,144 bytes`，折合 `89,889.49 B/msg`。这两个值应与短时 canary
  的 `8.9546 ops/msg`、`94,933.63 B/msg` 分开看，正式轮包含完整启动、稳定和长尾窗口。
- 报告和五个受管进程 stderr 中均未出现 `TaskCanceledException` 或
  `SemaphoreFullException`；runtime exception series 只有 `ArgumentException`，增量为
  `0`。Realtime、两个 Gateway 和两个 load child 均无 OOM 证据；退出码 `137` 仅为
  orchestrator 在报告完成后的清理动作。

### 10.3 资源与 Gateway 内存趋势

- gateway-1 RSS 基线/最终窗口中位数 `296.93 → 272.16 MiB`，增长 `-8.34%`，末段
  斜率 `-1.95 MiB/h`；gateway-2 为 `304.11 → 267.67 MiB`，增长 `-11.98%`，末段
  斜率 `-0.52 MiB/h`。两者均 `Stable=true`，无重启、OOM 或持续增长。
- 平均 CPU：Realtime `4.51%`，Gateway-1/Gateway-2 `1.96%/1.94%`，load child
  `1.20%/1.19%`；NATS/PostgreSQL/Garnet 容器约 `14.02%/43.67%/5.16%`。8 条 series
  （5 个进程 + 3 个依赖容器）均满足 `>=90%` 采样覆盖门禁。

完整可复核报告：[`.artifacts/remote-reports/soak-8h-cross-gateway-shared-v1`](../.artifacts/remote-reports/soak-8h-cross-gateway-shared-v1)。

## 11. 644 GB 块写归因与第三轮数据库优化（2026-08-09）

第二轮正式报告中的 PostgreSQL Docker Block I/O 为 `2.24 GB / 644 GB`。该值是容器块设备
累计读写，不等于表大小、业务 payload 或 WAL；它同时包含 WAL、heap/index 脏页、checkpoint、
Vacuum 和临时 I/O。旧 harness 没有采集 PostgreSQL 原生统计，因此不能仅凭 `644 GB` 判定
真实业务数据膨胀。

本轮已给容量曲线加入 measurement 边界内的 `pg_stat_wal`、`pg_stat_database`、
`pg_stat_bgwriter`、表/索引大小与 tuple churn，并启用 `pg_stat_statements` 的 Top SQL
`wal_bytes`。Markdown 报告直接输出 WAL/消息、tuple 插入/更新/删除/消息、Outbox HOT 比例和
每条 SQL 的 WAL/call；Docker 表也修复为同时展示 net I/O 与 block I/O。下一次正式 Linux
运行可明确回答 `644 GB` 中 WAL、checkpoint 和表/索引各占多少，而不是继续以块设备总量猜测。

代码侧完成三项有安全边界的降写优化：

- PostgreSQL 单聊授权被合入消息写事务的 admission SQL，与生命周期共享锁和幂等 canonical
  共用一次连接/一次往返。只有生产 Npgsql 账本实例声明该能力；自定义存储和测试路径继续使用
  原授权链。拒绝结果仍保留 `blocked/privacy_rejected/not_friend/user_not_found` 语义。
- 迁移 56 删除仅供低频 `MAX(attempt_count)` 统计的 `ix_outbox_pending_attempts`，并将 Outbox
  fillfactor 设为 90。claim 更新不再因修改 `attempt_count` 强制重写索引，具备 HOT 条件；
  Pending 峰值很小时低频统计改扫活跃集合。
- `PublishedRetentionHours=0` 现在代表紧凑完成：NATS/JetStream 确认发布后，按
  `event_id + claim_token + Pending` 一次删除，不再生成 Published tuple 后再由清理任务删除。
  若进程在 NATS 确认后、删除前崩溃，租约到期仍以同一 EventId 重试并由 JetStream 去重；
  失败与 Dead 行完全保留。需要查询 Published 历史的环境可把该值设为正数恢复保留模式。
- 专项 120 秒报告显示 messages 索引增长约 `13.13 MB`、heap 增长约 `4.65 MB`。
  迁移 57 删除两个严格冗余热索引：非唯一 `(conversation_id, conversation_sequence)`
  已被同列唯一部分索引覆盖；单列 `target_user_id` 已被
  `(target_user_id, event_type)` 左前缀覆盖。唯一约束、历史/同步/retention、账号清理和
  Dead/Published 查询能力均保留。

本地两次同形状 20 连接、跨 Gateway、5 秒 smoke 均 `PASSED`。短窗口存在 measurement
边界误差，只用于验证方向，不替代容量曲线或 8 小时门禁：启用紧凑完成后，报告的 WAL/消息
由约 `8,221.6 B` 降至 `5,720.4 B`（指示值约 `-30.4%`）；claim SQL 的 WAL/call 从
`257.4 B` 降至 `44.3 B`，完成 SQL 从 Published UPDATE 的 `860 B/call` 降至
claim-token DELETE 的 `54 B/call`，并消除了 6 小时后的第二次 DELETE。后一次 smoke 的
Outbox HOT 比例为 `100%`。正式收益仍需在相同冻结快照的容量曲线和 8 小时运行中以原生 WAL
计数确认。

更长的 1,000 连接、100 active sender、80 msg/s、30 秒 warmup + 120 秒 measurement
专项运行同样 **PASSED / VALID**：发送、ACK 和跨 Gateway 收到均为 `9,602`，吞吐
`79.996 msg/s`，重复/漏投为 0。WAL 为 `66,104,955 bytes`，即 `6,884.5 B/msg`；
数据库操作 `7.7279 ops/msg`，managed allocation `88,253.6 B/msg`。PostgreSQL 平均/
最大 CPU `15.27%/26.45%`、最大内存 `113.4 MiB`，没有 checkpoint、temp bytes 或
deadlock。SQL 级 WAL 构成为 Outbox INSERT `36.34%`、messages INSERT `35.72%`、
会话投影 `11.54%`、幂等账本 `8.16%`、claim `3.66%`、完成 DELETE `0.79%`；Outbox
HOT 比例 `99.42%`。因此当前首要剩余成本是两份必要的 durable payload 写入及 messages
历史/同步索引，而不是发布完成状态或数据库 CPU 饱和。

验证状态：Realtime Release 构建 0 warning / 0 error，单元测试 `294/294`、完整集成测试
`38/38`（含新增 Outbox 紧凑完成 ownership 测试）；TCP Release 构建 0 warning / 0 error，
测试 `519 passed / 1 environment-skipped`，PostgreSQL 诊断端到端 smoke 已生成 JSON/Markdown。

## 12. PostgreSQL 容量曲线与处理并发对照（2026-08-09）

首次 `80/160/320/640 msg/s` 曲线固定为 100 个 active sender，与生产单用户
`30 秒 / 30 条` 滑动窗口冲突。80 档 `9,599/9,599` 通过；160/320/640 档实际投递
分别固定在 `11,984/11,993/11,996` 条，而发送为 `19,201/38,460/76,928`。这条
曲线证明了单用户反滥用限流生效，但不能用来判断数据库容量；完整报告保留在
`.artifacts/postgres-capacity-v1/capacity-curve-20260808-185619Z`。

修正曲线将 1,000 个连接全部设为 active sender，使 640 档仅为
`0.64 msg/s/user`，生产限流配置保持不变。容量拐点对照如下：

| Realtime 并发 | 档位 | 结果 | 发送 / ACK / 跨 Gateway 投递 | JetStream 末段 | PostgreSQL 平均 / 峰值 CPU | 关键结论 |
|---:|---:|---|---:|---:|---:|---|
| 4 | 80 | PASSED / VALID | `9,600 / 9,600 / 9,600` | `0` | `15.00% / 25.78%` | 持续低负载健康 |
| 4 | 320 | PASSED / VALID | `38,396 / 38,396 / 38,396` | `0` | `46.15% / 106.88%` | 持续 320 msg/s 有余量 |
| 4 | 640 | FAILED / VALID | `76,810 / 76,810 / 54,417` | 约 `22.9k` pending | `60.39% / 141.48%` | 4 个处理槽先饱和，不是连接池耗尽 |
| 8 | 640 | FAILED / VALID | `76,818 / 76,818 / 71,490` | 约 `5–6k` pending | `106.47% / 237.25%` | 投递提升到 `93.06%`，仍未达 95% 门禁 |
| 16 | 640 | PASSED / VALID | `76,813 / 76,813 / 76,813` | 首/尾/峰值均 `0` | `87.39% / 196.40%` | 640 msg/s 完整无积压通过 |

并发 4 的 160 档曾因一次外部重复投递在约 4 秒处 fail-fast，该轮
`RunValid=false`，不纳入性能结论。并发 8 同档重跑为 **PASSED / VALID**：
`19,198/19,198/19,198`，无重复/漏投门禁错误。偶发重复不会被忽略，新的正式
8 小时轮仍将以零重复为硬门禁。

### 12.1 WAL、fsync 和 SQL 写放大

- 并发 4 / 80 档：WAL `6,779.4 B/msg`，数据库操作 `7.716 ops/msg`，managed
  allocation `88,706.7 B/msg`；ACK/投递兼容直方图 P95/P99 为 `2.176/3.072 ms`。
  WAL 占比为 Outbox INSERT `36.00%`、messages INSERT `33.61%`、会话投影 `13.15%`、
  幂等账本 `8.36%`、claim `3.84%`、完成 DELETE `0.69%`。
- 并发 4 / 320 档：WAL `7,027.7 B/msg`，`5.637 ops/msg`，allocation
  `76,627.1 B/msg`，PostgreSQL 平均 CPU `46.15%`，跨 Gateway 投递全量成功。批量
  claim/完成使 DB 客户端操作数随吞吐提高而摊薄。
- 并发 8 / 640 失败档已持久化 `71,547` 条，WAL `593,583,798 bytes`；
  `42,816` 次 WAL sync 累计等待 `88.66s`。Outbox claim UPDATE 占 WAL `16.31%`，
  说明高并发下宽行 HOT tuple 复制仍是明显成本，但 Outbox 发布后 pending 仅
  数十条，不是发布器积压。
- 并发 16 / 640 通过档：WAL `536,066,702 bytes = 6,978.9 B/msg`，
  `52,468` 次 sync，平均 `1.334 ms/sync`；`5.269 ops/msg`，平均 DB 操作
  `1.166 ms`，allocation `74,356 B/msg`。无 checkpoint、temp file、deadlock，Outbox pending
  峰值 `144`、末值 `0`。当前 Windows/Docker 环境中，受控并发 16 能利用 PostgreSQL
  group commit 和多核并行，是比关闭 `synchronous_commit` 更安全的扩容手段。

### 12.2 `644 GB` 的最终解释与后续边界

`644 GB` 是旧报告的 Docker block-device 累计写入，不是数据库大小；而
`605.6 GB` / 后续 allocation 数值是 .NET 进程在 8 小时内的累计分配流量，也不是
常驻内存或硬盘占用。当前 80 档实测 `88.7 KB/msg`，按 80 msg/s 运行 8 小时
外推约 `204 GB` 累计分配；640 档因批处理摊薄为 `74.4 KB/msg`，同消息数约
`171 GB`。两者都较旧 644 GB 量级明显下降，但新正式轮的真实值必须直接由
8 小时 measurement 给出，不以短测外推代替。

窄租约表/大型 CTE 合并可继续降低 claim 宽行 WAL 和客户端往返，但会扩大崩溃
恢复与租约所有权的一致性面。在并发 16 已稳定通过 640 msg/s 的证据下，本轮
不冒险改变可靠性协议；先以 Linux 正式 8 小时轮验证长期 WAL、checkpoint、
allocation、零重复/漏投和内存稳定性。

## 13. 第三轮 Linux 正式 8 小时最终 verdict（2026-08-09）

第三轮已于 `2026-08-09T03:47:37Z` 正常结束，launcher、全部受管进程和后缀为
`20260808194057z-1` 的三个临时容器均由编排器清理。运行仍绑定组合源码归档
`13787B6A3B10E87CB6B3DD1BD5BBA67C65C7861914B503796F275915D1F33B89`、规范包归档
`00823022224BC833BA1644D74A72B3E4A39FF6AB267C979F3B009FC76DDC6E4D` 和 .NET host
`763BFD4DBB1BB3A3B5257C6800EEF77BB4ABE2127E6FF9C33E2A56E2E814AEDF`。容量 manifest
为 `completed / ExitCode=0 / RunValid=true`，最终 verdict 为 **PASSED**：
`RunValid=true`、`MemoryConclusive=true`、`MemoryStable=true`。

### 13.1 逐消息正确性、延迟与观测完整性

- 两个 Gateway 各成功建立 `5,000/5,000` 条连接。两个 load child 各发送、MQ ACK、
  预期跨 Gateway 投递和实际收到 `1,152,000` 条，合计均为 `2,304,000`；拒绝、重复
  ACK、重复投递、漏投、outstanding、tracking expired/dropped、runtime failure 和死信
  全部为 `0`。两个 child 吞吐为 `39.999923/39.999923 msg/s`，合计约 `80 msg/s`，
  吞吐达成率 `99.999807%`。
- ACK 平均/P50/P95/P99/最大延迟：gateway-1 为
  `1.0439/1.024/1.280/1.600/204.138 ms`，gateway-2 为
  `1.0320/0.992/1.280/1.536/206.481 ms`。JetStream delivery 与最终 ACK 均为
  `2,304,000`，pending 首/尾为 `0/0`、峰值 `1`；Outbox persisted/published 均为
  `2,304,000`，pending 首/尾 `0/0`、峰值 `25`，dead 最终/峰值均为 `0`。
- harness 仍不跨 child 汇总 delivery latency histogram，也不导出跨 child 的同 ID
  ACK-ID/delivery-ID 集合相关性；`DeliveryLatency.Count=0` 代表**未采集**，不代表零延迟。
  全局数量与每个 child 的重复/漏投硬门禁仍然有效。
- 8 条 measurement 资源 series（Realtime、两个 Gateway、两个 load child、三个容器）
  均为 `14,291/14,400=99.243%`，Prometheus 覆盖率 `100%`。五个 stderr 为空，报告
  未出现 `TaskCanceledException` 或 `SemaphoreFullException`；唯一 exception series
  `ArgumentException` 增量为 `0`。

### 13.2 资源、分配和 Gateway 内存

- gateway-1 RSS 基线/最终窗口中位数 `313.58 → 273.60 MiB`，增长 `-12.75%`、末段
  斜率 `-5.96 MiB/h`；gateway-2 为 `307.49 → 279.95 MiB`、`-8.95%`、
  `-3.05 MiB/h`。两者均独立判定 `STABLE`，无重启或 OOM。
- 平均/峰值 CPU：Realtime `4.17%/9.00%`，Gateway-1 `1.95%/5.98%`，Gateway-2
  `1.94%/6.16%`；PostgreSQL `42.09%/111.59%`、NATS `13.93%/45.99%`、Garnet
  `5.02%/44.74%`。Npgsql used connection 峰值 `11/100`，连接池不是 80 msg/s 下的瓶颈。
- Realtime managed allocation measurement 增量 `207,980,983,800 bytes`，即
  `90,269.52 B/msg`（约 `193.70 GiB` 累计分配流量）；相对第二轮正式轮的
  `89,889.49 B/msg` 波动 `+0.42%`，基本持平，相对最初 `262,842 B/msg` 已下降
  `65.66%`。该值不是常驻内存或硬盘占用。数据库客户端操作为 `16,711,276`，即
  `7.253158 ops/msg`，较第二轮 `8.203062 ops/msg` 再下降 `11.58%`。

### 13.3 `606 GB` 块写的原生归因

本轮 PostgreSQL Docker Block I/O 仍为 `2.09 GB / 606 GB`，只比旧轮的 `644 GB`
低约 `5.9%`；因此“容器块设备写计数”本身没有被压到业务数据量级。但 PostgreSQL 原生
计数已经证明它**不是 606 GB 的表数据，也不是 606 GB 的 WAL**：

- `pg_stat_wal.wal_bytes=21,976,258,655`，即 `9,538.3 B/msg`（约 `20.47 GiB`）；
  `wal_syncs=6,343,996`，同步等待累计 `23,859,987.53 ms`，平均
  `3.761 ms/sync`。`wal_buffers_full=0`，说明不是 WAL buffer 太小。
- 全部 `96` 次 checkpoint 都是定时触发，requested 为 `0`；checkpoint 写阶段累计
  `25,661,391 ms`、最终 sync 累计仅 `11,488 ms`。checkpoint、background writer 和
  backend 分别写出 `559,958 / 2,175,800 / 1,100,353` 个 8 KiB buffer，合计约
  `29.27 GiB`。原生 WAL 加这些逻辑页写约 `49.73 GiB`，远低于 Docker 的 `606 GB`；
  PGDATA 已挂载 Docker local volume，不能把差额简单归因于 OverlayFS。余量属于 cgroup/
  块设备聚合口径、文件系统 journal、WAL 段初始化和高频同步写造成的存储栈写放大，不能
  用于表示业务库大小；下一轮应同时采集原始 `io.stat`、`/proc/<pid>/io` 和宿主设备扇区。
- `temp_files/temp_bytes/deadlocks` 均为 `0`，没有临时文件、排序落盘或死锁；shared block
  hit rate 约 `99.45%`。数据库瓶颈是 durable WAL/fsync 和热页/index 写放大，不是连接池、
  临时磁盘或查询读盘。
- 最终核心表实际增长约 `6.09 GiB`：messages `5,545,918,464 bytes`（heap
  `2,097,340,416`、indexes `3,447,980,032`），幂等账本 `984,702,976 bytes`
  （heap `651,018,240`、indexes `333,479,936`），Outbox 仅 `2,334,720 bytes`。
  Outbox 插入/更新/删除各 `2,304,000`，HOT 更新 `2,303,513`，HOT 比例
  `99.9789%`，证明 fillfactor 与去除 attempt-count 索引已生效；紧凑完成也阻止了
  Published 历史表膨胀。

SQL 级 WAL 归因已经把下一步优先级收敛到少数路径：

| 路径 | WAL | WAL/call | 总 WAL 占比 |
|---|---:|---:|---:|
| messages INSERT | `13,764,832,421 B` | `5,974.3 B` | `62.64%` |
| Outbox INSERT | `3,576,944,815 B` | `1,552.5 B` | `16.28%` |
| conversation/member 投影 | `2,051,078,453 B` | `890.2 B` | `9.33%` |
| idempotency ledger INSERT | `1,495,143,779 B` | `648.9 B` | `6.80%` |
| Outbox claim UPDATE | `549,761,395 B` | `177.7 B` | `2.50%` |
| 单条/批量完成 DELETE | `149,096,557 B` | — | `0.67%` |

### 13.4 后续优化顺序与安全边界

1. **先消除 Outbox 全局 Pending 热扫**：当前实现已只写 canonical `payload_utf8`，
   `payload_json` 为 `NULL`，不存在双份 payload 落盘。真正的读放大来自
   `ix_outbox_pending` 与 `ix_outbox_pending_created`：8 小时分别读取约 `10.776` 亿与
   `6.160` 亿个索引 tuple，合计约 `16.936` 亿，而 Pending 峰值只有 `25`。优先增加
   “事务提交后投递 event id 到有界共享队列”的快路径，publisher 按 id 小批认领；队列满、
   进程崩溃或跨实例接管时仍由现有 `SKIP LOCKED` 扫描恢复。两个 Pending 索引不能直接
   同时保留或盲删，应以 retry/lease 场景的 `EXPLAIN (ANALYZE, BUFFERS)` 决定合并方案。
2. **验证并移除阻断 Conversation HOT 的全局列表索引**：`conversations` 只有 50 个
   活跃行却更新 `2,303,950` 次，HOT 更新为 `0`；`ix_conversations_last_message_list`
   在本轮扫描数为 `0`，并且当前用户列表 SQL 先按 members 的 `user_id/is_pinned` 过滤和排序，
   该全局索引不能直接覆盖完整 keyset。先用高会话数的混合读写数据验证查询计划；若确认
   无回归，`DROP INDEX CONCURRENTLY` 并给 conversations 预留 fillfactor，可降低会话投影
   `9.33%` WAL 中的显著部分。写入型 soak 的零扫描不能单独作为删除依据。
3. **批量化 claim/完成事务与连接租约**：当前约 `2.75 WAL sync/msg`，且 230.4 万事件
   产生 `309.4` 万次 claim 和 `191.7` 万次单条完成 DELETE。每个 publisher worker 可
   安全复用自己的非并发 Npgsql session/预编译命令，共享协调器合并唤醒并以 2–5 ms
   有界窗口批量 claim/完成；恢复扫描仍保留，不能在 publish 前删除 Outbox，也不能跨
   worker 并发共享同一个 `NpgsqlConnection`。
4. **优先审计 messages 索引，不盲删可靠性索引**：messages indexes 已是 heap 的
   `1.64x`。`received_at_ms, message_id` B-tree 支撑有序 retention keyset，BRIN 不能直接
   等价替换；历史、同步、唯一幂等索引只有在生产查询 telemetry、`EXPLAIN`、retention、
   离线同步和回滚测试全部通过后才能替换。成功消息若能由 messages 唯一键直接提供
   canonical 结果，幂等账本可演进为消息删除前才写入的冷记录或稀疏结果表，理论上可减少
   `6.80%` WAL 和约 `0.92 GiB` 表增长，但这是协议级改动，不能直接合并。
5. **再评估紧凑 Outbox 描述符**：Outbox 的 `16.28%` WAL 不是 JSON/UTF-8 双写，而是
   消息事件本身在 messages 与 Outbox 中各持久化一次。仅对 ChatMessage 事件存
   `message_id + routing`、发布时读取 messages 可降低 Outbox WAL，但会增加一次读取并引入
   retention/删除耦合；只有在消息行保证晚于发布删除、失败重放可验证时才值得 A/B。
6. **只做不牺牲 durability 的 PostgreSQL A/B**：保留 `synchronous_commit=on`、
   `full_page_writes=on`；评估 `wal_compression=lz4`，以及将纯 timed checkpoint 从 5 分钟
   延长到 10–15 分钟并同步提高 `max_wal_size`。目标是降低 `1,332,094` 个 full-page image
   和 checkpoint 后热页首次写成本，代价是更长崩溃恢复窗口，必须在正式配置门禁中量化。

完整本地报告：
[`soak-8h-cross-gateway-dbopt-v1`](../.artifacts/remote-reports/soak-8h-cross-gateway-dbopt-v1)。

## 14. 第四阶段极致优化实现与复测门禁（2026-08-09）

第 13.4 节的前三项高收益方案已经实现，但尚未用新的 Linux 正式 8 小时轮测量，不能把
下面的预期收益写成实测结果：

- **提交 ID 提示 + 精确认领**：业务事务提交后才把 `event_id` 放入容量 65,536 的有界
  共享队列，publisher 使用相同 Pending/retry/lease/claim-token 条件按 ID 领取。队列溢出、
  进程重启、跨实例写入和信号异常均由每 5 秒恢复扫描补偿；提示失败不会把已经提交的业务
  事务伪报为失败。空闲时也不再按 200 ms 发起空 Pending 扫描，而是直接等待恢复门限。
- **安全连接/命令复用**：每个 publisher worker 独占一个串行 Npgsql session，并复用两条
  prepared claim 命令；不跨线程、跨 worker 共享连接。异常时释放并重建，不把坏连接留在
  热路径。发布完成按最多 100 行或 100 ms 合并，NATS 投递不等待批次，仅数据库完成状态延迟；
  flush 门限小于 lease 的三分之一，崩溃仍由租约和 JetStream MsgId 去重恢复。
- **HOT 与索引写放大**：Migration 058 删除本轮扫描为 0 的
  `ix_conversations_last_message_list` 并设置 conversations `fillfactor=80`；Migration 059
  删除低频运维使用的重复 `ix_outbox_pending_created`。负责 retry/recovery 的
  `ix_outbox_pending(next_attempt_at_ms, created_at_ms)` 以及 Dead、Published 清理、目标用户和
  所有权索引全部保留。messages 的历史、同步、retention、唯一幂等索引没有依据写入型 soak
  的零扫描被盲删。
- **PostgreSQL 安全参数 A/B**：正式脚本默认启用 `wal_compression=lz4`、
  `checkpoint_timeout=900s`、`max_wal_size=4096MB`，并写入 invocation manifest/summary；
  `synchronous_commit=on` 与 `full_page_writes=on` 保持不变。目标是降低 1,332,094 个 FPI 与
  96 次 timed checkpoint 后的热页首次写，而不是用降低 durability 换性能。

本地验证已完成：Realtime Release 构建 0 warning/0 error，单元测试 `296/296`、PostgreSQL
容器集成测试 `42/42`；TCP Release 构建 0 warning/0 error，测试
`519 passed / 1 environment-skipped`；两个 PowerShell 入口的 AST 语法检查均已通过。

下一次正式轮必须同时对比：`wal_bytes/msg`、`wal_records/fpi`、`wal_syncs/msg`、Top SQL
WAL、claim/空 claim/完成调用数、两个 Pending 索引 tuple read（已删除索引应不存在）、
conversation HOT 比例、DB ops/msg、allocation/msg、GC pause、PG CPU/Block I/O、10,000 连接、
跨 Gateway 逐消息 ACK/投递、零重复/漏投/死信和 8 条 measurement 覆盖率。若这些门禁通过，
下一层才是 messages/幂等账本的协议级瘦身；它涉及 retention/replay 正确性，不纳入本轮。

## 15. 测试反馈环调整（2026-08-09）

后续优化不再每次启动 8 小时 soak。统一使用 `Run-PerformanceValidation.ps1`：修改后先跑
`Smoke`（20 秒测量，单 80 msg/s 档）或默认 `Change`（每档 45 秒，80/320 msg/s）；涉及
批处理、数据库或并发上限时跑 `Capacity`（每档 90 秒，80/320/640 msg/s）；功能和容量均
稳定后才跑 30 分钟 `Candidate`。8 小时 `Formal` 只用于发布候选、累计重大存储协议变更或
长期内存/WAL 结论复核。

各档仍使用两个 Gateway、跨 Gateway 逐消息 ACK/投递、零重复/漏投/死信、临时隔离依赖、
PostgreSQL 原生诊断和资源覆盖率门禁。区别仅在证据时间尺度：轻量档能快速发现正确性、
吞吐、SQL/WAL 和明显资源回归，但不得声明 `MemoryStable`。`Candidate`/`Formal` 需要
`-ConfirmLongRun`，Formal 额外强制冻结快照 SHA-256，避免误跑和报告混用。

## 16. 轻量反馈闭环与精确认领最终 A/B（2026-08-09）

本轮没有再次启动 8 小时测试。先用 `Change` 在约 3 分钟内完成 80/320 msg/s 两档，再用
约 55 秒的 `Smoke` 对精确认领 SQL 做同配置迭代。`Run-PerformanceValidation.ps1` 会为
非 Formal 档自动选择空闲端口，因此没有停止或改动本机已有的 NATS/PostgreSQL/Garnet；
首个端口冲突暴露后，外层脚本也已正确传播子进程失败退出码。`Candidate`/`Formal` 仍需
`-ConfirmLongRun`，避免开发阶段误启动长测。

### 16.1 Change 基线

两档均为 **PASSED / VALID**：1,000/1,000 连接、逐消息 ACK 和跨 Gateway 投递均为
`100%`，重复、漏投、拒绝、死信、deadlock 和 temp bytes 均为 `0`。

| 档位 | 实际吞吐 | ACK P95/P99 | WAL/msg | sync/msg | DB ops/msg | allocation/msg | Pending read/msg |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 80 | `79.97 msg/s` | `2.816/5.632 ms` | `6,055.1 B` | `2.033` | `6.078` | `82,518.8 B` | `1,135.3` |
| 320 | `319.76 msg/s` | `2.432/4.608 ms` | `5,816.8 B` | `1.062` | `5.487` | `80,253.5 B` | `23.8` |

该结果确认最新事务/完成批处理已经把第三轮正式报告的 `7.253 DB ops/msg` 压到约
`5.5–6.1`，但低速档的精确领取仍因资格谓词被 PostgreSQL 规划到 partial Pending 索引，
短短 3,602 条消息就读取 `4,089,506` 个 Pending index tuple。

### 16.2 主键锁定与物化资格校验

最终 SQL 将提示快路径拆成三个安全阶段：先仅按 `event_id` 主键 `FOR UPDATE SKIP LOCKED`
锁定行并读取资格字段；再在显式 `MATERIALIZED` 候选上验证 Pending、retry 和 lease；最后
只按主键执行 UPDATE。恢复路径仍按 `next_attempt_at_ms, created_at_ms` 使用 Pending 索引，
数据库 Outbox、claim token 和租约仍是权威，没有把可靠性转移到进程内队列。

最终 Smoke 报告为 **PASSED / VALID**：发送/ACK/跨 Gateway 投递均为 `1,600/1,600`，
达成率 `99.8%`，ACK P95/P99 `2.688/2.944 ms`，死信、重复、漏投、deadlock、temp bytes
均为 `0`。Conversation 更新 `100% HOT`。关键效率指标为：

| WAL/msg | sync/msg | DB ops/msg | allocation/msg | Pending read/msg | PK read/msg | exact/recovery/complete calls |
|---:|---:|---:|---:|---:|---:|---:|
| `6,326.7 B` | `2.092` | `6.109` | `82,608.4 B` | `6.067` | `2.000` | `1,571/6/168` |

相同 20 秒 Smoke 中，单层条件版本的 Pending read/msg 为 `37.746`；显式物化后再下降
`83.9%`。相对 Change 低速基线的 `1,135.3`，累计下降约 `99.47%`。WAL、DB 操作和
allocation 没有因 SQL 分层出现实质回归，延迟仍处于约 3 ms 内。容量报告现在会自动输出
上述指标，后续同配置 A/B 不再需要人工遍历子报告。

### 16.3 验证与停止条件

- Realtime Release 构建 `0 warning / 0 error`，单元 `296/296`、PostgreSQL 集成 `42/42`；
  精确领取租约语义和 worker-local prepared session 用例均通过。
- TCP Release 构建 `0 warning / 0 error`，测试 `519 passed / 1 environment-skipped`；三个
  PowerShell 入口 AST 解析通过。
- 低速时精确领取仍接近每条消息一次调用。进一步用每条消息 `Task.Delay` 凑批会引入大量
  timer/task 和额外投递延迟，不符合减少资源创建销毁的目标；要继续降低该调用数，应做
  协议级“事务写入批次/claim-on-insert”设计，并放到独立 Candidate 后评估，而不是继续
  扩大本轮热路径改动。
- 本节是分钟级变更反馈，只证明正确性和明显资源回归门禁；不声明 `MemoryStable`。累计
  内存、长期 WAL/checkpoint 和存储栈写放大仅在发布候选稳定后运行一次 Formal 验证。

最终轻量报告：
[`capacity-curve-report.md`](../.artifacts/performance-validation-final-materialized/validation-smoke-20260809-072708Z/capacity-curve-20260809-072708Z/capacity-curve-report.md)。

## 17. Outbox 事务内预领取与只读发布校验（2026-08-09）

第 16 节的精确认领虽然已消除全局 Pending 热扫，但新提交的单聊事件仍需在事务提交后执行
一次 `UPDATE ... RETURNING` 才取得发布租约。现在单聊热路径会在 Outbox INSERT 前从有界共享
协调器预留提示槽，并在**同一个业务事务**中写入 `locked_by/locked_until_ms/claim_token`；只有
事务提交成功后才发布 owner/token 提示。Publisher 使用 worker-local prepared `SELECT` 按
`event_id` 主键读取，并在物化结果上校验 owner、token、Pending、retry 和未过期租约，不再产生
额外 claim UPDATE、WAL 或 fsync。

安全边界保持不变：队列满时退化到第 16 节的精确认领；INSERT 冲突、回滚和 Dispose 会归还
预留槽；事务提交后提示丢失或进程崩溃时，数据库中的预领取行在租约到期后由恢复扫描重新领取；
发布完成仍以 `event_id + claim_token` 条件删除，过期 worker 不能删除已被恢复者重新领取的行。
协调器不缓存 payload，也不共享 Npgsql connection。owner、lease 和 claim token 作为一个不可变
配置原子发布；同一 publisher 生命周期复用一个 generation token，进程/worker 重新配置即换新
token，从而消除每消息 `Guid`/字符串创建而不削弱陈旧完成保护。

最终 20 秒 Smoke 使用 1,000 个连接、两个 Gateway、跨 Gateway peer-ring，结果为
**PASSED / VALID**：发送、ACK、投递 `1,599/1,599/1,599`，达成率 `99.8%`，P95/P99
`2.944/5.632 ms`，Outbox/JetStream 最终 pending、dead、DLQ、重复和漏投均为 `0`；全部资源
采样覆盖率为 `100%`。效率指标如下：

| WAL/msg | sync/msg | DB ops/msg | allocation/msg | Pending read/msg | exact/preclaimed/recovery/complete calls | Conversation HOT |
|---:|---:|---:|---:|---:|---:|---:|
| `6,078.4 B` | `1.117` | `6.115` | `84,309.1 B` | `6.375` | `0/1,578/6/169` | `100%` |

与第 16 节最终精确认领 Smoke 相比，claim UPDATE 调用由 `1,571` 降为 `0`，WAL/msg 下降约
`3.9%`，WAL sync/msg 下降约 `46.6%`；DB ops/msg 基本持平，因为写领取被一次只读主键校验
替代。初版只读 SQL 曾被规划到 Pending partial index，造成 `39.905 read/msg`；改为主键
LATERAL 命中并物化后，两次复测为 `2.469` 和 `6.375 read/msg`，恢复到仅由低频 recovery
扫描主导的量级。allocation 的短窗口结果在 `83.6–84.3 KB/msg` 间波动，不能证明显著下降，
但 generation token 已从代码层面移除每消息 GUID/string 创建。

验证门禁：Realtime Release 构建 `0 warning / 0 error`；预领取定向单元测试覆盖容量上限、
commit/cancel、owner/token 传递与 generation token 轮换；PostgreSQL 集成测试覆盖错误 owner/token、
租约到期和恢复领取；最终全量结果为单元 `298/298`、PostgreSQL 集成 `43/43`，TCP 为
`519 passed / 1 environment-skipped`，改动后的跨 Gateway Smoke 继续通过。该分钟级报告不声明
`MemoryStable`，长时结论仍只在发布候选稳定后执行一次 Candidate/Formal。

最终轻量报告：
[`capacity-curve-report.md`](../.artifacts/performance-validation-preclaim-shared-token/validation-smoke-20260809-100657Z/capacity-curve-20260809-100657Z/capacity-curve-report.md)。

## 18. Managed allocation trace 与不可变 SQL 共享层（2026-08-09）

在第 17 节之后使用隔离安装于 `.artifacts/diagnostics-tools` 的 `dotnet-trace`，对 Change
首个 80 msg/s 档的 Realtime 进程采集 20 秒 `gc-verbose`。采集覆盖连接稳定与消息测量窗口；
无 profile 的首个不完整 trace 已排除，不参与结论。有效基线共捕获 `1,247` 个
allocation-tick 样本，最大可控热点是每消息重新插值整段 SQL：

- `MessageWriteAdmissionReader.AcquireAsync` 导致 `79` 个 `String.Ctor` 样本；
- `ConversationWriteCommands.TryAllocateDirectSequenceAsync` 导致 `81` 个 `String.Ctor` 样本；
- messages INSERT 与幂等账本 INSERT 同样在热路径重新构造固定 SQL，并反复创建解析输入；
- 其余主要样本来自 Npgsql/NATS async state machine、参数对象、必要的 wire `byte[]` 和
  第三方 request/reply，不能通过跨线程共享 command/connection 安全消除。

实现新增 `RealtimeDatabaseSchema.GetOrAddCommandText`：缓存归属于具体 schema 实例，key 使用
ordinal 比较，value 为 `Lazy<string>` + `ExecutionAndPublication`。静态 factory 只在首次访问
执行；并发首次访问只物化一个最终 SQL；不同 schema 实例绝不共享表名。该层只共享不可变命令
文本，不共享 `NpgsqlCommand`、parameter、connection 或 transaction。首批接入 admission、
单聊序号分配、messages INSERT、幂等账本 INSERT/read 五条高频 SQL。

幂等账本成功路径同时移除了无用的 `INSERT ... RETURNING` DataReader：
`ON CONFLICT DO NOTHING` 直接用 `ExecuteNonQueryAsync` 的受影响行数区分首次插入与冲突；仅
冲突时再读取 canonical。canonical 不覆盖、同键并发、事务回滚和 retention 后防重语义均保持。

同配置 Change A/B：

| 档位 | allocation 基线 | SQL 缓存后 | 变化 | WAL 基线/缓存后 | 正确性 |
|---:|---:|---:|---:|---:|---|
| 80 msg/s | `82,533.3 B/msg` | `70,357.0 B/msg` | `-14.75%` | `5,824.0 / 5,821.1 B/msg` | ACK/投递 100% |
| 320 msg/s | `81,489.9 B/msg` | `68,972.3 B/msg` | `-15.36%` | `5,583.6 / 5,587.9 B/msg` | ACK/投递 100% |

相同 20 秒 trace 中，allocation-tick `1,247 → 1,026`（`-17.72%`），全部
`String.Ctor` `173 → 14`（`-91.91%`），admission/direct-sequence 的 `160` 个重复 SQL
字符串样本降为 `0`。这与端到端 allocation/msg 的下降方向一致，不依赖单一计数器噪声。

加入账本去 DataReader 后的最终 Smoke 为 **PASSED / VALID**：发送/ACK/跨 Gateway 投递
`1,600/1,600/1,600`，P95/P99 `2.56/3.20 ms`，WAL `6,068.4 B/msg`、sync
`1.119/msg`、DB ops `6.121/msg`、allocation `70,185.6 B/msg`、Pending index read
`6.224/msg`，Outbox exact/preclaimed/recovery/complete 为 `0/1,587/7/170`，Conversation
HOT `100%`；JetStream/Outbox 最终 pending、dead、DLQ、重复和漏投均为 `0`。Top SQL 已确认
账本语句不再含 `RETURNING`。

验证：Realtime Release 构建 `0 warning / 0 error`；schema cache 并发/隔离定向测试通过；
幂等账本五项并发集成定向测试通过；最终全量单元 `300/300`、PostgreSQL 集成 `43/43`。
下一层若要把约 `6.1 DB ops/msg` 继续压低，需要把 ledger/message 或 projection/message 合并为
数据修改 CTE/批次协议；这会改变并发冲突与返回行数边界，不在分钟级热路径微调中贸然实施。
剩余分配主要位于 Npgsql/NATS 状态机、参数和必要 payload byte[]，跨 worker 复用 command、
connection 或保留 payload 会引入并发/内存安全风险，因此本阶段停止继续池化。

报告与 trace：

- [`Change SQL cache`](../.artifacts/performance-validation-sql-cache/validation-change-20260809-102822Z/capacity-curve-20260809-102822Z/capacity-curve-report.md)
- [`最终 Smoke`](../.artifacts/performance-validation-sql-cache-ledger/validation-smoke-20260809-103444Z/capacity-curve-20260809-103444Z/capacity-curve-report.md)
- `../.artifacts/allocation-trace-current-v2/realtime-gc-verbose.speedscope.json`
- `../.artifacts/allocation-trace-sql-cache/realtime-gc-verbose.speedscope.json`

## 19. 单聊持久化合并与预领取恢复隔离（2026-08-09）

第 18 节最终 Smoke 的 `6.121 DB ops/msg` 已按 `pg_stat_statements` 拆清：每条单聊固定包含
admission、Conversation 序号分配、messages INSERT、Outbox INSERT、幂等账本 INSERT 五次数据库
调用，剩余约 `1.12` 次来自 Outbox 只读预领取、完成批次和低频恢复。继续微调连接池不能消除这五次
往返，因此本阶段先把无附件单聊的 messages、Outbox 和可选幂等账本合并为一个数据修改 CTE；
随后把 admission 与单聊序号分配合并为另一个命令，把五条固定热命令压缩为两条。

合并写入只共享由 `RealtimeDatabaseSchema` 缓存的不可变 SQL。每次调用仍创建并独占自己的
connection、transaction、command、parameter 和 scalar result；使用 PostgreSQL positional
parameter，避免 Npgsql 重复解析命名参数；以一个整数 bit mask 返回三张表是否写入，不再创建
DataReader。Outbox wire payload 序列化入口也收敛到一个 helper，避免两条写入路径漂移。
附件消息仍保留原有分步事务，因为最终事件必须包含已绑定的附件元数据。

可靠性边界保持不变：messages 冲突时 CTE 不会留下孤立 Outbox/ledger；Outbox 预领取仍在同一事务
内写入 owner、claim token 和 lease，提交后才发布提示；完成仍按 event id + claim token 删除。
新预领取行把 `next_attempt_at_ms` 设置为租约到期时间，使 ownerless recovery 的 Pending 索引扫描
不会在有效租约期间反复读取活跃行。预领取 worker 的主键只读校验不再套用 recovery 的
`next_attempt_at_ms <= now` 条件，但仍严格验证 Pending、owner、claim token 和未过期 lease；
租约到期后原有 recovery 路径仍可重新领取。

同配置 Change A/B 结果如下。基线是第 18 节 SQL cache 后、合并写入前的报告：

| 档位 | DB ops/msg 基线→合并后 | allocation/msg 基线→合并后 | ACK P99 基线→合并后 | WAL/msg 基线→合并后 | 正确性 |
|---:|---:|---:|---:|---:|---|
| 80 msg/s | `6.101 → 4.090` (`-33.0%`) | `70,357.0 → 64,575.7 B` (`-8.2%`) | `3.200 → 2.944 ms` | `5,821.1 → 5,837.0 B` | ACK/投递 100% |
| 320 msg/s | `5.557 → 3.773` (`-32.1%`) | `68,972.3 → 63,316.3 B` (`-8.2%`) | `17.408 → 4.352 ms` | `5,587.9 → 5,591.6 B` | ACK/投递 100% |

Top SQL 已确认单独的 messages、Outbox、ledger 三条 INSERT 被一条组合 CTE 取代；Conversation
HOT 为 `100%`，最终 pending、dead、DLQ、重复、漏投、deadlock 和 temp bytes 均为 `0`。
WAL 基本持平，说明本次收益来自减少数据库往返、command/parameter/state-machine，而不是关闭
durability 或少写业务数据。

将预领取行的 retry 时间移到 lease expiry 后，最终定向 320 msg/s Smoke 为 **PASSED / VALID**：
发送/ACK/跨 Gateway 投递 `6,395/6,395/6,395`，P95/P99 `2.944/6.656 ms`，DB ops
`3.710/msg`、allocation `63,920.3 B/msg`、WAL `5,688.7 B/msg`、sync `0.775/msg`；
Pending index read 为 `55.664/msg`，较修改前相同高吞吐短窗观察值 `129.072/msg` 下降约
`56.9%`。两者窗口不同，因此这里只作为方向性门禁，不替代 Candidate A/B。Outbox
exact/preclaimed/recovery/complete 调用为 `0/4,324/6/179`，最终队列清空。

在该轮报告基础上，Top SQL 显示 admission 与 Conversation allocation 仍各执行一次。最终实现把
生命周期共享 advisory lock、tombstone、直接消息授权、canonical ledger 读取与 Conversation/member
序号分配放入同一个 CTE 命令。`write_gate` 只有在生命周期 Active、授权 Allowed 且 canonical 不存在
时才允许数据修改 CTE 运行，所以重放、冲突、授权拒绝、冻结或注销路径不会推进序号；后续 bundle
仍是独立的第二条命令，保留客户端权威 wire serializer 和现有事务提交边界。没有通过跨消息共享
connection 或关闭 Npgsql reset 换取指标，16 个处理分区的并发隔离保持不变。

最终 320 msg/s Smoke 同样为 **PASSED / VALID**：6,400 条消息全部 ACK 并跨 Gateway 投递，
无拒绝、重复、漏投或 DLQ。与上一轮 320 Smoke 相比，DB ops `3.710 → 2.622/msg`
（再降 `29.3%`），allocation `63,920.3 → 62,571.2 B/msg`（再降 `2.1%`），WAL
`5,688.7 → 5,680.5 B/msg`、sync `0.775 → 0.688/msg`；Pending index read
`55.664 → 55.015/msg`，没有把往返收益转成索引热扫。两个 Gateway 的 ACK P95/P99 最大值为
`3.584/7.424 ms`，属于 20 秒短窗波动，后续 Candidate 才做尾延迟判定。Top SQL 已只剩
`6,400` 次 admission+sequence CTE 与 `6,400` 次 message+Outbox+ledger CTE 两条固定业务命令。
相对第 18 节 320 Change 基线，DB ops 从 `5.557` 累计降至 `2.622/msg`（`-52.8%`）。

开发中曾有一轮在设置未来 `next_attempt_at_ms` 后仍沿用 recovery 资格谓词，导致预领取 worker
读不到有效租约、投递为 `0`。该轮仅用于暴露语义缺口，已修复并明确排除：
`../.artifacts/performance-validation-preclaim-next-at/validation-smoke-20260809-110554Z/`。

最终验证：Realtime Release 构建 `0 warning / 0 error`；单元 `300/300`、PostgreSQL 集成
`44/44`。Outbox 租约集成用例覆盖错误 owner/token 拒绝、未来 retry 时间下的合法预领取、
到期前 recovery 不可领取及到期后重新领取；direct admission gate 用例确认幂等重放不重复推进
Conversation/sent_count，授权拒绝不创建会话、消息、Outbox 或账本。本节继续使用分钟级
Smoke/Change，不声明新的
`MemoryStable`；30 分钟 Candidate 和 8 小时 Formal 留到发布候选冻结后各执行一次。

报告：

- [`合并写入 Change`](../.artifacts/performance-validation-dml-bundle-final/validation-change-20260809-105931Z/capacity-curve-20260809-105932Z/capacity-curve-report.md)
- [`预领取恢复隔离 320 msg/s Smoke`](../.artifacts/performance-validation-preclaim-next-at-v2/validation-smoke-20260809-110853Z/capacity-curve-20260809-110853Z/capacity-curve-report.md)
- [`最终 admission+sequence 320 msg/s Smoke`](../.artifacts/performance-validation-admission-sequence/validation-smoke-20260809-112138Z/capacity-curve-20260809-112138Z/capacity-curve-report.md)

## 20. Outbox hint 合并窗口重复轻量 A/B（2026-08-09）

候选实现允许 Publisher 在提示批未满时等待 `0..50 ms` 后再读取一次队列；`0` 完全关闭该等待。
它不改变数据库 Pending、lease、claim token、恢复扫描或 JetStream 去重语义，也不共享 connection、
command、transaction、payload 或可变 session。配置校验和开关保留，但默认值必须同时满足资源收益
与尾延迟门禁。

在相同 Release 二进制、两个 Gateway、320 msg/s、20 秒 measurement 下，分别对 `0 ms` 和
`2 ms` 运行三次无干扰对照。所有有效轮次均逐消息 ACK/跨 Gateway 投递 100%，无重复、漏投、
outstanding、dead 或最终积压。中位数如下：

| 窗口 | 吞吐 | DB ops/msg | preclaim 调用 | ACK P99（GW1/GW2） | Delivery P95（GW1/GW2） | Delivery P99（GW1/GW2） |
|---:|---:|---:|---:|---:|---:|---:|
| `0 ms` | `319.478/s` | `2.79109` | `4,846` | `5.120/5.376 ms` | `24.576/24.576 ms` | `73.728/69.632 ms` |
| `2 ms` | `319.315/s` | `2.22276` | `1,232` | `6.144/6.656 ms` | `47.104/38.912 ms` | `98.304/98.304 ms` |

`2 ms` 的 DB ops/msg 和 preclaim 调用分别下降 `20.36%`、`74.58%`，但 Delivery P95/P99
明显退化，并有 `1/3` 轮同时出现两个 child 的 ACK P99 `47–49 ms`、Delivery P99 约
`393 ms`；`0 ms` 三轮没有同级尖峰。最终决定是默认保持 `0 ms`，使默认热路径不创建合并
delay timer；`2 ms` 仅作为部署方明确接受尾延迟风险时的资源优先选项，`3 ms` 也不得默认启用。

一轮 `0 ms` 与其他项目构建/测试可能重叠，已排除并补跑，不进入统计。容量报告和
`run-manifest.json` 现记录实际 `OutboxHintCoalescingWindowMs`，后续 A/B 不得混用配置。
本节仍是轻量配置门禁，不产生新的 `MemoryStable` 或发布 soak 结论。

有效报告：

- [`0 ms repeat 2`](../.artifacts/performance-validation-hint-coalesce-0ms-repeat2/validation-smoke-20260809-154227Z/capacity-curve-20260809-154227Z/capacity-curve-report.json)
- [`0 ms repeat 3`](../.artifacts/performance-validation-hint-coalesce-0ms-repeat3/validation-smoke-20260809-154332Z/capacity-curve-20260809-154332Z/capacity-curve-report.json)
- [`0 ms repeat 4`](../.artifacts/performance-validation-hint-coalesce-0ms-repeat4/validation-smoke-20260809-154454Z/capacity-curve-20260809-154454Z/capacity-curve-report.json)
- [`2 ms repeat 1`](../.artifacts/performance-validation-hint-coalesce-2ms-repeat1/validation-smoke-20260809-153340Z/capacity-curve-20260809-153340Z/capacity-curve-report.json)
- [`2 ms repeat 2`](../.artifacts/performance-validation-hint-coalesce-2ms-repeat2/validation-smoke-20260809-153445Z/capacity-curve-20260809-153446Z/capacity-curve-report.json)
- [`2 ms repeat 3`](../.artifacts/performance-validation-hint-coalesce-2ms-repeat3/validation-smoke-20260809-153553Z/capacity-curve-20260809-153553Z/capacity-curve-report.json)

## 21. 10k 长连接资源画像与刷新 cadence（2026-08-10）

本轮使用两个 Gateway、每实例 5,000 个已认证连接、全部连接 45 秒 heartbeat、20 秒稳定期和
60 秒 measurement。两轮 child benchmark 均建立 10,000/10,000 连接，连接失败、服务端关闭和
协议拒绝均为 0；每个 child 发送约 6,665–6,667 个 heartbeat，仅结束边界各有至多 1 个 ACK
未计入，P95/P99 为 `0.400–0.432 / 0.544–0.672 ms`。

| 配置 | Gateway 平均 CPU | 私有内存 ramp 增量上界 | max handles | max threads | Garnet 平均 CPU | Garnet 网络 |
|---|---:|---:|---:|---:|---:|---:|
| 30 秒 lease/presence 刷新 | `0.69/0.81%` | `31.97/33.48 KiB/连接` | `5,556/5,512` | `59/49` | `13.16%` | `16.7 MB / 3.4 MB` |
| 90 秒 lease/presence 刷新 | `0.85/0.89%` | `29.60/31.25 KiB/连接` | `5,583/5,505` | `59/48` | `13.21%` | `12.3 MB / 3.58 MB` |

私有内存增量是进程从早期 ramp 到 measurement 末尾的差值，包含 JIT、GC committed/free、
ArrayPool、native runtime 与缓存；它只是每连接上界，不能解释成 session 对象大小，也不能用两次
进程启动差直接宣称内存下降。handle 接近连接数而线程保持约 50，确认当前实现没有每连接线程。
下一阶段必须同时采 gcdump、PSS/smaps、`ss -tinm` 与 cgroup sock，才能把 managed retained、
进程 native 与内核 TCP buffer 分开。

90 秒 cadence 使用现有全局 bucket/有界 worker，没有增加每 session timer 或状态；配置校验要求
TTL 至少容纳三个实际扫描周期，使连续漏两轮刷新后仍有机会续租。该样本的 Garnet 入站网络下降
约 `26%`，但 CPU 均值未下降；60 秒窗口只覆盖有限刷新周期且包含认证/ramp，因此当前只证明网络
与理论命令频率下降，不宣称 CPU 收益。编排器现把两个 refresh interval 写进 report configuration，
后续同构测试可直接核对配置。

两份旧 capacity wrapper 因 Windows load timer 正偏差 `60.64–60.70s` 超出旧的 `60.6s` 上界而
标为 invalid，底层 benchmark 本身为 Succeeded/valid。门禁已改为严格 `>=99.5%` 下界和
`目标 + max(1 秒, 1%)` 上界，允许有界调度正偏差而不放过提前结束；本节只作为诊断证据，不把
旧 wrapper 重写成正式容量 PASS。

证据：

- [`30 秒基线`](../.artifacts/connection-resource-profile/authenticated-heartbeat-10k-v2/capacity-curve-20260809-195004Z/rate-1/benchmark-20260809-195010Z/benchmark-report.json)
- [`90 秒 cadence`](../.artifacts/connection-resource-profile/authenticated-heartbeat-10k-cadence90/capacity-curve-20260809-195521Z/rate-1/benchmark-20260809-195525Z/benchmark-report.json)
