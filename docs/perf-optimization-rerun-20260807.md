# TCP / Realtime 性能优化与跨 Gateway 容量复测（2026-08-07）

## 1. 结论

截图中列出的六项工作已完成代码修复或形成可执行结论。当前实现已启用跨 Gateway
路由验证、聚合授权查询、单条多目标 Outbox 事件和 6 小时已发布事件保留；本轮进一步
通过 Npgsql 原生 positional parameters、缓存 schema SQL 和减少 Outbox 临时对象，降低
Realtime 热路径分配。Linux 复测在 10,000 条 TCP 连接下通过 80/160/320/640 msg/s
四档跨 Gateway 曲线，未发现正确性或吞吐拐点，当前实测容量下界为 `>=640 msg/s`。

考虑 PostgreSQL 在 640 msg/s 时平均 CPU 已达到 `91.84%`，持续运行建议值为
`320 msg/s`，640 msg/s 只作为本环境短时能力上界，不能当作长期生产配置。

## 2. 原问题关闭状态

| 优先级 | 原问题 | 当前状态 | 证据 / 结论 |
|---|---|---|---|
| P0 | 460.8 万次事件全部走 `broadcast/no_pattern` | 已修复并复测 | Sharded Routing 已启用；本轮两个 load child 分别接入两个 Gateway，并在另一 Gateway 统计外部投递。四档跨 Gateway 曲线均为有效运行。 |
| P0 | 每条单聊消息约 5 次串行授权查询 | 已修复 | 直接消息授权合并为一次连接、一次 SQL 的授权快照读取；正式 8 小时旧轮的约 1,152 万次查询不再代表当前实现。 |
| P1 | 每条消息生成两个 Outbox/NATS 事件 | 已修复 | 语义允许时使用一条多目标 Outbox 事件，当前 5 分钟 A/B 每消息数据库操作由 `10.70` 降至 `10.29`。 |
| P1 | 已发布 Outbox 默认保留 7 天 | 已修复当前压力点 | 已发布记录保留期为 6 小时；pending/dead 仍保留并可诊断。按时间分区或 history 表属于更高数据规模下的后续演进，不是当前发布阻塞项。 |
| P2 | Realtime 总分配约 605.6 GB / 263 KB 每消息 | 已定位并优化 | 同负载 A/B 从 `127,756.48` 降至 `99,122.23 B/msg`，下降 `22.41%`；相对旧 8 小时报告的 `262,842 B/msg` 约下降 `62.3%`，但后者实现版本和运行窗口不同，仅作趋势参考。 |
| P2 | 尚未测出容量拐点 | 已完成本轮范围 | 80/160/320/640 msg/s 全部通过，实测下界 `>=640 msg/s`；资源拐点出现在 640 档的 PostgreSQL，持续建议 320 msg/s。 |

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
