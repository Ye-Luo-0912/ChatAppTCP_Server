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
soak 将保持 80 msg/s，以便与 2026-08-06 的旧正式轮做稳定性对比；320 msg/s 是容量建议，
不是本次稳定性报告的负载参数。正式轮启动后将在本节补充冻结快照、PID、报告目录和最终
verdict。
