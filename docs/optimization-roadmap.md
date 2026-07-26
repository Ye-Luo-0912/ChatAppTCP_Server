# 优化与功能路线图

## 当前完成的工程基线

- .NET 10 / SDK 10.0.301（与 Server、RealtimeServices 对齐，见 `docs/sdk-baseline.md`）、
  集中依赖和严格分析器构建。默认发布为 **JIT + TieredPGO**（`PublishAot=false`）；
  协议 JSON 走源生成 `JsonSerializerContext`，便于日后重新启用 Native AOT。
- Pipe 背压、连接/速率/鉴权/空闲/发送超时和慢消费者隔离。
- 出站帧池化、用户会话快照和“条数 + 字节数”双重队列预算；全局入站预算
  （`GlobalInboundBudget`）覆盖 Pipe 暂存与 lane 复制 payload。
- JSON 编解码通过接口隔离，为后续二进制协议保留扩展点。
- TCP 网关已接入同级 RealtimeServices 的 NATS/JetStream 集成模块。
- 消息幂等持久化、事务 Outbox、每网关 durable consumer 和 ACK/NAK 已完成。
- 设备送达与用户已读回执已通过独立 JetStream Subject 接入。
- 回执状态与 Outbox 原子提交；已读隐式包含送达，重复/乱序不回退。
- 历史消息已使用 Core NATS request/reply 接入，不占用持久写入流。
- 历史分页使用 (UserId, ReceivedAtMs DESC, MessageId DESC) 复合索引和
  keyset cursor，不使用 OFFSET。
- TCP 已提供 MessageHistoryRequest(106) / MessageHistoryPage(107)；网关只采用
  已认证会话的 UserId。
- 单页最多 100 条，响应软预算 64 KiB（`WireResponseSoftLimit`）、硬上限
  `MaxPayloadSize`（80 KiB），查询工作器为有界并发。
- 群聊成员管理、撤回/编辑/反应、附件生命周期事件、Push Token 注册等协议面
  已部分落地；持续扩展时优先拆分 `TcpGatewayService` / `RealtimeEventDispatcher`。
- 真实跨进程探针已覆盖消息、已读回执、Outbox 和历史查询完整闭环。
- 已新增持久消息全链路负载工具，输出固定内存直方图及 JSON/Markdown 报告。
- History 查询队列深度和执行中数量已纳入运行时快照与 Meter 指标。
- NATS 连接/断线/重连失败、本地消息丢弃和慢消费者已纳入 Meter 指标。
- JetStream pending、delivery/redelivery、ACK in-flight、ACK 延迟和失败已按 consumer 采集。
- 隔离的真实 NATS 断线恢复演练已验证连接状态 1 -> 0 -> 1 和自动重连计数。
- 多进程编排器已统一启动/清理服务、执行双负载、采样进程与 Docker、输出指标前后差值。
- TCP 负载工具已补齐不包含令牌明文的 JSON/Markdown 结构化报告。
- 编排器的自动化 TCP 临时鉴权使用随机令牌和仅用户可读写的令牌文件；令牌不进入进程
  参数、报告或日志，测试结束自动删除令牌文件和缓存记录。
- 性能门禁在配置 TCP 负载时强制校验全部预期连接成功且 0 失败；Linux 双 Gateway
  安全组合回归已验证 pipeline 1,368/1,368、TCP 40/40、p95 101 ms。
- 首轮本机 30 分钟正式基线已通过：1000 个 TCP 长连接全部成功，持久链路
  94,715/94,715 成功、52.61 pipeline/s、p95/p99 437.5/441.5 ms，消息积压最终归零。
- 基准报告已补齐进程与容器内存起止/增量，并保存脱敏版本化 JSON 和 10% 同机复核线。
- Outbox 已实现容量 1 的事务提交主动唤醒并保留 200 ms 跨实例/恢复兜底轮询；
  相同 30 分钟 A/B 吞吐提升 120.2%，完整链路 p95/p99 降低 71.9%/62.6%，0 失败。
- History 已实现收件人/发件人索引分支有界 Top-N；真实短测数据上的扫描行从 1,535
  降至 11、缓冲页从 1,162 降至 17、执行时间从 6.055 ms 降至 0.071 ms。
- 固定速率容量曲线与 5 分钟确认已完成：目标 120/s 时持续实际 115.35/s、
  34,626/34,626 成功、p95/p99 174/214 ms；初始单节点运行预算为 80/s。
- 故障注入工具与首轮短测已完成：Garnet 568/568、PostgreSQL 修复后 575/575；
  PostgreSQL 短停不再终止宿主；NATS pause/unpause 短断线 513/513、最终积压归零。

## 下一轮主目标：全链路性能与稳定性门禁

这轮完成后再进入大规模业务功能扩展。当前不应凭感觉继续微优化；先建立可重复的
性能基线和故障验收，才能判断 JSON、数据库、NATS 或 TCP 哪一层是真正瓶颈。

第一阶段已完成：现有 TCP 负载工具负责连接、心跳、扇出和慢消费者，新工具独立测量
消息写入、Outbox、已读回执和历史查询的持久链路，并生成机器可读报告。5 秒短跑仅用于
验证工具和指标闭环，不作为容量结论；执行规范与当前烟测结果见 `performance-baseline.md`。

### P1：多进程基准场景

已完成：一键编排器可启动并采样多个 TCP 网关和 RealtimeServices，把 TCP 与持久
链路负载、显式 NATS/PostgreSQL/Garnet 容器、Prometheus 前后快照纳入同一报告。

Outbox 尾延迟优化已经完成并通过相同 30 分钟 A/B。优化后 208,523 条完整链路全部
成功，吞吐 115.84 pipeline/s，p95/p99 为 123/165 ms。Gateway 和 RealtimeServices
工作集仍稳定，因此当前不应优先改 TCP 热路径或把 JSON 替换为二进制。

History SQL 的两个分支扫描放大已经消除。固定速率曲线显示 120/s 可在 5 分钟内
持续达到 115.35/s 且 0 失败；高档位存在明显非单调抖动，因此 252.08/s 只作为短时
峰值，不作为容量承诺。下一步转向依赖故障注入和恢复收敛验证。

待完成：

1. 用 `Run-ConversationCombo.ps1` 在 Linux 正式机复跑：会话历史翻页 + 列表/SyncBootstrap
   与 TCP chat 扇出/慢消费者并行；校准会话阶段 p95 阈值。
2. 用生产近似的数据规模与资源限制复跑 8–24 小时浸泡，并校准告警阈值。
3. 将版本化 JSON 的短期门禁接入定时 CI；硬件或拓扑变化时重建基线。

验收标准：测试可一条命令重复执行；报告包含吞吐、端到端 p50/p95/p99、错误率、
CPU、工作集、分配、Gen2/LOH、TCP 排队字节、JetStream pending/重投和 Outbox pending。

### P1：可观测性与容量信号

已完成：

- Gateway 与 RealtimeServices 接入 OpenTelemetry Metrics/Tracing，并支持稳定版 OTLP 导出。
- W3C Trace Context 已贯穿 TCP 命令、NATS/JetStream、消费处理、PostgreSQL/Outbox 和回推事件；旧 JSON 事件仍可兼容反序列化。
- RealtimeServices 提供 Prometheus `/metrics`，JSON 快照迁移到 `/diagnostics/runtime`；Gateway 的预发布 HttpListener exporter 默认关闭。
- History 耗时/失败/队列深度、Outbox pending/最老消息年龄/最大尝试次数、运行时和 Npgsql Meter 已纳入采集。
- NATS 生命周期和 JetStream 投递/确认指标已接入；初始告警阈值见 `observability-alerts.md`。

下一步：

1. 已在 Linux 测试机部署 Prometheus、Grafana、初始告警规则与实时仪表盘，并用真实
   RealtimeServices `/metrics` target 验证采集成功；再用后续正式基准校准绝对阈值。
2. 在 OTLP Collector 中统一转发 Prometheus/Trace，并校验跨进程 Trace 查询体验。
3. 为 Alertmanager 选择并配置实际通知通道；在此之前告警规则只负责暴露触发状态。
4. 日志只保留结构化故障信息，避免在聊天热路径恢复高频 Info 日志。

验收标准：一次消息可关联 Gateway -> NATS -> RealtimeServices -> PostgreSQL/Outbox；
关键容量瓶颈能从指标中定位，而不依赖临时加日志。

### P1：故障与恢复测试

- 已完成 NATS Core 客户端的隔离断线/重连指标演练。
- 滚动重启网关和 RealtimeServices，确认 durable consumer、Outbox 和客户端去重。
- 短暂断开 JetStream、PostgreSQL、Garnet，验证超时、退避、重投、积压收敛和恢复后无静默丢失。
- 注入慢客户端和超大历史页，确认有界队列、字节预算和连接隔离。
- 校验同毫秒消息的游标翻页不遗漏、不重复，重复请求结果稳定。

验收标准：依赖恢复后自动收敛；内存不随离线时长或慢客户端数量无界增长。

### P1：CI 与发布门禁

- 已实现跨平台 `ChatApp.Performance.Gate`：对编排器报告做失败闭环检查，拒绝缺失
  JetStream/Outbox 指标的报告；Linux 8 小时原始报告已复验通过。
- Release 构建、全部测试和数据库契约检查进入 CI；Native AOT 发布为可选实验
  （默认关闭，见 `AGENTS.md`），以 JIT/TieredPGO 吞吐测试决定是否重新启用。
- 真实 NATS/PostgreSQL 探针进入集成测试；长时间压测和性能门禁进入 Linux 自托管定时任务。
- 保存基准结果、门禁结果并比较历史版本，性能退化必须有明确说明。
- .NET 11 稳定版发布后，与 Server/RealtimeServices **同步**升级 SDK/依赖并重跑基线
  （当前基线为 .NET 10，见 `docs/sdk-baseline.md`）。

## 当前执行顺序

1. 以 `Run-ConversationCombo.ps1` 与现有浸泡脚本在 Linux 正式机复跑，基于结果校准
   Prometheus 告警与会话阶段门禁阈值。
2. 注册 Linux 自托管 CI runner，并把 Release、测试、真实依赖探针、定时浸泡和性能门禁接入；当前仓库未配置 Git 远程，因此这是环境接入任务。
3. 配置 Alertmanager 的实际通知通道与 OTLP Collector 的 Trace 汇聚，完成从指标发现到告警响应的闭环。
4. 门禁稳定后继续拆分 oversized Gateway 类型，并按业务优先级扩展产品面。

## 性能门禁后进入的功能阶段

会话列表、未读、按会话历史、多设备 SyncBootstrap、群成员变更、撤回/编辑/反应与
附件生命周期事件契约已部分完成。

后续按业务优先级再评估：

1. 群聊产品面补齐与权限边界 hardening。
2. 推送通知端到端与附件元数据完整闭环。
3. 好友列表等其它 RealtimeEvent 的完整 TCP 产品面。
4. 将 `TcpGatewayService` / `RealtimeEventDispatcher` 拆成可独立测试的处理器模块。

## 二进制协议时机

开发阶段继续使用 JSON。只有全链路基准证明 JSON 是主要 CPU 或分配瓶颈后，再实现
二进制编码；升级必须通过协议版本或能力协商保留旧 JSON 客户端兼容性。
