# 版本化性能基线

本目录保存可进入版本控制的脱敏性能摘要，供同一硬件、依赖拓扑和负载参数下做版本比较。
完整运行日志和原始高基数 Prometheus 快照仍保留在 .artifacts/performance/，不提交仓库。

命名格式为 YYYY-MM-DD-环境-时长.json。新增基线必须包含环境、负载参数、吞吐、
p50/p95/p99、错误率、消息积压/失败计数、进程与依赖资源，以及明确的比较范围。
本机数据不能直接当作生产容量承诺；跨硬件或依赖拓扑的结果不得套用同一回退阈值。

当前基线：

- 2026-07-20-local-single-node-30m.json：Outbox 固定 200 ms 轮询的优化前基线；
- 2026-07-20-local-single-node-30m-outbox-signal.json：事务提交主动唤醒、
  200 ms 轮询兜底的优化后基线。两者均为 2 个 Gateway、1000 个 TCP 长连接、
  8 路持久链路并发和单节点 NATS/PostgreSQL/Garnet；
- 2026-07-20-local-single-node-capacity-curve.json：32 路并发的固定速率曲线、
  5 分钟 120/s 持续确认、短期门禁和初始运行预算；
- 2026-07-22-linux-remote-resilience.json：Linux 8 小时持久链路 soak 与 JetStream
  三副本硬重启恢复摘要；
- 2026-07-28-linux-inbound-transport-ab.json：Pipelines/DirectSocket 双 Gateway、
  1000 认证连接 × 每连接 20 heartbeat/s 的短时可回退默认值门禁。
- 2026-08-07-linux-cross-gateway-capacity.json：10,000 条 TCP 连接、1,000 个 active
  senders、跨 Gateway 的 80/160/320/640 msg/s 固定速率曲线，以及 SQL/Outbox 分配
  优化前后的 80 msg/s A/B。四档正确性门禁全部通过；实测容量下界为 640 msg/s，
  PostgreSQL 资源余量约束下的持续建议值为 320 msg/s。
- 2026-08-08-linux-cross-gateway-soak-8h.json：最终优化提交快照、10,000 条连接、
  100 个 active senders、80 msg/s 的正式 8 小时跨 Gateway 稳定性摘要；逐消息
  ACK/投递、内存稳定性、资源覆盖率及 Outbox/JetStream 边界均已审计。
