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
  5 分钟 120/s 持续确认、短期门禁和初始运行预算。