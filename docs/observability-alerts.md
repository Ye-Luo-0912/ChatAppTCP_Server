# 可观测性与告警基线

本文给出首轮生产告警阈值。它们用于尽早发现连接中断、消费积压和持久化链路停滞，
不是最终容量结论。2026-07-20 的本机 30 分钟基准未触发积压/失败阈值；完成
8–24 小时 Linux 浸泡与三副本 JetStream 硬重启已经通过；生产真实流量上线后仍需继续校准。

## NATS 与 JetStream

Prometheus 会把 Meter 名称中的点转换为下划线，并为 Counter 添加 `_total`。同一指标
还包含 `otel_scope_name`、`otel_scope_version`，JetStream 指标另按 `consumer` 区分。

| 信号 | 初始阈值 | 级别 | 处理重点 |
| --- | --- | --- | --- |
| `chatapp_nats_connection_connected` | 连续 30 秒为 0 | Critical | NATS 可达性、服务端集群和客户端重连日志 |
| `chatapp_nats_connection_reconnect_failures_total` | 5 分钟增加至少 3 | Warning | DNS、网络抖动、认证和服务端容量 |
| `chatapp_nats_messages_dropped_total` | 5 分钟有任何增长 | Critical | 本地订阅缓存和慢消费者；这表示客户端已丢弃消息 |
| `chatapp_nats_slow_consumers_total` | 5 分钟有任何增长 | Warning | 消费处理耗时、并发度和队列容量 |
| `chatapp_nats_server_errors_total` | 5 分钟有任何增长 | Warning | 权限、Subject、JetStream 配额和服务端错误类型 |
| `chatapp_jetstream_redeliveries_total / chatapp_jetstream_deliveries_total` | 5 分钟比例大于 1%；大于 5% 升为 Critical | Warning | 消费异常、ACK 超时和依赖写入延迟 |
| `chatapp_jetstream_pending` | 超过 1000 且连续 10 分钟增长 | Warning | 消费吞吐低于生产吞吐；按 consumer 定位 |
| `chatapp_jetstream_ack_duration_seconds` p99 | 5 分钟大于 2 秒；达到 `AckWait / 2` 升为 Critical | Warning | 处理链路、数据库事务和 ACK 调度 |
| `chatapp_jetstream_ack_failures_total` | 5 分钟有任何增长 | Warning | ACK/NAK/Terminate 网络失败 |

推荐的核心 PromQL：

```promql
max_over_time(chatapp_nats_connection_connected[30s]) == 0

increase(chatapp_nats_connection_reconnect_failures_total[5m]) >= 3

sum by (consumer) (rate(chatapp_jetstream_redeliveries_total[5m]))
/
clamp_min(sum by (consumer) (rate(chatapp_jetstream_deliveries_total[5m])), 1)
> 0.01

histogram_quantile(
  0.99,
  sum by (le, consumer) (rate(chatapp_jetstream_ack_duration_seconds_bucket[5m]))
) > 2
```

## 持久化与查询链路

| 信号 | 初始阈值 | 级别 |
| --- | --- | --- |
| `realtime_outbox_oldest_age_seconds` | 大于 30 秒；大于 120 秒升为 Critical | Warning |
| `realtime_outbox_pending` | 超过 1000 且连续 10 分钟增长 | Warning |
| `realtime_outbox_failures_total` | 5 分钟增加至少 3 | Warning |
| `realtime_messages_dead_letters_total` | 5 分钟有增长；超过 10 条升为 Critical | Warning |
| `realtime_history_failures_total / realtime_history_queries_total` | 5 分钟失败率大于 1% | Warning |
| `realtime_history_duration_milliseconds` p99 | 5 分钟大于 2 秒 | Warning |
| `realtime_history_queue_depth` | 连续 5 分钟高于配置容量的 80% | Warning |

`/ready` 当前是健康端点而不是 Meter。部署环境应由负载均衡器、Kubernetes 探针或
Prometheus blackbox exporter 每 10–15 秒探测；连续 2 分钟非 2xx 触发 Critical。

## Linux 监控部署

可部署的 Prometheus、Grafana、规则和仪表盘位于
[`deploy/observability`](../deploy/observability)。Linux 测试机已经验证 Prometheus
能够抓取真实 RealtimeServices `/metrics`；两个管理端口均只监听 `127.0.0.1`，应通过
SSH 隧道访问，而不是直接暴露到局域网。

2026-07-21 的 8 小时 Linux 浸泡完成 2,303,575 次 pipeline、0 失败，JetStream 和
Outbox 最终均无积压；Gen2 增量 59、Npgsql 使用连接峰值 14。2026-07-22 的三副本
JetStream 单节点硬重启产生 1 次断线与 1 次自动重连，pipeline 4,816/4,816 成功，
最终 pending=0。这些结果支持保留当前告警阈值，详细基线见
[Linux 韧性归档](performance-baselines/2026-07-22-linux-remote-resilience.json)。
## 仪表盘最小集合

1. NATS 在线状态、断线、重连失败、服务端错误和本地丢弃。
2. 按 consumer 展示 JetStream deliveries、redeliveries、pending、ACK in-flight 和 ACK p99。
3. Outbox pending、最老年龄、最大尝试次数、发布失败与 DLQ 增长。
4. History 吞吐、失败率、p95/p99、队列深度和 in-flight。
5. 进程 CPU、工作集、GC 分配、Gen2/LOH、线程池排队和 Npgsql 连接池。

告警规则进入生产前需要完成三项校准：排除部署启动宽限期、按实例/consumer 保留标签，
并用一轮故障演练确认告警能够触发和恢复，而不是只验证 PromQL 语法。

## 30 分钟本机校准结果

2026-07-20 在 8 路最大吞吐持久链路和 1000 个 TCP 长连接下，NATS 连接全程正常，
两个 JetStream consumer 最终 pending 均为 0；失败、重投、死信、丢弃和慢消费者
计数无增长。

Outbox 主动唤醒优化前完成 94,715 条链路，最终 pending 为 8、最老年龄 3.605 秒；
优化后完成 208,523 条，最终 pending 为 3、最老年龄 2.852 秒。两者均处于在途窗口和
5 秒采集周期内，远低于生产初始阈值。优化后 PostgreSQL 平均约占 5.72 核，说明
仪表盘必须同时展示数据库 CPU、I/O、连接池和 History 延迟，不能只看消息积压。

该结果支持保留当前“持续增长/持续超限”告警条件，避免按瞬时在途窗口误报，但不支持
降低生产绝对阈值。完整本机数据见
[优化前基线](performance-baselines/2026-07-20-local-single-node-30m.json)和
[主动唤醒基线](performance-baselines/2026-07-20-local-single-node-30m-outbox-signal.json)。