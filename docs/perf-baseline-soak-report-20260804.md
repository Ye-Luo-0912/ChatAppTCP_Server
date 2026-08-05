# R2 基线 8h 浸泡测试报告 — DirectSocket + PersistentSendLoop + BoundedChannel

> 报告日期: 2026-08-05
> 运行窗口: 2026-08-04 20:36:58 → 2026-08-05 04:42:56（+08:00），duration=28800s + 300s warmup
> 结论: **FAILED**（内存稳定性未达标 + 负载侧未真正执行）

## 1. 测试配置

| 项 | 值 |
|----|----|
| 组合 | DirectSocket + PersistentSendLoop + BoundedChannel |
| 负载模式 | chat |
| 连接数 | 10,000（2 × Load Generator，各 5,000） |
| Payload | 512 B，5 msg/s |
| 实例 | Realtime(:18080) + Gateway-1(:18888) + Gateway-2(:18889) |

## 2. Soak Verdict

文件: `.artifacts/perf/soak-baseline/soak-verdict-20260804-204300Z.json`

| First MiB | Last MiB | Avg MiB | Max MiB | Growth MiB | Growth % | Last vs Avg % | Plateau |
|---|---:|---:|---:|---:|---:|---:|---:|
| 200.7 | 55.1 | 275.5 | 3278.4 | -145.6 | -72.6% | -80.0% | False |

- Threshold: Growth ≤ 20%、Last vs Avg 偏离 ≤ 15%
- **MemoryStable: `false`**（FAILED；Max WS 高达 3.2 GiB，且存在进程被杀）

## 3. 失败根因分析

本次浸泡**未能有效验证稳定性**，原因链条如下：

1. **Redis circuit breaker 打开（认证失败）**
   - Gateway 认证路径 `RedisResumeTokenStore.IssueAsync` 持续抛 `StackExchange.Redis.RedisException: Redis circuit breaker is open`（约 20:41–20:42 批量出现）。
   - 连接结果：`tcp-load-1: 649 succeeded / 4351 failed (87.02%)`；`tcp-load-2: 0 succeeded / 5000 failed (100%)`。
   - 根因：Garnet(Redis, :16379) 不可用或过载，熔断器打开，导致握手/认证大量失败。

2. **`invalid_self_chat` 死信泛滥（业务负载未真正执行）**
   - Realtime 侧 `realtime_messages_dead_letters_total{reason="invalid_self_chat"}` 与 `dotnet_exceptions_total{error_type="OperationCanceledException"}` 均达 **76,643,856**。
   - 说明 Load Generator 发送的是**发给自己的消息（self-chat）**，被 Realtime 判定为无效并全部丢弃——业务链路并未真正跑起来。

3. **进程被杀（OOM）**
   - `gateway-2` 退出码 **137**（SIGKILL），Max Working Set 达 3.2 GiB，疑似负载/资源异常触发 OOM。

> ⚠️ 由于连接与业务负载均未正常建立/执行，本次 verdict 的「内存不达 STABLE」**不具备稳定性判定参考意义**，需先修复下述问题后重跑。

## 4. 建议修复项

| 项 | 说明 |
|----|------|
| Redis/Garnet 可用性 | 检查 :16379 容器存活与连接上限，消除熔断器打开；浸泡脚本应在启动后确认 Redis 就绪再拉起负载 |
| self-chat payload | 修正 Load Generator 的 chat 消息目标，避免发送给自己触发 `invalid_self_chat` 死信 |
| OOM 复现 | 结合 gateway stderr 与 Max WS 3.2 GiB 定位具体进程与分配热点 |

## 5. 原始报告位置（gitignored，仅本地/Linux 保留）

- Soak 日志: `/home/yeluo/chatapp-perf/soak-baseline-20260804-123649Z.log`
- Verdict: `.artifacts/perf/soak-baseline/soak-verdict-20260804-204300Z.{json,md}`
- Benchmark: `.artifacts/perf/soak-baseline/capacity-curve-20260804-123649Z/rate-1/benchmark-20260804-123658Z/`