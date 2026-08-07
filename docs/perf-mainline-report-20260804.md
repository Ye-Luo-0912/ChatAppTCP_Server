# 极致性能主线 — 测试报告

> 报告日期: 2026-08-04
> 提交: `5b8cf68` feat(perf): Transport Matrix 12组合 + Slowloris + Soak内存稳定性 + 指标采集

## 1. 交付物总览

| 门禁 | 状态 | 说明 |
|------|------|------|
| Gate 1 — 冻结新底层原语 | ✅ 已冻结 | Actor Mailbox / Durable Actor / 二进制协议 / Native AOT / ActorCell slab 暂停新增 |
| Gate 2 — Transport Matrix 12 组合 | ✅ 基础设施完成 | Inbound(2) × Send(3) × Queue(2) = 12 组合，10 场景全部定义 |
| Gate 3 — 默认值切换门槛 | ✅ 验收检查点已落地 | 0 correctness / 0 budget leak / 0 stranded frames / WS 下降 / p99 ≤+5% |
| Gate 4 — Realtime SQL 门禁 | ✅ 7 项基准已建 | Relationship / Attachment / Reply-Mention / SyncBootstrap / Authorization / ReadReceipt / MentionValidation |

## 2. Transport Matrix（12 组合）

| Inbound | Send Mode | Outbound Queue | 场景覆盖 |
|---------|-----------|----------------|----------|
| Pipelines | PersistentSendLoop | BoundedChannel | 全 10 场景 |
| Pipelines | PersistentSendLoop | LazySegmented | 全 10 场景 |
| Pipelines | OnDemandSendPump | BoundedChannel | 全 10 场景 |
| Pipelines | OnDemandSendPump | LazySegmented | 全 10 场景 |
| Pipelines | PerSessionDrain | BoundedChannel | 全 10 场景 |
| Pipelines | PerSessionDrain | LazySegmented | 全 10 场景 |
| DirectSocket | PersistentSendLoop | BoundedChannel | 全 10 场景 |
| DirectSocket | PersistentSendLoop | LazySegmented | 全 10 场景 |
| DirectSocket | OnDemandSendPump | BoundedChannel | 全 10 场景 |
| DirectSocket | OnDemandSendPump | LazySegmented | 全 10 场景 |
| DirectSocket | PerSessionDrain | BoundedChannel | 全 10 场景 |
| DirectSocket | PerSessionDrain | LazySegmented | 全 10 场景 |

### 10 个测试场景

1. 10,000 空闲连接
2. 1% / 10% / 50% 活跃
3. Heartbeat-only
4. 512 B Chat
5. 64 KiB Chat
6. Header Slowloris（不完整 Header 帧）
7. Payload Slowloris（不完整 Payload 帧）
8. 慢读客户端
9. Outbound Byte Budget 耗尽
10. Inbound Global Budget 耗尽

采集指标类别: 内存 / GC / 调度 / 延迟 / Queue / Actor / 后端 (SQL/NATS/Redis/Outbox)。

## 3. 关键测试结果

### 3.1 AB 测试: PersistentSendLoop vs OnDemandSendPump

- 日期: 2026-07-26
- 配置: 1 Gateway, 50 TCP chat 连接, 30s, chat 模式
- 结果: **两组均 PASSED，0 错误**

| 指标 | PersistentSendLoop | OnDemandSendPump |
|------|-------------------:|------------------:|
| Throughput/s | 498.40 | 498.46 |
| Error rate | 0.000% | 0.000% |
| Gateway WS (Start→End) | 60.08 → 110.77 MiB | 60.56 → 111.59 MiB |
| Gateway WS 增量 | +50.69 MiB | +51.03 MiB |
| GC Gen0 / Gen1 / Gen2 | 40 / 1 / 3 | 41 / 1 / 3 |
| GC Pause (total) | 0.052s | 0.055s |
| ThreadPool Queue | 0 | 0 |
| Outbox Pending | 0 | 0 |
| JetStream Pending | 0 | 0 |

**结论**: 在 50 连接 / 500 msg/s 规模下，两种 Send Mode 性能无显著差异，均无内存泄漏迹象。

### 3.2 8 小时 Linux Soak 测试（历史基线 · 2026-07-21）

> ⚠️ **历史数据标记**：本小节为 **2026-07-21** 的历史基线记录，仅用于追溯。
> 其内存判定方法（首值→末值、聚合结论）已被后续报告脚本取代，**不作为当前稳定性结论**。
> 当前门禁采用：排除 Warmup 后建基线 + 最终窗口斜率 + 分位中位数 + 每进程单独判定（见 `Run-Soak.ps1`）。

- 日期: 2026-07-21
- 配置: 2 Gateway, 1000 TCP 连接 (500/GW), Pipeline 32 并发, 8h (28800s)
- 环境: CachyOS, .NET 10.0.10, 12 核, 16 GB RAM
- 结果: **PASSED，0 错误**（正确性门禁通过；内存稳定性按旧口径标记，见下方修正）

| 指标 | 值 |
|------|------|
| Pipeline 消息总数 | 2,303,575 |
| Pipeline 失败数 | 0 |
| Pipeline 错误率 | 0.000% |
| Pipeline 吞吐/s | 79.98 |
| Pipeline p50 / p95 / p99 | 68.5 / 200.5 / 270.0 ms |

**内存稳定性（8h Working Set，首值→末值口径）:**

| 进程 | Start WS | End WS | 增量 | 增占比 | Max WS |
|------|---------:|-------:|-----:|-------:|-------:|
| gateway-1 | 77.63 MiB | 112.52 MiB | +34.89 MiB | +44.9% | 136.12 MiB |
| gateway-2 | 77.59 MiB | 115.94 MiB | +38.35 MiB | +49.4% | 134.51 MiB |
| realtime-1 | 118.67 MiB | 203.41 MiB | +84.74 MiB | +71.4% | 211.07 MiB |

> **修正说明**：旧文称「内存增量 < 40 MiB/Gateway（远低于 20% 阈值），进入稳定平台」**不成立**。
> - 按首/末口径，gateway-1 增长 +44.9%、gateway-2 增长 +49.4%，均**远超 20% 阈值**；
> - realtime-1 增量 +84.74 MiB（>40 MiB），增长 +71.4%；
> - 因此按当前门禁（最终窗口斜率 + 中位数平台判定）**不能判定为 STABLE**，内存仍处于爬升阶段。
>
> 该记录仅作为历史基线，不作为当前稳定性验收依据。

**GC 与队列稳定性（8h）:**

| 指标 | Start | End | Max |
|------|------:|----:|----:|
| GC Gen0 collections | 0 | 52,436 | — |
| GC Gen1 collections | 1 | 1,347 | — |
| GC Gen2 collections | 2 | 61 | — |
| GC Pause total | 0.007s | 208.1s | — |
| ThreadPool Queue | 0 | 0 | 9 |
| JetStream Pending | 0 | 0 | 1 |
| Outbox Pending | 0 | 0 | 8 |
| Outbox Oldest Age | 0s | 0s | 4.6s |
| DB idle connections | 2 | 6 | 18 |

**结论（历史基线）**:
- 8h 内 0 错误，正确性通过（JetStream / Outbox pending 全程接近 0，无积压；ThreadPool 队列最大 9，无饱和）
- GC Gen2 仅 61 次（8h），Pause total 208s / 28800s ≈ 0.72%，可接受
- ⚠️ **内存稳定性：未达 STABLE**。按当前门禁口径（排除 Warmup 后基线 + 最终窗口斜率 + 中位数平台），gateway 与 realtime 内存仍在爬升（+44.9% / +49.4% / +71.4%），**不判定为稳定平台**。需后续用新口径复跑确认。

## 4. Realtime SQL 门禁（Gate 4）

| 功能 | 基准文件 | SQL 上限 | 状态 |
|------|----------|----------|------|
| Relationship 操作 | `RelationshipBenchmarks.cs` | 7/9/6/5 | ✅ |
| Attachment Finalize | `AttachmentFinalizeBenchmarks.cs` | 1 | ✅ |
| Reply/Mention 批量富集 | `ReplyMentionBenchmarks.cs` | 1 (批量) | ✅ |
| SyncBootstrap | `SyncBootstrapBenchmarks.cs` | 1 (批量, 无 N+1) | ✅ |
| Authorization (ACL) | `AuthorizationChainBenchmarks.cs` | 5 | ✅ |
| Read Receipt | `ReadReceiptBenchmarks.cs` | 1 (聚合) | ✅ |
| Mention 可见性过滤 | `MentionValidationBenchmarks.cs` | 0/1/1/1 | ✅ |

## 5. 待完成工作

| 项目 | 说明 | 阻塞项 |
|------|------|--------|
| 8h Soak（Runtime V2, PersistentSendLoop + BoundedChannel） | Linux 测试机执行；须用新口径（Warmup 后基线 + 最终窗口斜率 + 分位中位数 + 每进程判定 + PID 稳定性）复测，确认内存达 STABLE | RealtimeServices 构建路径修复 (`ChatApp.RealtimeServices/bin/Release` 缺失) |
| 历史基线（2026-07-21）内存结论修订 | 已修正：+44.9% / +49.4% / +71.4%，未达 STABLE，标记为历史基线 | 无（已修订） |
| Transport Matrix 全量执行 | 12 组合 × 10 场景 | 依赖 Linux Soak 环境就绪 |
| 性能基线复跑 | 会话翻页 + SyncBootstrap + TCP chat 扇出/慢消费者 | 依赖 Linux 环境就绪 |
| 版本化 JSON 短期门禁接入定时 CI | 自托管 Linux runner | P1 CI runner 部署 |

## 6. 原始报告位置

以下报告位于 `.artifacts/performance/`（gitignored，仅本地保留）:

- AB 测试: `ab-runtime-v2/PersistentSendLoop/` 与 `ab-runtime-v2/OnDemandSendPump/`
- 8h Soak: `remote-linux/linux-soak-20260721-090450Z/`
- 容量曲线: `capacity-curve-20260720-*/`
- 故障注入: `fault-injection-20260720-*/`
