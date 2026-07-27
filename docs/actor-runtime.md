# Actor Runtime 与网关接入

`ActorRuntime/` 是一个仅依赖 BCL 的轻量分片 Actor 运行时。当前先用于网关
`Ephemeral` 命令通道；`Inline` 握手/鉴权及需要强顺序保证的 `OrderedWrite`
仍沿用原流程。

## 热路径结构

- `ActorKey` 只做一次分片；每个 shard 单消费者执行 Actor 行为，同一 Actor
  不并发，无需在状态上加锁。
- 入口使用有界 MPSC 环形队列。槽位通过受控的 `Unsafe.Add` 访问，避免数组边界检查；
  不持有裸指针，不跨越托管对象生命周期。
- 就绪 Actor 使用 intrusive 单链队列，不再为每次调度创建节点，也没有固定 256
  容量造成的丢唤醒风险。
- FIFO 邮箱前 4 项放在 `InlineArray` 中，超过后才租用 `ArrayPool<T>` 环形数组。
  核心邮箱不使用 `Queue<T>`，空闲和短突发 Actor 不创建额外队列对象。
- `LatestOnly` 使用单槽覆盖语义，适合状态同步类消息；被覆盖消息通过
  `IActorMessageDropHandler<T>` 释放其资源。
- 完成消息常态走独立的预分配高优先级 MPSC 环，只有极端溢出才惰性创建
  `ConcurrentQueue` 兜底；可恢复 `Suspend` Actor，且不与普通入口争用容量。
- deadline wheel 使用双缓冲桶，触发时不生成快照数组；共享定时 pulse 唤醒所有 shard。
- 计数器按 shard 本地累加，快照读取使用 `Volatile`，避免每条消息执行全局原子操作。

## 背压与资源所有权

FIFO 入队在生产者侧预留 mailbox credit，因此成功返回即代表消息已获得邮箱容量；
邮箱已满时同步返回 `MailboxFull`，不会出现“入口接受、稍后静默丢弃”。

消息被替换、Actor 故障、过期、运行时停止或 deadline 未能投递时，运行时都会调用
drop handler。网关的 handler 统一归还 `ArrayPool<byte>` payload 并释放
`GlobalInboundBudget`，避免异常路径泄漏预算。

异步 I/O 不在 shard 上直接 `await`。Actor 提交到全局有界执行器后进入
`Suspend`，操作通过带 generation 的 completion 恢复对应 Actor。异常、超时和停机
都会调用 `IAsyncOperation.OnFailure`；Runtime `Drain` 会等待入口、邮箱、执行中 Actor
及异步操作全部归零。

## 网关启用与回退

`TcpGatewayOptions.UseActorRuntimeForEphemeralCommands` 控制接入：

- `true`：`SessionRuntime` 将 `CommandLane.Ephemeral` 交给
  `EphemeralCommandPipeline` / Actor Runtime。
- `false`：继续使用原 `SessionCommandExecutor`，便于快速回退和 A/B。

`appsettings.json` 当前已启用新通道。可通过以下选项调节：

- `EphemeralActorShardCount`
- `EphemeralActorIngressCapacity`（必须为 2 的幂）
- `EphemeralActorAsyncConcurrency`
- `EphemeralActorIdleTimeout`
- `EphemeralActorOperationTimeout`

Meter 暴露 active/busy actor、pending ingress/mailbox/async operation 及 processed
计数。生产切换时重点观察入口拒绝、邮箱拒绝、异步超时、drop reason、工作集和
p95/p99。

## 本地微基准

```powershell
dotnet run -c Release --project tools/ChatApp.ActorRuntime.Benchmarks -- `
  --messages 5000000 --keys 16384 --producers 8 --shards 8
```

本机开发环境结果（2026-07-28）：

| 场景 | 吞吐 | 分配/消息 | 入口重试 |
|---|---:|---:|---:|
| 1,024 keys / 8 producers / 8 shards / 5M | 11.80 M msg/s | 3.74 B | 6,890 |
| 16,384 keys / 8 producers / 8 shards / 5M | 12.31 M msg/s | 2.74 B | 6,432 |
| 单热点 Actor / 1M | 0.22 M msg/s | 0.05 B | 63,500 |

基准会先激活全部 Actor/Admission，再开始统计稳态吞吐和分配。单热点结果体现单 Actor
串行和 mailbox 背压上限，不代表多 Actor 总吞吐。此微基准不能替代 Linux 多进程容量
曲线与 8–24 小时 soak。
