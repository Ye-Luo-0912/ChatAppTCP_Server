# 历史变更

本文件记录已完成的历史变更，按时间倒序排列。当前状态见 `roadmap-current-state.md`，
未完成工作见 `roadmap-todo.md`。本文件不再保留过期测试数字（当时通过数仅作参考）。

## 2026-07-31

### 八.1 PerSessionDrain 零分配 CAS

- `DrainOperation` 改用 `ManualResetValueTaskSourceCore<bool>` 实现 `IValueTaskSource`，
  单实例跨代次复用，消除每入队 `TaskCompletionSource` 分配。
- 用 packed `long`（`_drainStateGen`）合并 Running 位 + 32-bit generation，
  替代对象引用 CAS，实现零分配原子状态转换。
- `SpinLock` 串行化 `Reset`/`Complete`，防止跨代次竞态（旧代次完成不污染新代次）。
- `DrainOperation.Complete` 增加代次校验 + 幂等保护（`_completed` 标志）。

### 八.4 心跳队列安全门指标

- `HeartbeatRefreshWork` 增加 `EnqueuedAtTimestamp`，跟踪队列项排队年龄。
- 新增 `_oldestEnqueueTimestamp` + `_tickInterval`，队列空→非空时 CAS 记录最老项。
- `RunAsync` 跟踪 tick schedule_lag（实际 vs 计划触发时间）与 full_cycle.duration。
- `WorkerLoopAsync` 检测排队超时（>tickInterval），记录 refresh.overdue 计数。
- 新增指标：`gateway.heartbeat.queue.depth` / `queue.oldest_age`（ObservableGauge）、
  `schedule_lag` / `full_cycle.duration`（Histogram）、`refresh.overdue`（Counter，tag: kind）。
- `TcpGatewayService` 注册 queue.depth/oldest_age 观察者回调。

### 群聊幂等指纹 SHA-256 稳定化 + Redis L2 条件写 + Realtime Consumer 异常传播

- 群组幂等 payload hash 改用 SHA-256（归一化二进制表示），替代 `System.HashCode`
  （进程随机种子，跨进程不稳定）。
- Redis L2 idempotency `TryAdd` 改为条件 Lua 写（仅当 key 缺失或存储 payloadHash 匹配才写），
  防止并发 Miss last-writer-wins 覆写。
- Realtime Consumer Worker 故障须先 `await workersTask` 捕获根因，再取消 mainLoop，
  防止 `OperationCanceledException` 掩盖真实 worker 异常。
- Realtime Consumer Drain 窗口使用独立 `CancellationTokenSource.CancelAfter(DrainTimeout)`，
  不链接已取消的 stoppingToken，保留 30s drain 预算。

### P0 生产路径关键缺陷修复（8 项）

- Redis 熔断器拆分（ResumeToken / DeviceLease / GroupIdempotency / PushToken / RoutingDirectory），
  防止跨域故障耦合。
- Resume Prepare 阶段改用 `TryClaim`/`CommitClaim`/`ReleaseClaim` Lua 原子操作，替代 `GETDEL`，
  防止 token 在 Commit 前被消费。
- Session 跟踪显式 `AdmissionState`（Unauthenticated/Promoted/Released），防止连接计数泄漏。
- `DomainWorkLane` 改用 `Channel<TWork>` 替代 `BoundedMpmcRing` + `SemaphoreSlim` + `_signalState`。
- `DomainWorkLane` worker 按 burst（1-8 项）处理 + 每项 stopToken 检查，防止停机后批量执行。
- `ActorRuntime.TryTellDurable` 区分新 Actor 激活（全局配额 + Shard 限制）与已有 Actor 投递
  （仅 Mailbox 容量约束）。
- `RedisDeviceSessionLeaseStore.TakeOverAsync` Redis 失败时抛 `RedisException`（非返回 null），
  确保 fail-closed。
- `ResumeVerification` 注入确定性 `DeviceIdHash`（按 userId 派生），支持同设备 fencing。

## 2026-07-30

### Resume 闭环验证

- Redis owner 探针 + NATS 故障本机关闭。
- takeover-competition 保留旧 Socket 读写断言（`ReadClosed` / `WriteClosed`，800ms 传播延迟）。
- Resume error code 区分 + 10k reconnect storm 压测。

## 2026-07-29

### Resume 可靠性补强

- Redis 应用层熔断器（`IRedisCircuitBreaker` / `RedisCircuitBreaker`）：
  Closed→Open→HalfOpen 三状态机，连续失败阈值 `CircuitBreakerFailureThreshold`（默认 5），
  开路时长 `CircuitBreakerOpenDuration`（默认 5s）。
- `RevokeAsync` DemandMaster，防止主从切换期间撤销失效。
- Resume 路径广播 `SessionRevoked`，跨 Gateway 旧 session 不继续发送出站帧。
- Resume 可观测性指标：`gateway.resume.attempts/succeeded/failed`（tag: reason）、
  `gateway.redis.circuit_breaker.open`。

### 群聊 RequestId 幂等缓存

- `GroupRequestIdempotencyCache`：有界 TTL+LRU，4096 条/30s，键为 `(ActorUserId, RequestId)`。
- 仅缓存 Realtime 正常返回（含业务失败），异常路径不缓存可重试。
- 指标：`gateway.group.idempotent.hit` / `miss` / `conflict` /
  `redis_hit` / `redis_miss` / `redis_failure`。

### R1 真实 Ephemeral A/B 测试

- 覆盖三条 Ephemeral 管道（Legacy / Actor Generic / Specialized TypingActor）。
- 10+1 场景全覆盖：授权缓存命中/未命中/拒绝、NATS 延迟/断线、同 Key 高频覆盖、
  连接 churn、10k 活跃 Actor、单热点 Actor、预算归零、Legacy vs TypingActor 对比。
- 关键 Bug 修复：`BoundedMpscRing.TryDequeue` 多消费者竞态、
  `ReceiveAuthorizationCompleted` 主动 TryEmit。

## 2026-07-28

### DirectSocket 默认切换

- `InboundTransportMode` 默认从 `Pipelines` 切换为 `DirectSocket`。
- Linux 真实集成短测通过：200 认证 Heartbeat 连接，1,216/1,216 持久 Pipeline 成功，
  40.12/s，p95/p99 253.5/364.5 ms。
- 修复 RealtimeServices `ResolveSyncWatermarksAsync` 在 Npgsql `SequentialAccess` 下
  先读第 6 列再回读第 5 列的错误。

## 早期已完成（按主题归并，日期不详）

### Runtime V2 Phase 1+2

- `TcpClientSession` 拆分为 `Outbound`（Durable FIFO + Ephemeral keyed mailbox + 两种发送驱动）
  与 `Transport`（Close/Dispose、deadline、SendFrameAsync）partial。
- 全局共享执行器替代每连接资源：`DeadlineWheel`、`SessionCommandExecutor`、`OutboundPumpCoordinator`。
- `OutboundSendMode` A/B：`PersistentSendLoop`（默认）vs `OnDemandSendPump`。
- `SessionLifecycleCoordinator` 拆分为 `Presence` 与 `DeviceSession` partial；
  `GroupCommandHandler` 拆分为 `Create`/`Mutate`/`Helpers`；
  `TcpGatewayService` 由 ~1800 行降至当时 669 行；
  `RealtimeEventDispatcher` 由 1316 行降至 190 行。
- `DeadlineWheel` 计数器修复：`_fired` 集合使 `Cancel` 在已触发注册上幂等忽略
  （**注：`_fired` 后续已移除**，改用分桶 + 计数器）。
- `SessionCommandExecutor.StopAsync` 移除 early-return，确保排空队列释放预算与缓冲。

### 轻量 Actor Runtime 接入

- BCL-only `ActorRuntime/`：分片单消费者、有界 MPSC 入口、intrusive ready queue、
  前 4 项内联 + `ArrayPool<T>` 溢出环形邮箱。
- `EphemeralCommandPipeline` 接入 `SessionRuntime`，由 `UseActorRuntimeForEphemeralCommands` 切换。
- 设计与实测见 `docs/actor-runtime.md`。

### P0 热路径性能修复

- 心跳分桶扫描：`HeartbeatBucketRegistry` 按 connectionId/userId 分桶，每 tick 仅枚举一桶，
  不再 `_sessions.Values.ToArray()` 全量扫描。
- 心跳刷新指标正确区分成功/失败（`RefreshLeaseAsync` / `RefreshPresenceAsync` 返回 bool）。
- `CommandContext` 转为 readonly struct，消除每命令堆分配。
- `TypingFanoutHost` 有界发布：keyed pending + 单槽唤醒 + 固定 publisher worker。
- `ResponseByteBudget` 边界修复：`TruncateOutcome` 枚举，item_too_large 错误。
- Presence Redis 热路径：Lua 移除 `ZREMRANGEBYSCORE`，`ZCOUNT` 替代；`RunMaintenanceAsync` 后台清理。
- Realtime JetStream 分区并行消费：按 `TargetUserId % PartitionCount` 路由。

### P1 业务闭环

- Resume 同步水位恢复（`LastConversationSequence`）。
- 群聊廉价结构校验（`GroupCommandHandler.Validation.cs`）。
- Push Token 注册原子化（Lua 合并 HSET+ZADD+淘汰+TTL，Hash tag 对齐 Cluster slot）。
- Relationship 授权缓存主动失效（friendship/blocked-user 双向失效）。
- 协议版本与兼容性基础（`GatewayFeature`、`ProtocolPayloadFormat`、`Deprecated`、
  最低客户端版本、FeatureBits 强制检查、命令级能力协商）。

### 性能基线与故障注入

- 本机 30 分钟基线：1000 TCP 长连接，94,715 持久链路成功，52.61 pipeline/s。
- Outbox 优化 A/B：吞吐 +120.2%，p95/p99 -71.9%/-62.6%。
- History SQL 放大消除：扫描行 1,535→11，缓冲页 1,162→17，执行时间 6.055ms→0.071ms。
- 容量曲线：120/s 目标 5 分钟持续 115.35/s，34,626 成功。
- 故障注入短测：Garnet 568/568、PostgreSQL 575/575、NATS pause/unpause 513/513。
- 多进程编排器统一启动/采样/报告；`ChatApp.Performance.Gate` 失败闭环检查。

### 可观测性部署

- Gateway 与 RealtimeServices 接入 OpenTelemetry Metrics/Tracing + OTLP。
- W3C Trace Context 贯穿全链路。
- Linux 测试机部署 Prometheus + Grafana + 初始告警规则 + 仪表盘（`deploy/observability/`）。
