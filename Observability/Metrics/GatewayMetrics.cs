using System.Diagnostics.Metrics;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Observability.Logging;

namespace ChatApp.TcpGateway.Observability.Metrics;

/// <summary>
/// All gateway meters and counters. High-cardinality identifiers (ConnectionId,
/// RequestId, UserId, SessionId, MessageId) MUST NOT be used as tags here; they
/// belong in logs or traces.
/// </summary>
public sealed partial class GatewayMetrics : IDisposable
{
    public const string MeterName = "ChatApp.TcpGateway";

    private readonly Meter _meter = new(MeterName, "1.0.0");
    private readonly Counter<long> _connectionsAccepted;
    private readonly Counter<long> _connectionsRejected;
    private readonly UpDownCounter<long> _connectionsActive;
    private readonly Counter<long> _packetsReceived;
    private readonly Counter<long> _framesSent;
    private readonly Counter<long> _outboundRejected;
    private readonly UpDownCounter<long> _outboundQueuedFrames;
    private readonly UpDownCounter<long> _outboundQueuedBytes;
    private readonly Counter<long> _protocolErrors;
    private readonly Counter<long> _authenticationFailures;
    private readonly Counter<long> _messagesPublished;
    private readonly Counter<long> _messagePublishFailures;
    private readonly Counter<long> _receiptsPublished;
    private readonly Counter<long> _receiptPublishFailures;
    private readonly Counter<long> _historyQueriesCompleted;
    private readonly Counter<long> _historyQueryFailures;
    private readonly Counter<long> _realtimeEventsReceived;
    private readonly Counter<long> _pushDeliveriesReceived;
    private readonly Counter<long> _pushDeliveriesFailed;
    private readonly Counter<long> _realtimeEventsHandled;
    private readonly Counter<long> _realtimeEventDeliveries;
    private readonly Counter<long> _realtimeEventsRejected;
    // 聚合事件本机命中分布：用于路由分片监控（fanout 本机命中率 = local_recipients_sum / total_targets_sum）。
    private readonly Counter<long> _realtimeAggregatedEventsDispatched;
    private readonly Histogram<long> _realtimeAggregatedLocalRecipients;
    private readonly Histogram<long> _realtimeAggregatedTotalTargets;
    // 过载保护 metrics
    private readonly Counter<long> _connectionsRejectedPerIp;
    private readonly Counter<long> _connectionsRejectedUnauthLimit;
    private readonly Counter<long> _authAttemptsRejectedPerIp;
    private readonly Counter<long> _outboundRejectedGlobalBudget;
    private readonly UpDownCounter<long> _connectionsUnauthenticated;

    // 通用计数器：命令失败、依赖操作失败、瞬态丢弃。
    private readonly Counter<long> _commandFailures;
    private readonly Counter<long> _dependencyOperationFailures;
    private readonly Counter<long> _ephemeralEventsDropped;
    private readonly Counter<long> _presenceQueriesFailed;

    // Resume 路径可观测性：区分 Resume 成功与完整认证成功率，识别 Redis 故障期间的快速失败比例。
    // attempts：每次 TryResumeAsync 调用 +1，用于计算 Resume 命中率。
    // succeeded：Resume 成功 +1。
    // failed：Resume 失败 +1，tag: reason（invalid_token/redis_failure/circuit_open/lease_mismatch）。
    // circuit_breaker_open：Redis 熔断器开路快速失败 +1（含 Resume 之外的其他 Redis 路径）。
    private readonly Counter<long> _resumeAttempts;
    private readonly Counter<long> _resumeSucceeded;
    private readonly Counter<long> _resumeFailed;
    private readonly Counter<long> _redisCircuitBreakerOpen;

    // Presence 路由/分片监控指标。
    // transitions：全局状态转换计数（tag: transition=online/offline/none），用于观测冗余 SetOnline/SetOffline 比例。
    // fanout.delivered / fanout.recipients：本机 fanout 事件数与接收者数直方图，用于监控放大系数。
    // fanout.skipped：watcher 目录为空时跳过的 fanout 次数，用于监控"发布但无本机 watcher"的浪费。
    // ephemeral.published：跨网关 Ephemeral Presence 发布成功计数，与 DependencyOperationFailed 互补。
    // watcher.directory_ops：watcher 目录 Register/Unregister 操作计数（tag: operation, outcome）。
    private readonly Counter<long> _presenceTransitions;
    private readonly Counter<long> _presenceFanoutDelivered;
    private readonly Histogram<long> _presenceFanoutRecipients;
    private readonly Counter<long> _presenceFanoutSkipped;
    private readonly Counter<long> _presenceEphemeralPublished;
    private readonly Counter<long> _watcherDirectoryOps;

    // 群组命令 RequestId 幂等缓存监控。
    // hit：缓存命中（重复请求从缓存返回，未调用 Realtime）。
    // miss：缓存未命中（新请求，将调用 Realtime 并缓存结果）。
    // conflict：同一 RequestId 但负载指纹不匹配（客户端复用 RequestId 提交不同参数）。
    // redis_hit：L2（Redis）命中（L1 未命中后 L2 命中，含回填 L1）。
    // redis_miss：L2（Redis）未命中（含 fail-open Miss）。
    // redis_failure：L2（Redis）调用失败（异常或熔断器开路，fail-open）。
    // 用于观测客户端重试率、缓存命中率与 Redis L2 层有效性。
    private readonly Counter<long> _groupIdempotentHits;
    private readonly Counter<long> _groupIdempotentMisses;
    private readonly Counter<long> _groupIdempotentConflicts;
    private readonly Counter<long> _groupIdempotentRedisHits;
    private readonly Counter<long> _groupIdempotentRedisMisses;
    private readonly Counter<long> _groupIdempotentRedisFailures;

    // 心跳分桶扫描监控指标。
    // scan.duration：单次 tick（桶扫描+刷新）总耗时直方图，用于观测周期性脉冲与分桶后的平滑度。
    // sessions.scanned：每次 tick 扫描的 session 数（含超时关闭与刷新候选），用于观测扫描负载。
    // refresh.attempts / refresh.failures：设备租约+Presence 刷新尝试与失败计数，tag: kind=lease/presence。
    // refresh.duration：单次刷新操作耗时直方图，用于观测 Redis 延迟。
    // bucket.skew：当桶内实际 session 数偏离均值时的偏移（仅在分桶模式下记录），用于观测负载不均。
    private readonly Histogram<double> _heartbeatScanDuration;
    private readonly Counter<long> _heartbeatSessionsScanned;
    private readonly Counter<long> _heartbeatRefreshAttempts;
    private readonly Counter<long> _heartbeatRefreshFailures;
    private readonly Histogram<double> _heartbeatRefreshDuration;
    private readonly Counter<long> _heartbeatBucketSkew;

    // 八.4：心跳队列延迟与周期安全门禁指标。
    // queue.depth：当前待刷新工作项数（ObservableGauge，拉取式）。
    // queue.oldest_age：最老待处理项的排队年龄 ms（ObservableGauge）——超过 LeaseTtl 安全余量即告警。
    // refresh.schedule_lag：tick 实际触发时间相对计划时间的延迟 ms（Redis 慢 → WriteAsync 阻塞 → tick 延迟）。
    // refresh.overdue：排队等待超过阈值的刷新计数（tag: kind=lease/presence）。
    // full_cycle.duration：完成一个完整扫描周期（bucketCount 个 tick）的耗时 ms。
    private readonly Histogram<double> _heartbeatScheduleLag;
    private readonly Counter<long> _heartbeatRefreshOverdue;
    private readonly Histogram<double> _heartbeatFullCycleDuration;

    // Specialized Typing Actor 管道可观测性：覆盖 Generic Actor 指标未暴露的领域维度。
    // typing_actor.* 反映 Specialized Actor Runtime 真实负载（active/busy/ingress/replaced/admission）；
    // typing_auth.* 反映授权 I/O DomainWorkLane 状态与耗时，避免开启 Specialized 后仪表盘仅显示空闲数据。
    private readonly Histogram<double> _typingAuthDuration;

    public GatewayMetrics()
    {
        _connectionsAccepted = _meter.CreateCounter<long>(
            "gateway.connections.accepted");
        _connectionsRejected = _meter.CreateCounter<long>(
            "gateway.connections.rejected");
        _connectionsActive = _meter.CreateUpDownCounter<long>(
            "gateway.connections.active");
        _packetsReceived = _meter.CreateCounter<long>(
            "gateway.packets.received");
        _framesSent = _meter.CreateCounter<long>(
            "gateway.frames.sent");
        _outboundRejected = _meter.CreateCounter<long>(
            "gateway.outbound.rejected");
        _outboundQueuedFrames = _meter.CreateUpDownCounter<long>(
            "gateway.outbound.queued.frames");
        _outboundQueuedBytes = _meter.CreateUpDownCounter<long>(
            "gateway.outbound.queued.bytes",
            unit: "By");
        _protocolErrors = _meter.CreateCounter<long>(
            "gateway.protocol.errors");
        _authenticationFailures = _meter.CreateCounter<long>(
            "gateway.authentication.failures");
        _messagesPublished = _meter.CreateCounter<long>(
            "gateway.messages.published");
        _messagePublishFailures = _meter.CreateCounter<long>(
            "gateway.messages.publish.failures");
        _receiptsPublished = _meter.CreateCounter<long>(
            "gateway.receipts.published");
        _receiptPublishFailures = _meter.CreateCounter<long>(
            "gateway.receipts.publish.failures");
        _historyQueriesCompleted = _meter.CreateCounter<long>(
            "gateway.history.queries.completed");
        _historyQueryFailures = _meter.CreateCounter<long>(
            "gateway.history.queries.failures");
        _realtimeEventsReceived = _meter.CreateCounter<long>(
            "gateway.realtime.events.received");
        _pushDeliveriesReceived = _meter.CreateCounter<long>("gateway.push.received");
        _pushDeliveriesFailed = _meter.CreateCounter<long>("gateway.push.failed");
        _realtimeEventsHandled = _meter.CreateCounter<long>(
            "gateway.realtime.events.handled");
        _realtimeEventDeliveries = _meter.CreateCounter<long>(
            "gateway.realtime.deliveries.queued");
        _realtimeEventsRejected = _meter.CreateCounter<long>(
            "gateway.realtime.events.rejected");
        _realtimeAggregatedEventsDispatched = _meter.CreateCounter<long>(
            "gateway.realtime.aggregated.events");
        _realtimeAggregatedLocalRecipients = _meter.CreateHistogram<long>(
            "gateway.realtime.aggregated.local_recipients");
        _realtimeAggregatedTotalTargets = _meter.CreateHistogram<long>(
            "gateway.realtime.aggregated.total_targets");
        _connectionsRejectedPerIp = _meter.CreateCounter<long>(
            "gateway.connections.rejected.per_ip_limit");
        _connectionsRejectedUnauthLimit = _meter.CreateCounter<long>(
            "gateway.connections.rejected.unauth_limit");
        _authAttemptsRejectedPerIp = _meter.CreateCounter<long>(
            "gateway.authentication.rejected.per_ip_rate");
        _outboundRejectedGlobalBudget = _meter.CreateCounter<long>(
            "gateway.outbound.rejected.global_budget");
        _connectionsUnauthenticated = _meter.CreateUpDownCounter<long>(
            "gateway.connections.unauthenticated");
        _resumeAttempts = _meter.CreateCounter<long>(
            "gateway.resume.attempts");
        _resumeSucceeded = _meter.CreateCounter<long>(
            "gateway.resume.succeeded");
        _resumeFailed = _meter.CreateCounter<long>(
            "gateway.resume.failed");
        _redisCircuitBreakerOpen = _meter.CreateCounter<long>(
            "gateway.redis.circuit_breaker.open");

        _commandFailures = _meter.CreateCounter<long>(
            "gateway.commands.failures");
        _dependencyOperationFailures = _meter.CreateCounter<long>(
            "gateway.dependency.operations.failed");
        _ephemeralEventsDropped = _meter.CreateCounter<long>(
            "gateway.ephemeral.events.dropped");
        _presenceQueriesFailed = _meter.CreateCounter<long>(
            "gateway.presence.queries.failed");
        _presenceTransitions = _meter.CreateCounter<long>(
            "gateway.presence.transitions");
        _presenceFanoutDelivered = _meter.CreateCounter<long>(
            "gateway.presence.fanout.delivered");
        _presenceFanoutRecipients = _meter.CreateHistogram<long>(
            "gateway.presence.fanout.recipients");
        _presenceFanoutSkipped = _meter.CreateCounter<long>(
            "gateway.presence.fanout.skipped");
        _presenceEphemeralPublished = _meter.CreateCounter<long>(
            "gateway.presence.ephemeral.published");
        _watcherDirectoryOps = _meter.CreateCounter<long>(
            "gateway.presence.watcher.directory_ops");

        _heartbeatScanDuration = _meter.CreateHistogram<double>(
            "gateway.heartbeat.scan.duration",
            unit: "ms");
        _heartbeatSessionsScanned = _meter.CreateCounter<long>(
            "gateway.heartbeat.sessions.scanned");
        _heartbeatRefreshAttempts = _meter.CreateCounter<long>(
            "gateway.heartbeat.refresh.attempts");
        _heartbeatRefreshFailures = _meter.CreateCounter<long>(
            "gateway.heartbeat.refresh.failures");
        _heartbeatRefreshDuration = _meter.CreateHistogram<double>(
            "gateway.heartbeat.refresh.duration",
            unit: "ms");
        _heartbeatBucketSkew = _meter.CreateCounter<long>(
            "gateway.heartbeat.bucket.skew");
        _heartbeatScheduleLag = _meter.CreateHistogram<double>(
            "gateway.heartbeat.schedule_lag",
            unit: "ms");
        _heartbeatRefreshOverdue = _meter.CreateCounter<long>(
            "gateway.heartbeat.refresh.overdue");
        _heartbeatFullCycleDuration = _meter.CreateHistogram<double>(
            "gateway.heartbeat.full_cycle.duration",
            unit: "ms");
        _groupIdempotentHits = _meter.CreateCounter<long>(
            "gateway.group.idempotent.hit");
        _groupIdempotentMisses = _meter.CreateCounter<long>(
            "gateway.group.idempotent.miss");
        _groupIdempotentConflicts = _meter.CreateCounter<long>(
            "gateway.group.idempotent.conflict");
        _groupIdempotentRedisHits = _meter.CreateCounter<long>(
            "gateway.group.idempotent.redis_hit");
        _groupIdempotentRedisMisses = _meter.CreateCounter<long>(
            "gateway.group.idempotent.redis_miss");
        _groupIdempotentRedisFailures = _meter.CreateCounter<long>(
            "gateway.group.idempotent.redis_failure");

        _typingAuthDuration = _meter.CreateHistogram<double>(
            "gateway.typing_auth.duration",
            unit: "ms");
    }

    public void ConnectionAccepted()
    {
        _connectionsAccepted.Add(1);
        _connectionsActive.Add(1);
    }

    public void ConnectionRejected() => _connectionsRejected.Add(1);

    // 过载保护 metrics 方法
    public void ConnectionRejectedPerIpLimit() =>
        _connectionsRejectedPerIp.Add(1);

    public void ConnectionRejectedUnauthLimit() =>
        _connectionsRejectedUnauthLimit.Add(1);

    public void AuthenticationRejectedPerIpRate() =>
        _authAttemptsRejectedPerIp.Add(1);

    public void OutboundRejectedGlobalBudget() =>
        _outboundRejectedGlobalBudget.Add(1);

    public void UnauthenticatedConnectionAccepted() =>
        _connectionsUnauthenticated.Add(1);

    public void UnauthenticatedConnectionClosed() =>
        _connectionsUnauthenticated.Add(-1);

    public void ConnectionClosed() => _connectionsActive.Add(-1);

    public void PacketReceived() => _packetsReceived.Add(1);

    public void FrameSent() => _framesSent.Add(1);

    public void OutboundEnqueued(int byteCount)
    {
        _outboundQueuedFrames.Add(1);
        _outboundQueuedBytes.Add(byteCount);
    }

    public void OutboundDequeued(int byteCount)
    {
        _outboundQueuedFrames.Add(-1);
        _outboundQueuedBytes.Add(-byteCount);
    }

    public void OutboundRejected(string reason) =>
        _outboundRejected.Add(
            1,
            new KeyValuePair<string, object?>("reason", reason));

    public void ProtocolError() => _protocolErrors.Add(1);

    public void AuthenticationFailed(AuthenticationFailureKind kind) =>
        _authenticationFailures.Add(
            1,
            new KeyValuePair<string, object?>(
                "failure.kind",
                GetFailureKindName(kind)));

    // Resume 路径可观测性：每次 TryResumeAsync 调用 +1；
    // 成功/失败分别计数；失败按 reason 分组（invalid_token/redis_failure/circuit_open/lease_mismatch）。
    public void ResumeAttempted() => _resumeAttempts.Add(1);

    public void ResumeSucceeded() => _resumeSucceeded.Add(1);

    public void ResumeFailed(ResumeFailureReason reason) =>
        _resumeFailed.Add(
            1,
            new KeyValuePair<string, object?>(
                "reason",
                GetResumeFailureReasonName(reason)));

    /// <summary>
    /// Redis 熔断器开路快速失败计数。包含 Resume/Token 颁发/设备租约所有受熔断器保护的 Redis 路径。
    /// </summary>
    public void RedisCircuitBreakerOpen() => _redisCircuitBreakerOpen.Add(1);

    private static string GetResumeFailureReasonName(ResumeFailureReason reason) =>
        reason switch
        {
            ResumeFailureReason.InvalidToken => "invalid_token",
            ResumeFailureReason.RedisFailure => "redis_failure",
            ResumeFailureReason.CircuitOpen => "circuit_open",
            ResumeFailureReason.LeaseMismatch => "lease_mismatch",
            ResumeFailureReason.LeaseQueryFailed => "lease_query_failed",
            ResumeFailureReason.TakeOverUnavailable => "takeover_unavailable",
            ResumeFailureReason.UserFrozen => "user_frozen",
            _ => "unknown"
        };

    public void MessagePublished() => _messagesPublished.Add(1);

    public void MessagePublishFailed() => _messagePublishFailures.Add(1);

    public void ReceiptPublished() => _receiptsPublished.Add(1);

    public void ReceiptPublishFailed() => _receiptPublishFailures.Add(1);

    public void HistoryQueryCompleted() => _historyQueriesCompleted.Add(1);

    public void HistoryQueryFailed() => _historyQueryFailures.Add(1);

    public void RealtimeEventReceived() => _realtimeEventsReceived.Add(1);

    public void PushDeliveryReceived() => _pushDeliveriesReceived.Add(1);

    public void PushDeliveryFailed() => _pushDeliveriesFailed.Add(1);

    public void RealtimeEventHandled(int queuedDeliveries)
    {
        _realtimeEventsHandled.Add(1);
        _realtimeEventDeliveries.Add(queuedDeliveries);
    }

    /// <summary>
    /// 聚合群聊事件分发计数：记录本机命中接收者数与总目标数，
    /// 用于路由分片本机命中率监控（命中率 = local_recipients_sum / total_targets_sum）。
    /// </summary>
    /// <param name="totalTargets">聚合事件 <see cref="RealtimeEvent.TargetUserIds"/> 长度。</param>
    /// <param name="queuedRecipients">本机实际入队的接收者数（跳过来源 Session 后）。</param>
    public void RealtimeAggregatedDispatch(int totalTargets, int queuedRecipients)
    {
        _realtimeAggregatedEventsDispatched.Add(1);
        _realtimeAggregatedLocalRecipients.Record(Math.Max(0, queuedRecipients));
        _realtimeAggregatedTotalTargets.Record(Math.Max(0, totalTargets));
    }

    public void RealtimeEventRejected(RealtimeRejectReason reason) =>
        _realtimeEventsRejected.Add(
            1,
            new KeyValuePair<string, object?>(
                "reason",
                GetRejectReasonName(reason)));

    // 通用命令失败计数：command 为低基数标签。
    public void CommandFailed(PacketCommand command) =>
        _commandFailures.Add(
            1,
            new KeyValuePair<string, object?>(
                "command",
                PacketCommandNames.Get(command)));

    /// <summary>
    /// 群组命令 RequestId 幂等缓存命中：重复请求从缓存返回，未调用 Realtime。
    /// </summary>
    public void GroupIdempotentHit() => _groupIdempotentHits.Add(1);

    /// <summary>
    /// 群组命令 RequestId 幂等缓存未命中：新请求，将调用 Realtime 并缓存结果。
    /// </summary>
    public void GroupIdempotentMiss() => _groupIdempotentMisses.Add(1);

    /// <summary>
    /// 群组命令 RequestId 幂等冲突：同一 RequestId 但负载指纹不匹配。
    /// </summary>
    public void GroupIdempotentConflict() => _groupIdempotentConflicts.Add(1);

    /// <summary>
    /// 群组命令幂等 L2（Redis）命中：L1 未命中后 L2 命中。
    /// </summary>
    public void GroupIdempotentRedisHit() => _groupIdempotentRedisHits.Add(1);

    /// <summary>
    /// 群组命令幂等 L2（Redis）未命中（含 fail-open Miss）。
    /// </summary>
    public void GroupIdempotentRedisMiss() => _groupIdempotentRedisMisses.Add(1);

    /// <summary>
    /// 群组命令幂等 L2（Redis）调用失败（异常或熔断器开路）。
    /// </summary>
    public void GroupIdempotentRedisFailure() => _groupIdempotentRedisFailures.Add(1);

    // 通用依赖操作失败计数：dependency 与 operation 均为低基数标签。
    public void DependencyOperationFailed(
        GatewayDependency dependency,
        GatewayDependencyOperation operation) =>
        _dependencyOperationFailures.Add(
            1,
            new KeyValuePair<string, object?>("dependency", dependency.ToString().ToLowerInvariant()),
            new KeyValuePair<string, object?>("operation", operation.ToString().ToLowerInvariant()));

    // 瞬态易失事件丢弃（TryQueueEphemeral 队列满时）。
    public void EphemeralEventDropped(string reason) =>
        _ephemeralEventsDropped.Add(
            1,
            new KeyValuePair<string, object?>("reason", reason));

    // Presence 查询失败（瞬态，依赖故障期间高频，仅计数不日志）。
    public void PresenceQueryFailed() => _presenceQueriesFailed.Add(1);

    /// <summary>
    /// Presence 全局状态转换计数。transition 为 "online"/"offline"/"none"。
    /// none 表示无转换（已在线再上线/已离线再离线），用于观测冗余 SetOnline/SetOffline 调用比例。
    /// </summary>
    public void PresenceTransition(string transition) =>
        _presenceTransitions.Add(
            1,
            new KeyValuePair<string, object?>("transition", transition));

    /// <summary>
    /// 本地 Presence fanout 完成：记录事件数与实际入队接收者数直方图。
    /// 用于监控 fanout 放大系数与本机命中率。
    /// </summary>
    public void PresenceFanoutDelivered(int recipientCount)
    {
        _presenceFanoutDelivered.Add(1);
        _presenceFanoutRecipients.Record(Math.Max(0, recipientCount));
    }

    /// <summary>
    /// 本地 Presence fanout 跳过：watcher 目录为空，无本机接收者。
    /// 用于监控"发布但无本机 watcher"的浪费比例。
    /// </summary>
    public void PresenceFanoutSkipped() => _presenceFanoutSkipped.Add(1);

    /// <summary>
    /// 跨网关 Ephemeral Presence 发布成功计数。
    /// 与 <see cref="DependencyOperationFailed"/>(RealtimeService, EphemeralPresencePublish) 互补。
    /// </summary>
    public void PresenceEphemeralPublished() => _presenceEphemeralPublished.Add(1);

    /// <summary>
    /// Watcher 目录操作计数。operation 为 "register"/"unregister"，success 为是否成功。
    /// 用于监控分片路由目录写入成功率与频次。
    /// </summary>
    public void WatcherDirectoryOp(string operation, bool success) =>
        _watcherDirectoryOps.Add(
            1,
            new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("outcome", success ? "success" : "failure"));

    // ===== 心跳分桶扫描 metrics =====

    /// <summary>
    /// 单次心跳 tick 总耗时（含扫描、刷新、等待）。
    /// 分桶后应显著低于全量扫描模式，且分布更平稳。
    /// </summary>
    public void HeartbeatScanCompleted(TimeSpan duration) =>
        _heartbeatScanDuration.Record(duration.TotalMilliseconds);

    /// <summary>
    /// 单次 tick 扫描的 session 数（含超时关闭与刷新候选）。
    /// 分桶后应约为总 session 数 / 桶数。
    /// </summary>
    public void HeartbeatSessionsScanned(int sessionCount) =>
        _heartbeatSessionsScanned.Add(sessionCount);

    /// <summary>
    /// 设备租约或 Presence 刷新尝试计数。kind 为 "lease"/"presence"。
    /// </summary>
    public void HeartbeatRefreshAttempted(string kind) =>
        _heartbeatRefreshAttempts.Add(
            1,
            new KeyValuePair<string, object?>("kind", kind));

    /// <summary>
    /// 设备租约或 Presence 刷新失败计数。kind 为 "lease"/"presence"。
    /// 与 <see cref="DependencyOperationFailed"/> 互补：此处按心跳路径单独计数。
    /// </summary>
    public void HeartbeatRefreshFailed(string kind) =>
        _heartbeatRefreshFailures.Add(
            1,
            new KeyValuePair<string, object?>("kind", kind));

    /// <summary>
    /// 单次刷新操作（Redis 往返）耗时直方图，用于观测 Redis 延迟与分桶后的并发平滑度。
    /// </summary>
    public void HeartbeatRefreshCompleted(TimeSpan duration, string kind) =>
        _heartbeatRefreshDuration.Record(
            duration.TotalMilliseconds,
            new KeyValuePair<string, object?>("kind", kind));

    /// <summary>
    /// 桶内实际 session 数偏离均值的偏移（仅在分桶模式下记录）。
    /// 用于观测负载不均，便于调优桶数与分桶策略。
    /// </summary>
    public void HeartbeatBucketSkew(int skew) =>
        _heartbeatBucketSkew.Add(skew);

    /// <summary>
    /// 八.4：tick 实际触发时间相对计划时间的延迟。
    /// Redis 慢速时 WriteAsync 阻塞导致 tick 延迟，schedule_lag 持续增长意味着
    /// 完整扫描周期可能超过 LeaseTtl 安全窗口。
    /// </summary>
    public void HeartbeatScheduleLag(TimeSpan lag) =>
        _heartbeatScheduleLag.Record(lag.TotalMilliseconds);

    /// <summary>
    /// 八.4：排队等待超过阈值的刷新计数。kind 为 "lease"/"presence"。
    /// 阈值由调用方判定后调用此方法——安全门禁：overdue 应持续为 0。
    /// </summary>
    public void HeartbeatRefreshOverdue(string kind) =>
        _heartbeatRefreshOverdue.Add(
            1,
            new KeyValuePair<string, object?>("kind", kind));

    /// <summary>
    /// 八.4：完成一个完整扫描周期（bucketCount 个 tick）的耗时。
    /// 正常应接近 HeartbeatScanInterval（默认 30s）。显著超过意味着 Redis 慢速
    /// 导致 tick 积压，可能使部分会话租约在刷新前过期。
    /// </summary>
    public void HeartbeatFullCycleCompleted(TimeSpan duration) =>
        _heartbeatFullCycleDuration.Record(duration.TotalMilliseconds);

    private static string GetFailureKindName(AuthenticationFailureKind kind) =>
        kind switch
        {
            AuthenticationFailureKind.InvalidCredentials => "invalid_credentials",
            AuthenticationFailureKind.DeviceMismatch => "device_mismatch",
            AuthenticationFailureKind.DependencyUnavailable => "dependency_unavailable",
            AuthenticationFailureKind.UserFrozen => "user_frozen",
            AuthenticationFailureKind.None => "none",
            _ => "unknown"
        };

    private static string GetRejectReasonName(RealtimeRejectReason reason) =>
        reason switch
        {
            RealtimeRejectReason.MissingPayload => "missing_payload",
            RealtimeRejectReason.InvalidJson => "invalid_json",
            RealtimeRejectReason.InvalidPayload => "invalid_payload",
            RealtimeRejectReason.TargetMismatch => "target_mismatch",
            RealtimeRejectReason.MissingSessionId => "missing_session_id",
            _ => "unknown"
        };

    public void Dispose() => _meter.Dispose();
}

/// <summary>
/// Resume 失败原因分类，用于 metrics tag。
/// </summary>
public enum ResumeFailureReason
{
    /// <summary>Token 无效、过期或已被消费（GETDEL 返回 null）。</summary>
    InvalidToken,

    /// <summary>Redis 操作异常（非熔断器开路）。</summary>
    RedisFailure,

    /// <summary>Redis 熔断器开路快速失败。</summary>
    CircuitOpen,

    /// <summary>设备租约已被新会话接管，代次不匹配。</summary>
    LeaseMismatch,

    /// <summary>设备租约查询依赖不可用（Redis 异常或熔断器开路），fail-closed 拒绝恢复。</summary>
    LeaseQueryFailed,

    /// <summary>设备租约接管依赖不可用（Redis 异常或熔断器开路），fail-closed 拒绝恢复。</summary>
    TakeOverUnavailable,

    /// <summary>三-3：账号已被冻结，Resume 拒绝。</summary>
    UserFrozen
}
