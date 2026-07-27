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
public sealed class GatewayMetrics : IDisposable
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

    public void MessagePublished() => _messagesPublished.Add(1);

    public void MessagePublishFailed() => _messagePublishFailures.Add(1);

    public void ReceiptPublished() => _receiptsPublished.Add(1);

    public void ReceiptPublishFailed() => _receiptPublishFailures.Add(1);

    public void HistoryQueryCompleted() => _historyQueriesCompleted.Add(1);

    public void HistoryQueryFailed() => _historyQueryFailures.Add(1);

    public void RealtimeEventReceived() => _realtimeEventsReceived.Add(1);

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

    // ===== 入站预算可观测性 =====
    //
    // 通过 ObservableGauge 暴露 GlobalInboundBudget / GlobalOutboundBudget 的当前值。
    // Instrument 生命周期与 Meter 绑定，无需单独释放；调用方须持有 provider 委托引用以避免 GC 回收。

    /// <summary>
    /// 注册入站预算 ObservableGauge：committed（已预留）字节数、max 上限。
    /// 调用方须持有 provider 委托的引用，避免委托被 GC 回收导致观察失败。
    /// </summary>
    public void RegisterInboundBudgetObservers(
        Func<long> committedBytesProvider,
        Func<long> maxBytesProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.inbound.committed.bytes",
            () => committedBytesProvider(),
            unit: "By",
            description: "已向 GlobalInboundBudget 预留的入站字节数（含 Pipe segment + lane 复制/池化 payload）。");
        _meter.CreateObservableGauge(
            "gateway.inbound.max.bytes",
            () => maxBytesProvider(),
            unit: "By",
            description: "GlobalInboundBudget 上限，对应 TcpGatewayOptions.GlobalMaxInboundBufferedBytes。");
    }

    /// <summary>
    /// 注册出站预算 ObservableGauge：committed（已入队）字节数、max 上限。
    /// </summary>
    public void RegisterOutboundBudgetObservers(
        Func<long> committedBytesProvider,
        Func<long> maxBytesProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.outbound.committed.bytes",
            () => committedBytesProvider(),
            unit: "By",
            description: "全局出站队列当前暂存字节数。");
        _meter.CreateObservableGauge(
            "gateway.outbound.max.bytes",
            () => maxBytesProvider(),
            unit: "By",
            description: "全局出站队列字节上限，对应 TcpGatewayOptions.GlobalMaxOutboundQueuedBytes。");
    }

    /// <summary>
    /// 注册进程级与派生资源 ObservableGauge，用于观测 GlobalInboundBudget 与物理内存的差距。
    /// <para>
    /// GlobalInboundBudget 只跟踪已 <c>TryReserve</c> 的字节数，但 Fill 路径在
    /// <c>socket.ReceiveAsync</c> 返回前已向 Pipe 申请 segment，此部分未计入预算；
    /// ArrayPool/MemoryPool 的池化余量与对象开销也不在 committed 内。
    /// 通过 working set 与 committed 的差值（unaccounted）可观测这部分"隐藏"内存，
    /// 避免将 committed 误用为物理内存硬上限。
    /// </para>
    /// </summary>
    /// <param name="workingSetBytesProvider">进程 WorkingSet64（物理内存）。</param>
    /// <param name="activeSessionsProvider">当前活跃连接数。</param>
    /// <param name="inboundCommittedBytesProvider">GlobalInboundBudget.CurrentBytes。</param>
    public void RegisterResourceObservers(
        Func<long> workingSetBytesProvider,
        Func<int> activeSessionsProvider,
        Func<long> inboundCommittedBytesProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.process.working_set.bytes",
            () => workingSetBytesProvider(),
            unit: "By",
            description: "进程物理内存 WorkingSet64，用于与 GlobalInboundBudget.committed 对比观测隐藏内存。");

        _meter.CreateObservableGauge(
            "gateway.inbound.avg_per_session.bytes",
            () =>
            {
                var sessions = activeSessionsProvider();
                return sessions > 0
                    ? inboundCommittedBytesProvider() / sessions
                    : 0;
            },
            unit: "By",
            description: "每连接平均已提交入站字节数 = committed / active_sessions，用于估算单连接内存成本。");

        _meter.CreateObservableGauge(
            "gateway.inbound.unaccounted.bytes",
            () => Math.Max(0, workingSetBytesProvider() - inboundCommittedBytesProvider()),
            unit: "By",
            description: "WorkingSet 与 committed 的差值，包含 Pipe 未计入 segment、池化余量、对象开销等隐藏内存。");
    }

    /// <summary>
    /// 注册 Runtime V2 共享执行器 ObservableGauge：DeadlineWheel 活跃 deadline 数、
    /// OnDemandSendPump ready queue 深度与累计调度次数。
    /// <para>
    /// provider 为 null 时对应指标跳过注册：PersistentSendLoop 模式下 OutboundPumpCoordinator 为 null，
    /// DeadlineWheel 在测试场景下也可为 null。调用方须持有 provider 委托引用避免 GC。
    /// </para>
    /// </summary>
    /// <param name="activeDeadlinesProvider">DeadlineWheel.ActiveDeadlineCount。</param>
    /// <param name="outboundPumpReadyQueueProvider">OutboundPumpCoordinator.ReadyQueueCount（OnDemandSendPump 模式）。</param>
    /// <param name="outboundPumpTotalScheduledProvider">OutboundPumpCoordinator.TotalScheduled 累计调度次数。</param>
    /// <param name="outboundPumpWorkerCountProvider">OutboundPumpCoordinator.WorkerCount。</param>
    public void RegisterRuntimeV2Observers(
        Func<long>? activeDeadlinesProvider,
        Func<long>? outboundPumpReadyQueueProvider,
        Func<long>? outboundPumpTotalScheduledProvider,
        Func<int>? outboundPumpWorkerCountProvider)
    {
        if (activeDeadlinesProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.deadline_wheel.active_deadlines",
                () => activeDeadlinesProvider(),
                unit: "{deadlines}",
                description: "全局 DeadlineWheel 当前活跃 deadline 数（认证/空闲/发送超时注册，已注册未触发也未取消）。");
        }

        if (outboundPumpReadyQueueProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.outbound_pump.ready_queue.depth",
                () => outboundPumpReadyQueueProvider(),
                unit: "{sessions}",
                description: "OnDemandSendPump 模式下 ready queue 中待 pump 的 session 数。PersistentSendLoop 模式下不注册此指标。");
        }

        if (outboundPumpTotalScheduledProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.outbound_pump.total_scheduled",
                () => outboundPumpTotalScheduledProvider(),
                unit: "{schedules}",
                description: "OnDemandSendPump 模式下累计成功调度的 session 次数（含重新入队的轮转）。用于 A/B 评估唤醒频率。");
        }

        if (outboundPumpWorkerCountProvider is not null)
        {
            _meter.CreateObservableGauge(
                "gateway.outbound_pump.worker_count",
                () => outboundPumpWorkerCountProvider(),
                unit: "{workers}",
                description: "OnDemandSendPump 模式下共享出站 worker 池大小。");
        }
    }

    /// <summary>
    /// 注册 Ephemeral ActorRuntime 的低基数运行时指标。
    /// Observability 只接收 BCL provider，不依赖 ActorRuntime 项目。
    /// </summary>
    public void RegisterActorRuntimeObservers(
        Func<long> activeActorsProvider,
        Func<long> busyActorsProvider,
        Func<long> pendingIngressProvider,
        Func<long> pendingMailboxProvider,
        Func<int> pendingAsyncProvider,
        Func<long> totalProcessedProvider)
    {
        _meter.CreateObservableGauge(
            "gateway.actor.active",
            () => activeActorsProvider(),
            unit: "{actors}");
        _meter.CreateObservableGauge(
            "gateway.actor.busy",
            () => busyActorsProvider(),
            unit: "{actors}");
        _meter.CreateObservableGauge(
            "gateway.actor.ingress.pending",
            () => pendingIngressProvider(),
            unit: "{messages}");
        _meter.CreateObservableGauge(
            "gateway.actor.mailbox.pending",
            () => pendingMailboxProvider(),
            unit: "{messages}");
        _meter.CreateObservableGauge(
            "gateway.actor.async.pending",
            () => pendingAsyncProvider(),
            unit: "{operations}");
        _meter.CreateObservableGauge(
            "gateway.actor.messages.processed",
            () => totalProcessedProvider(),
            unit: "{messages}");
    }

    private static string GetFailureKindName(AuthenticationFailureKind kind) =>
        kind switch
        {
            AuthenticationFailureKind.InvalidCredentials => "invalid_credentials",
            AuthenticationFailureKind.DeviceMismatch => "device_mismatch",
            AuthenticationFailureKind.DependencyUnavailable => "dependency_unavailable",
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
