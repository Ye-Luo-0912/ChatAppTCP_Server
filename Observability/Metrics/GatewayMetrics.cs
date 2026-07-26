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
