using System.Diagnostics.Metrics;
using ChatApp.TcpGateway.Core.Authentication;

namespace ChatApp.TcpGateway.Gateway.Diagnostics;

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
    }

    public void ConnectionAccepted()
    {
        _connectionsAccepted.Add(1);
        _connectionsActive.Add(1);
    }

    public void ConnectionRejected() => _connectionsRejected.Add(1);

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
            new KeyValuePair<string, object?>("failure.kind", kind.ToString()));

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

    public void RealtimeEventRejected(string reason) =>
        _realtimeEventsRejected.Add(
            1,
            new KeyValuePair<string, object?>("reason", reason));

    public void Dispose() => _meter.Dispose();
}
