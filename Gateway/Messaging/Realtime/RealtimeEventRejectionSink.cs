using ChatApp.Realtime.Abstractions.Events;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime;

/// <summary>
/// Realtime 事件校验失败统一出口：记录 <see cref="GatewayMetrics.RealtimeEventRejected"/>
/// 与结构化日志。从 <c>RealtimeEventDispatcher.RejectEvent</c> 抽取，便于各 handler 共享。
/// </summary>
internal sealed class RealtimeEventRejectionSink
{
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;

    public RealtimeEventRejectionSink(GatewayMetrics metrics, ILogger logger)
    {
        _metrics = metrics;
        _logger = logger;
    }

    public void Reject(RealtimeEvent realtimeEvent, RealtimeRejectReason reason)
    {
        _metrics.RealtimeEventRejected(reason);
        _logger.RealtimeEventRejected(
            realtimeEvent.EventId,
            realtimeEvent.Type.ToString(),
            reason);
    }
}
