using ChatApp.Realtime.Abstractions.Events;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 会话吊销事件处理器（SessionRevoked）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。行为独特：不构造出站帧，而是遍历目标用户的本机会话，
/// 关闭 SessionId 匹配的会话（<see cref="SessionCloseReason.SessionRevoked"/>）。
/// 与其他 handler 不共享 fanout 路径。
/// </para>
/// </summary>
internal sealed class SessionRevocationHandler : IRealtimeEventHandler
{
    private readonly UserSessionRegistry _userSessions;
    private readonly GatewayMetrics _metrics;
    private readonly RealtimeEventRejectionSink _rejection;

    public SessionRevocationHandler(
        UserSessionRegistry userSessions,
        GatewayMetrics metrics,
        RealtimeEventRejectionSink rejection)
    {
        _userSessions = userSessions;
        _metrics = metrics;
        _rejection = rejection;
    }

    public void Handle(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.SessionId))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingSessionId);
            return;
        }

        var closedSessions = 0;
        foreach (var session in _userSessions.GetSnapshot(realtimeEvent.TargetUserId))
        {
            if (!string.Equals(
                    session.SessionId,
                    realtimeEvent.SessionId,
                    StringComparison.Ordinal))
            {
                continue;
            }

            session.Close(SessionCloseReason.SessionRevoked);
            closedSessions++;
        }

        _metrics.RealtimeEventHandled(closedSessions);
    }
}
