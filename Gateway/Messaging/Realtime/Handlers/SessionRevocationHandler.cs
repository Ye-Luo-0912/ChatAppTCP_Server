using ChatApp.Realtime.Abstractions.Events;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 会话吊销事件处理器（SessionRevoked）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。行为独特：不构造出站帧，而是遍历目标用户的本机会话，
/// 关闭 SessionId 匹配的会话（<see cref="SessionCloseReason.SessionRevoked"/>）。
/// 与其他 handler 不共享 fanout 路径。
/// </para>
/// <para>
/// 关闭匹配会话前会尽力撤销其 ResumeToken（若 <see cref="IResumeTokenStore"/> 已注入），
/// 防止被替换的旧会话在 Token TTL 窗口内凭此 Token 跨网关复活。
/// </para>
/// </summary>
internal sealed class SessionRevocationHandler : IRealtimeEventHandler
{
    private readonly UserSessionRegistry _userSessions;
    private readonly GatewayMetrics _metrics;
    private readonly RealtimeEventRejectionSink _rejection;
    private readonly IResumeTokenStore? _resumeTokenStore;
    private readonly ILogger _logger;

    public SessionRevocationHandler(
        UserSessionRegistry userSessions,
        GatewayMetrics metrics,
        RealtimeEventRejectionSink rejection,
        ILogger logger,
        IResumeTokenStore? resumeTokenStore = null)
    {
        _userSessions = userSessions;
        _metrics = metrics;
        _rejection = rejection;
        _logger = logger;
        _resumeTokenStore = resumeTokenStore;
    }

    public void Handle(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.SessionId))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingSessionId);
            return;
        }

        // P0-7：优先按 ConnectionLeaseId 精确匹配（PayloadJson 携带旧连接的 lease id），
        // 避免在 SessionId 相同时（Resume 复用原 SessionId）误关新连接。
        // PayloadJson 为空时回退到 SessionId 匹配（兼容未携带 lease id 的旧事件）。
        var targetLeaseId = realtimeEvent.PayloadJson;
        var matchByLeaseId = !string.IsNullOrWhiteSpace(targetLeaseId);

        var closedSessions = 0;
        foreach (var session in _userSessions.GetSnapshot(realtimeEvent.TargetUserId))
        {
            if (matchByLeaseId)
            {
                if (!string.Equals(
                        session.ConnectionLeaseId,
                        targetLeaseId,
                        StringComparison.Ordinal))
                {
                    continue;
                }
            }
            else if (!string.Equals(
                        session.SessionId,
                        realtimeEvent.SessionId,
                        StringComparison.Ordinal))
            {
                continue;
            }

            // 撤销 ResumeToken：尽力而为，Redis 故障不阻断关闭。
            // fire-and-forget：SessionRevoked 是单向事件，不应因 Redis 延迟阻塞 dispatcher。
            // Token 在 TTL 到期后自然失效，此处撤销只是加速失效。
            // 异常通过 RevokeResumeTokenSafeAsync 观测并记录，避免静默吞没 Redis 故障。
            if (_resumeTokenStore is not null
                && !string.IsNullOrWhiteSpace(session.CurrentResumeToken))
            {
                var token = session.CurrentResumeToken;
                session.CurrentResumeToken = null;
                _ = RevokeResumeTokenSafeAsync(_resumeTokenStore, token);
            }

            session.Close(SessionCloseReason.SessionRevoked);
            closedSessions++;
        }

        _metrics.RealtimeEventHandled(closedSessions);
    }

    /// <summary>
    /// 尽力撤销 ResumeToken，所有异常（同步与异步）均观测并记录。
    /// 复用 <c>PublishEphemeralTypingSafeAsync</c> 模式，确保 fire-and-forget 不产生未观测异常。
    /// </summary>
    private async Task RevokeResumeTokenSafeAsync(IResumeTokenStore store, string token)
    {
        try
        {
            await store.RevokeAsync(token, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _metrics.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.ResumeTokenRevoke);
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.ResumeTokenRevoke,
                ex);
        }
    }
}
