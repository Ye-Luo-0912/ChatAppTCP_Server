using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 同设备会话替换与 ResumeToken 撤销逻辑。
/// <para>
/// 当同一用户+设备的新连接进来时，旧会话必须被踢下线：
/// <list type="bullet">
/// <item>本机旧连接通过 <see cref="UserSessionRegistry.TakeOverSameDevice"/> 立即关闭；</item>
/// <item>跨 Gateway 的旧连接通过 Redis 设备租约 <c>TakeOverAsync</c> 发现，
///   再通过 <see cref="IRealtimeMessageBus.PublishEventAsync"/> 广播
///   <see cref="RealtimeEventType.SessionRevoked"/> 事件由目标 Gateway 关闭。</item>
/// </list>
/// </para>
/// <para>
/// 撤销旧会话的 <c>ResumeToken</c> 是关键安全步骤：防止被替换的旧会话在 Token TTL 窗口内
/// 凭此 Token 跨网关恢复。撤销是尽力而为，Redis 故障不阻断关闭流程。
/// </para>
/// </summary>
internal sealed partial class SessionLifecycleCoordinator
{
    private async ValueTask ReplaceSameDeviceSessionsAsync(
        TcpClientSession incoming,
        CancellationToken cancellationToken)
    {
        // 1) 本机旧连接立即踢下线。
        var localVictims = _userSessions.TakeOverSameDevice(incoming);
        var occurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        foreach (var victim in localVictims)
            await RevokeSessionAsync(victim, occurredAtMs, cancellationToken).ConfigureAwait(false);

        // 2) Redis/Garnet 设备租约：发现跨 Gateway 的旧 SessionId 并广播 SessionRevoked。
        if (incoming.DeviceIdHash is not { } deviceHash
            || string.IsNullOrWhiteSpace(incoming.SessionId)
            || incoming.UserId <= 0)
        {
            return;
        }

        // TTL 略长于空闲超时，避免正常心跳间隙丢租约；断开时 ReleaseIfOwner。
        var leaseTtl = _options.IdleTimeout + TimeSpan.FromMinutes(5);
        DeviceLeaseTakeoverResult? takeover;
        try
        {
            // 传入 ConnectionLeaseId 作为所有权令牌。
            takeover = await _deviceSessionLeaseStore
                .TakeOverAsync(
                    incoming.UserId,
                    deviceHash,
                    incoming.SessionId,
                    incoming.ConnectionLeaseId,
                    leaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.SessionRevocationFailed(
                incoming.ConnectionId,
                incoming.SessionId,
                exception);
            return;
        }

        // P0-7：按 ConnectionLeaseId 判断是否存在跨 Gateway 旧连接需要吊销。
        // 仅比较 SessionId 会在 Resume 复用 SessionId 时漏发吊销事件。
        if (takeover is not { } t
            || string.IsNullOrWhiteSpace(t.PreviousConnectionLeaseId)
            || string.Equals(
                t.PreviousConnectionLeaseId,
                incoming.ConnectionLeaseId,
                StringComparison.Ordinal))
        {
            return;
        }

        // 本机已踢过的连接不必再发；按 ConnectionLeaseId 判断（比 SessionId 更精确）。
        var alreadyLocal = localVictims.Any(v =>
            string.Equals(v.ConnectionLeaseId, t.PreviousConnectionLeaseId, StringComparison.Ordinal));
        if (alreadyLocal)
            return;

        await PublishSessionRevokedEventAsync(
            incoming.UserId,
            !string.IsNullOrWhiteSpace(t.PreviousSessionId) ? t.PreviousSessionId! : incoming.SessionId!,
            occurredAtMs,
            incoming.ConnectionId,
            t.PreviousConnectionLeaseId,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 发布 <see cref="RealtimeEventType.SessionRevoked"/> 事件，通知所有 Gateway
    /// 关闭指定 SessionId 的旧连接。Resume 路径接管设备租约后也调用此方法，
    /// 以确保跨 Gateway 的旧连接及时关闭（与 <see cref="ReplaceSameDeviceSessionsAsync"/>
    /// 行为一致）。
    /// <para>
    /// P0-7：<paramref name="connectionLeaseId"/> 携带旧连接的租约 ID，写入 PayloadJson，
    /// 供目标 Gateway 的 <c>SessionRevocationHandler</c> 按 ConnectionLeaseId 精确匹配旧连接，
    /// 避免在 SessionId 相同时（Resume 复用）误关新连接。
    /// </para>
    /// <para>
    /// Best-effort：发布失败仅记录日志，不阻断调用方流程。NATS/Realtime bus 故障时
    /// 旧连接依赖设备租约 TTL 自然失效。
    /// </para>
    /// </summary>
    /// <param name="userId">用户 Id。</param>
    /// <param name="revokedSessionId">被吊销的 SessionId。</param>
    /// <param name="occurredAtMs">事件发生时间（Unix 毫秒）。</param>
    /// <param name="reportingConnectionId">用于日志追溯的当前连接 Id。</param>
    /// <param name="connectionLeaseId">旧连接的 ConnectionLeaseId，写入 PayloadJson 供精确匹配。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    private async ValueTask PublishSessionRevokedEventAsync(
        long userId,
        string revokedSessionId,
        long occurredAtMs,
        uint reportingConnectionId,
        string? connectionLeaseId,
        CancellationToken cancellationToken)
    {
        try
        {
            await _messageBus
                .PublishEventAsync(
                    new RealtimeEvent
                    {
                        EventId = SessionEventIdFactory.CreateSessionRevokedEventId(
                            userId,
                            revokedSessionId,
                            occurredAtMs),
                        Type = RealtimeEventType.SessionRevoked,
                        TargetUserId = userId,
                        SessionId = revokedSessionId,
                        // P0-7：携带旧连接的 ConnectionLeaseId 供目标 Gateway 精确匹配，
                        // 避免在 SessionId 相同时（Resume 复用原 SessionId）误关新连接。
                        PayloadJson = connectionLeaseId,
                        OccurredAtMs = occurredAtMs
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.SessionRevocationFailed(
                reportingConnectionId,
                revokedSessionId,
                exception);
        }
    }

    private async ValueTask RevokeSessionAsync(
        TcpClientSession victim,
        long occurredAtMs,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(victim.SessionId))
        {
            await RevokeResumeTokenSafeAsync(victim, cancellationToken).ConfigureAwait(false);
            victim.Close(SessionCloseReason.SessionRevoked);
            return;
        }

        // 撤销旧会话的 ResumeToken，防止其在 TTL 窗口内被用于恢复。
        // 必须在 Close 之前执行：Close 后 session 对象仍可访问 CurrentResumeToken。
        await RevokeResumeTokenSafeAsync(victim, cancellationToken).ConfigureAwait(false);

        await PublishSessionRevokedEventAsync(
            victim.UserId,
            victim.SessionId!,
            occurredAtMs,
            victim.ConnectionId,
            victim.ConnectionLeaseId,
            cancellationToken).ConfigureAwait(false);

        // 本机立即断开；跨 Gateway 实例依赖 SessionRevoked 事件。
        victim.Close(SessionCloseReason.SessionRevoked);
    }

    /// <summary>
    /// 尽力撤销会话的 ResumeToken。Redis 故障不阻断会话吊销流程，
    /// Token 将在其 TTL 到期后自然失效。
    /// </summary>
    private async ValueTask RevokeResumeTokenSafeAsync(
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (_resumeTokenStore is null)
            return;

        var token = session.CurrentResumeToken;
        if (string.IsNullOrWhiteSpace(token))
            return;

        try
        {
            await _resumeTokenStore
                .RevokeAsync(token, cancellationToken)
                .ConfigureAwait(false);
            session.CurrentResumeToken = null;
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.ResumeTokenRevoke,
                ex);
        }
    }
}
