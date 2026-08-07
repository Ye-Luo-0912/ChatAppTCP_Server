using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 全局在线路由租约、Presence 广播与发布逻辑。
/// <para>
/// 在线路由租约始终维护；仅在启用 Presence/Typing 且全局状态转换
/// （0→1 或 1→0）时广播与发布跨网关 Presence 事件。
/// 旧实现每实例本地首连/断开都无条件广播，导致多实例登录时互相覆盖、误报下线。
/// </para>
/// <para>
/// 本地广播使用 Ephemeral keyed mailbox：同一用户的在线状态可覆盖，
/// 避免瞬态 Presence 帧淹没慢消费者。
/// </para>
/// </summary>
internal sealed partial class SessionLifecycleCoordinator
{
    /// <summary>
    /// 始终维护全局在线路由租约；只在启用 Presence/Typing 且全局状态转换
    /// （0→1 或 1→0）时广播与发布跨网关 Presence 事件。
    /// 旧实现每实例本地首连/断开都无条件广播，导致多实例登录时互相覆盖、误报下线。
    /// </summary>
    private async Task UpdateGlobalPresenceAsync(
        long userId,
        bool isOnline,
        CancellationToken cancellationToken)
    {
        PresenceTransition transition;
        if (isOnline)
            transition = await _globalPresence
                .SetOnlineAsync(userId, _integrationOptions.InstanceId, cancellationToken)
                .ConfigureAwait(false);
        else
            transition = await _globalPresence
                .SetOfflineAsync(userId, _integrationOptions.InstanceId, cancellationToken)
                .ConfigureAwait(false);

        if (transition == PresenceTransition.None)
        {
            _metrics.PresenceTransition("none");
            return;
        }

        var globalIsOnline = transition == PresenceTransition.WentOnline;
        _metrics.PresenceTransition(globalIsOnline ? "online" : "offline");

        // The global presence ZSET is also the authoritative sharded-event routing
        // directory. It must be maintained even when optional Presence/Typing fanout
        // is disabled; only the user-visible notification remains feature-gated.
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        BroadcastPresenceChangedLocal(userId, globalIsOnline);

        try
        {
            await _messageBus
                .PublishEphemeralPresenceAsync(
                    new EphemeralPresenceEvent
                    {
                        OriginInstanceId = _integrationOptions.InstanceId,
                        UserId = userId,
                        IsOnline = globalIsOnline
                    },
                    cancellationToken)
                .ConfigureAwait(false);
            _metrics.PresenceEphemeralPublished();
        }
        catch (Exception ex)
        {
            _metrics.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralPresencePublish);
            _logger.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralPresencePublish,
                ex);
        }
    }

    private void BroadcastPresenceChangedLocal(long userId, bool isOnline)
    {
        var watchers = _presenceWatchers.GetWatchers(userId);
        if (watchers.Length == 0)
        {
            _metrics.PresenceFanoutSkipped();
            return;
        }

        var update = new PresenceChanged
        {
            UserId = userId,
            IsOnline = isOnline
        };

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.PresenceChanged,
            _presenceChangedCodec,
            update);
        // Key = UserId：同一用户的在线状态可覆盖。
        var key = EphemeralKey.Presence(userId);
        var recipientCount = 0;
        foreach (var watcherId in watchers)
        {
            foreach (var watcherSession in _userSessions.GetSnapshot(watcherId))
            {
                watcherSession.TryQueueEphemeral(frame, key);
                recipientCount++;
            }
        }
        _metrics.PresenceFanoutDelivered(recipientCount);
    }
}
