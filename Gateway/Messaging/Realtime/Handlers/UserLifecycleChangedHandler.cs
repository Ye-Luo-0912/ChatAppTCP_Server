using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 三-3：用户生命周期变更事件处理器（UserLifecycleChanged）。
/// <para>
/// 行为与 <see cref="SessionRevocationHandler"/> 类似：不构造出站帧，而是更新
/// <see cref="IFrozenUserCache"/> 并关闭冻结用户的活跃会话。
/// </para>
/// <para>
/// <b>Frozen</b>：标记缓存 + 遍历本机会话关闭（<see cref="SessionCloseReason.AccountSuspended"/>）。
/// <b>Active（解冻）</b>：清除缓存标记，不关闭会话（用户可重新认证）。
/// </para>
/// </summary>
internal sealed partial class UserLifecycleChangedHandler : IRealtimeEventHandler
{
    private readonly IFrozenUserCache _frozenUserCache;
    private readonly UserSessionRegistry _userSessions;
    private readonly GatewayMetrics _metrics;
    private readonly RealtimeEventRejectionSink _rejection;
    private readonly ILogger _logger;

    public UserLifecycleChangedHandler(
        IFrozenUserCache frozenUserCache,
        UserSessionRegistry userSessions,
        GatewayMetrics metrics,
        RealtimeEventRejectionSink rejection,
        ILogger logger)
    {
        _frozenUserCache = frozenUserCache;
        _userSessions = userSessions;
        _metrics = metrics;
        _rejection = rejection;
        _logger = logger;
    }

    public void Handle(RealtimeEvent realtimeEvent)
    {
        if (realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeUserLifecycleChangedPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize(
                realtimeEvent.PayloadJson,
                GatewayJsonSerializerContext.Default.RealtimeUserLifecycleChangedPayload);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var userId = realtimeEvent.TargetUserId;

        if (payload.NewState == UserLifecycleState.Frozen)
        {
            // 标记缓存：后续认证/Resume 路径快速拒绝。
            _frozenUserCache.MarkFrozen(userId, payload.ChangedAtMs);

            // 关闭冻结用户的所有本机活跃会话。
            var closedSessions = 0;
            foreach (var session in _userSessions.GetSnapshot(userId))
            {
                session.Close(SessionCloseReason.AccountSuspended);
                closedSessions++;
            }

            LogUserFrozen(userId, closedSessions);
            _metrics.RealtimeEventHandled(closedSessions);
        }
        else if (payload.NewState == UserLifecycleState.Active)
        {
            // 解冻：清除缓存标记。用户可重新认证。
            _frozenUserCache.MarkUnfrozen(userId);
            LogUserUnfrozen(userId);
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
        }
        else
        {
            // Deleting / Deleted 等其他状态：不更新缓存（由 tombstone 路径处理）。
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
        }
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "用户 {UserId} 已冻结，关闭 {ClosedSessions} 个活跃会话")]
    private partial void LogUserFrozen(long userId, int closedSessions);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Information,
        Message = "用户 {UserId} 已解冻，清除冻结缓存")]
    private partial void LogUserUnfrozen(long userId);
}
