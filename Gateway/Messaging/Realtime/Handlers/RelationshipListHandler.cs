using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 关系列表变更事件处理器（FriendRequestListChanged / FriendListChanged / BlockedListChanged）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。payload 为 <see cref="RealtimeDomainNotificationPayload"/>，
/// 通过 <see cref="GatewayJsonSerializerContext"/> 直接反序列化（不走 RealtimeWireSerializer）。
/// codec 未注入（测试场景）时静默跳过并记 0 入队指标。
/// </para>
/// <para>
/// 好友关系变更（<c>friendship</c>）与拉黑变更（<c>blocked-user</c>）会主动失效
/// Typing/Presence 授权缓存的双向条目，避免缓存窗口（默认 30s/10s）内继续允许
/// 已禁止的瞬态通知。好友请求列表变更（<c>friend-request</c>）不失效缓存，
/// 因为请求未建立关系，不影响授权结果。
/// </para>
/// </summary>
internal sealed class RelationshipListHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<RelationshipListChangedUpdate>? _relationshipListCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;
    private readonly GatewayMetrics _metrics;
    private readonly IDirectConversationAuthorizer? _authorizer;

    public RelationshipListHandler(
        IPayloadCodec<RelationshipListChangedUpdate>? relationshipListCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection,
        GatewayMetrics metrics,
        IDirectConversationAuthorizer? authorizer = null)
    {
        _relationshipListCodec = relationshipListCodec;
        _delivery = delivery;
        _rejection = rejection;
        _metrics = metrics;
        _authorizer = authorizer;
    }

    public void Handle(RealtimeEvent realtimeEvent)
    {
        if (_relationshipListCodec is null)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeDomainNotificationPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize(
                realtimeEvent.PayloadJson,
                GatewayJsonSerializerContext.Default.RealtimeDomainNotificationPayload);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.Resource)
            || string.IsNullOrWhiteSpace(payload.Action)
            || realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        // 主动失效 Typing/Presence 授权缓存。
        // 仅 friendship / blocked-user 变更影响授权；friend-request 未建立关系，不失效。
        // ActorUserId 可能缺失（系统事件），缺失时跳过失效（依赖 TTL 兜底）。
        var actorUserId = realtimeEvent.ActorUserId ?? 0;
        var targetUserId = realtimeEvent.TargetUserId;
        if (_authorizer is not null
            && actorUserId > 0
            && targetUserId > 0
            && actorUserId != targetUserId
            && IsAuthorizationAffectingChange(payload.Resource))
        {
            // 双向失效：(actor, target) 与 (target, actor)。
            // TryRemove 是原子同步操作，ValueTask 通常已完成；通过 AsTask() 转为 Task
            // 以满足 CA2012（ValueTask 必须被消费）并允许安全 fire-and-forget。
            _ = _authorizer.InvalidateAsync(actorUserId, targetUserId, CancellationToken.None)
                .AsTask();
            _ = _authorizer.InvalidateAsync(targetUserId, actorUserId, CancellationToken.None)
                .AsTask();
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.RelationshipListChanged,
            _relationshipListCodec,
            new RelationshipListChangedUpdate
            {
                Resource = payload.Resource,
                Action = payload.Action,
                ResourceId = payload.ResourceId,
                ActorUserId = actorUserId,
                Message = payload.Message,
                OccurredAtMs = realtimeEvent.OccurredAtMs
            },
            skipOriginSession: false);
    }

    /// <summary>
    /// 判断资源类型是否影响 Typing/Presence 授权。
    /// friendship / blocked-user 变更会改变授权结果；friend-request 未建立关系，不影响。
    /// </summary>
    private static bool IsAuthorizationAffectingChange(string resource) =>
        string.Equals(resource, "friendship", StringComparison.Ordinal)
        || string.Equals(resource, "blocked-user", StringComparison.Ordinal);
}
