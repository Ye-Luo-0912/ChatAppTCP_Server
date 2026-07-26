using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
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
/// </summary>
internal sealed class RelationshipListHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<RelationshipListChangedUpdate>? _relationshipListCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;
    private readonly GatewayMetrics _metrics;

    public RelationshipListHandler(
        IPayloadCodec<RelationshipListChangedUpdate>? relationshipListCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection,
        GatewayMetrics metrics)
    {
        _relationshipListCodec = relationshipListCodec;
        _delivery = delivery;
        _rejection = rejection;
        _metrics = metrics;
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

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.RelationshipListChanged,
            _relationshipListCodec,
            new RelationshipListChangedUpdate
            {
                Resource = payload.Resource,
                Action = payload.Action,
                ResourceId = payload.ResourceId,
                ActorUserId = realtimeEvent.ActorUserId ?? 0,
                Message = payload.Message,
                OccurredAtMs = realtimeEvent.OccurredAtMs
            },
            skipOriginSession: false);
    }
}
