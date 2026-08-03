using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Observability.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 会话聚合事件处理器（MembersAdded / ConversationDissolved）。
/// <para>
/// P0-6：这两个事件均携带 <see cref="RealtimeEvent.TargetUserIds"/> 多目标列表，
/// 走 <see cref="RealtimeEventDeliveryHelper.DeliverAggregated{TUpdate}"/> 聚合 fanout，
/// 与 <see cref="ConversationMemberEventHandler"/> 的单目标 <c>Deliver</c> 区分。
/// </para>
/// </summary>
internal sealed class ConversationAggregateEventHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<MembersAddedUpdate> _membersAddedCodec;
    private readonly IPayloadCodec<ConversationDissolvedUpdate> _conversationDissolvedCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;

    public ConversationAggregateEventHandler(
        IPayloadCodec<MembersAddedUpdate> membersAddedCodec,
        IPayloadCodec<ConversationDissolvedUpdate> conversationDissolvedCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection)
    {
        _membersAddedCodec = membersAddedCodec;
        _conversationDissolvedCodec = conversationDissolvedCodec;
        _delivery = delivery;
        _rejection = rejection;
    }

    public ValueTask HandleAsync(
        RealtimeEvent realtimeEvent,
        CancellationToken ct = default)
    {
        switch (realtimeEvent.Type)
        {
            case RealtimeEventType.MembersAdded:
                HandleMembersAdded(realtimeEvent);
                break;
            case RealtimeEventType.ConversationDissolved:
                HandleConversationDissolved(realtimeEvent);
                break;
        }
        return ValueTask.CompletedTask;
    }

    private void HandleMembersAdded(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeMembersAddedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMembersAdded(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.Members is null
            || payload.Members.Count == 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var addedUserIds = new long[payload.Members.Count];
        for (var i = 0; i < payload.Members.Count; i++)
            addedUserIds[i] = payload.Members[i].UserId;

        _delivery.DeliverAggregated(
            realtimeEvent,
            PacketCommand.MembersAddedUpdate,
            _membersAddedCodec,
            new MembersAddedUpdate
            {
                ConversationId = payload.ConversationId,
                AddedUserIds = addedUserIds,
                ActorUserId = payload.ActorUserId,
                Title = payload.Title,
                OccurredAtMs = payload.OccurredAtMs
            },
            skipOriginSession: true);
    }

    private void HandleConversationDissolved(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeConversationDissolvedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeConversationDissolved(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.DissolvedAtMs <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.DeliverAggregated(
            realtimeEvent,
            PacketCommand.ConversationDissolvedUpdate,
            _conversationDissolvedCodec,
            new ConversationDissolvedUpdate
            {
                ConversationId = payload.ConversationId,
                ActorUserId = payload.ActorUserId ?? 0,
                OccurredAtMs = payload.DissolvedAtMs
            },
            skipOriginSession: true);
    }
}
