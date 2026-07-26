using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Observability.Logging;
using ConversationMemberRole = ChatApp.TcpGateway.Core.Messaging.Conversations.ConversationMemberRole;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 会话成员管理事件处理器（MemberJoined / MemberLeft / MemberRemoved / RoleChanged）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。4 个事件共用 <see cref="Delivery.Deliver{TUpdate}"/>
/// 走"跳过来源 SessionId"的单目标 fanout，与原 <c>DispatchMemberFrame&lt;T&gt;</c> 行为完全等价。
/// </para>
/// </summary>
internal sealed class ConversationMemberEventHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<MemberJoinedUpdate> _memberJoinedCodec;
    private readonly IPayloadCodec<MemberLeftUpdate> _memberLeftCodec;
    private readonly IPayloadCodec<MemberRemovedUpdate> _memberRemovedCodec;
    private readonly IPayloadCodec<RoleChangedUpdate> _roleChangedCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;

    public ConversationMemberEventHandler(
        IPayloadCodec<MemberJoinedUpdate> memberJoinedCodec,
        IPayloadCodec<MemberLeftUpdate> memberLeftCodec,
        IPayloadCodec<MemberRemovedUpdate> memberRemovedCodec,
        IPayloadCodec<RoleChangedUpdate> roleChangedCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection)
    {
        _memberJoinedCodec = memberJoinedCodec;
        _memberLeftCodec = memberLeftCodec;
        _memberRemovedCodec = memberRemovedCodec;
        _roleChangedCodec = roleChangedCodec;
        _delivery = delivery;
        _rejection = rejection;
    }

    public void Handle(RealtimeEvent realtimeEvent)
    {
        switch (realtimeEvent.Type)
        {
            case RealtimeEventType.MemberJoined:
                HandleMemberJoined(realtimeEvent);
                return;
            case RealtimeEventType.MemberLeft:
                HandleMemberLeft(realtimeEvent);
                return;
            case RealtimeEventType.MemberRemoved:
                HandleMemberRemoved(realtimeEvent);
                return;
            case RealtimeEventType.RoleChanged:
                HandleRoleChanged(realtimeEvent);
                return;
        }
    }

    private void HandleMemberJoined(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeMemberJoinedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMemberJoined(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.UserId <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.MemberJoined,
            _memberJoinedCodec,
            new MemberJoinedUpdate
            {
                ConversationId = payload.ConversationId,
                UserId = payload.UserId,
                Role = (ConversationMemberRole)(byte)payload.Role,
                ActorUserId = payload.ActorUserId,
                Title = payload.Title,
                OccurredAtMs = payload.OccurredAtMs
            },
            skipOriginSession: true);
    }

    private void HandleMemberLeft(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeMemberLeftPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMemberLeft(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.UserId <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.MemberLeft,
            _memberLeftCodec,
            new MemberLeftUpdate
            {
                ConversationId = payload.ConversationId,
                UserId = payload.UserId,
                OccurredAtMs = payload.OccurredAtMs
            },
            skipOriginSession: true);
    }

    private void HandleMemberRemoved(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeMemberRemovedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMemberRemoved(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.UserId <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.MemberRemoved,
            _memberRemovedCodec,
            new MemberRemovedUpdate
            {
                ConversationId = payload.ConversationId,
                UserId = payload.UserId,
                ActorUserId = payload.ActorUserId,
                OccurredAtMs = payload.OccurredAtMs
            },
            skipOriginSession: true);
    }

    private void HandleRoleChanged(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeRoleChangedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeRoleChanged(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.UserId <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.RoleChanged,
            _roleChangedCodec,
            new RoleChangedUpdate
            {
                ConversationId = payload.ConversationId,
                UserId = payload.UserId,
                NewRole = (ConversationMemberRole)(byte)payload.NewRole,
                PreviousRole = payload.PreviousRole is null
                    ? null
                    : (ConversationMemberRole)(byte)payload.PreviousRole.Value,
                ActorUserId = payload.ActorUserId,
                OccurredAtMs = payload.OccurredAtMs
            },
            skipOriginSession: true);
    }
}
