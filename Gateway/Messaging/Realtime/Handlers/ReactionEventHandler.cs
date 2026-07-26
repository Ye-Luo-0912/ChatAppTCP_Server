using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Observability.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 反应事件处理器（ReactionAdded / ReactionRemoved）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。两个事件结构对称，仅在 PacketCommand/codec/DTO 上不同。
/// 跳过来源会话条件：reactor == target（即用户在自己接收端会话上不重复接收自己的反应）。
/// </para>
/// </summary>
internal sealed class ReactionEventHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<ReactionAddedUpdate> _reactionAddedCodec;
    private readonly IPayloadCodec<ReactionRemovedUpdate> _reactionRemovedCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;

    public ReactionEventHandler(
        IPayloadCodec<ReactionAddedUpdate> reactionAddedCodec,
        IPayloadCodec<ReactionRemovedUpdate> reactionRemovedCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection)
    {
        _reactionAddedCodec = reactionAddedCodec;
        _reactionRemovedCodec = reactionRemovedCodec;
        _delivery = delivery;
        _rejection = rejection;
    }

    public void Handle(RealtimeEvent realtimeEvent)
    {
        switch (realtimeEvent.Type)
        {
            case RealtimeEventType.ReactionAdded:
                HandleReactionAdded(realtimeEvent);
                return;
            case RealtimeEventType.ReactionRemoved:
                HandleReactionRemoved(realtimeEvent);
                return;
        }
    }

    private void HandleReactionAdded(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeReactionAddedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeReactionAdded(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.MessageId)
            || string.IsNullOrWhiteSpace(payload.Emoji)
            || payload.ReactorUserId <= 0
            || payload.MessageSenderUserId <= 0
            || payload.MessageReceiverUserId <= 0
            || payload.OccurredAtMs <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var isReceiverTarget = payload.MessageReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.MessageSenderUserId == realtimeEvent.TargetUserId;
        if (!isReceiverTarget && !isSenderEcho)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        // 跳过来源会话条件：reactor == target（与原 DispatchReactionAdded 完全等价）。
        // 注意：必须保留 realtimeEvent.SessionId 非空检查，否则会错误跳过多设备同 SessionId 的会话。
        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.ReactionAdded,
            _reactionAddedCodec,
            new ReactionAddedUpdate
            {
                MessageId = payload.MessageId,
                ConversationId = payload.ConversationId,
                ReactorUserId = payload.ReactorUserId,
                MessageSenderUserId = payload.MessageSenderUserId,
                MessageReceiverUserId = payload.MessageReceiverUserId,
                Emoji = payload.Emoji,
                EmojiCount = payload.EmojiCount,
                OccurredAtMs = payload.OccurredAtMs
            },
            skipOriginSession: payload.ReactorUserId == realtimeEvent.TargetUserId);
    }

    private void HandleReactionRemoved(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeReactionRemovedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeReactionRemoved(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.MessageId)
            || string.IsNullOrWhiteSpace(payload.Emoji)
            || payload.ReactorUserId <= 0
            || payload.MessageSenderUserId <= 0
            || payload.MessageReceiverUserId <= 0
            || payload.OccurredAtMs <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var isReceiverTarget = payload.MessageReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.MessageSenderUserId == realtimeEvent.TargetUserId;
        if (!isReceiverTarget && !isSenderEcho)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.ReactionRemoved,
            _reactionRemovedCodec,
            new ReactionRemovedUpdate
            {
                MessageId = payload.MessageId,
                ConversationId = payload.ConversationId,
                ReactorUserId = payload.ReactorUserId,
                MessageSenderUserId = payload.MessageSenderUserId,
                MessageReceiverUserId = payload.MessageReceiverUserId,
                Emoji = payload.Emoji,
                EmojiCount = payload.EmojiCount,
                OccurredAtMs = payload.OccurredAtMs
            },
            skipOriginSession: payload.ReactorUserId == realtimeEvent.TargetUserId);
    }
}
