using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Observability.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 消息生命周期事件处理器（MessageRecalled / MessageEdited）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。两个事件共用 isGroup/isReceiverTarget/isSenderEcho 校验块，
/// 跳过来源会话条件：isSenderEcho || isGroup（与原实现完全等价）。
/// </para>
/// </summary>
internal sealed class MessageLifecycleEventHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<MessageRecalledUpdate> _messageRecalledCodec;
    private readonly IPayloadCodec<MessageEditedUpdate> _messageEditedCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;

    public MessageLifecycleEventHandler(
        IPayloadCodec<MessageRecalledUpdate> messageRecalledCodec,
        IPayloadCodec<MessageEditedUpdate> messageEditedCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection)
    {
        _messageRecalledCodec = messageRecalledCodec;
        _messageEditedCodec = messageEditedCodec;
        _delivery = delivery;
        _rejection = rejection;
    }

    public ValueTask HandleAsync(
        RealtimeEvent realtimeEvent,
        CancellationToken ct = default)
    {
        switch (realtimeEvent.Type)
        {
            case RealtimeEventType.MessageRecalled:
                HandleMessageRecalled(realtimeEvent);
                break;
            case RealtimeEventType.MessageEdited:
                HandleMessageEdited(realtimeEvent);
                break;
        }
        return ValueTask.CompletedTask;
    }

    private void HandleMessageRecalled(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeMessageRecalledPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMessageRecalled(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.MessageId)
            || payload.SenderUserId <= 0
            || payload.RecalledAtMs <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var isGroup = !string.IsNullOrWhiteSpace(payload.ConversationId)
                      && ConversationId.IsGroup(payload.ConversationId);
        if (!isGroup && payload.ReceiverUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var isReceiverTarget = payload.ReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.SenderUserId == realtimeEvent.TargetUserId;
        if (!isGroup && !isReceiverTarget && !isSenderEcho)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        if (isGroup && realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.MessageRecalled,
            _messageRecalledCodec,
            new MessageRecalledUpdate
            {
                MessageId = payload.MessageId,
                ConversationId = payload.ConversationId,
                SenderUserId = payload.SenderUserId,
                ReceiverUserId = payload.ReceiverUserId,
                RecalledAtMs = payload.RecalledAtMs
            },
            skipOriginSession: isSenderEcho || isGroup);
    }

    private void HandleMessageEdited(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeMessageEditedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMessageEdited(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.MessageId)
            || payload.SenderUserId <= 0
            || payload.EditVersion < 1
            || payload.EditedAtMs <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var isGroup = !string.IsNullOrWhiteSpace(payload.ConversationId)
                      && ConversationId.IsGroup(payload.ConversationId);
        if (!isGroup && payload.ReceiverUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var isReceiverTarget = payload.ReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.SenderUserId == realtimeEvent.TargetUserId;
        if (!isGroup && !isReceiverTarget && !isSenderEcho)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        if (isGroup && realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.MessageEdited,
            _messageEditedCodec,
            new MessageEditedUpdate
            {
                MessageId = payload.MessageId,
                ConversationId = payload.ConversationId,
                SenderUserId = payload.SenderUserId,
                ReceiverUserId = payload.ReceiverUserId,
                Content = payload.Content,
                EditVersion = payload.EditVersion,
                EditedAtMs = payload.EditedAtMs
            },
            skipOriginSession: isSenderEcho || isGroup);
    }
}
