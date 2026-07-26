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
/// 消息回执事件处理器（MessageReceiptUpdated）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。独特校验：ActorUserId == ReceiverUserId 且
/// envelope.MessageId == payload.MessageId。无跳过来源会话（多设备同步需要回执）。
/// </para>
/// </summary>
internal sealed class MessageReceiptHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<MessageReceiptUpdate> _messageReceiptCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;
    private readonly RealtimeTimestampConverter _timestampConverter;

    public MessageReceiptHandler(
        IPayloadCodec<MessageReceiptUpdate> messageReceiptCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection,
        RealtimeTimestampConverter timestampConverter)
    {
        _messageReceiptCodec = messageReceiptCodec;
        _delivery = delivery;
        _rejection = rejection;
        _timestampConverter = timestampConverter;
    }

    public void Handle(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeMessageReceiptPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMessageReceipt(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.MessageId)
            || payload.ReceiverUserId <= 0
            || !Enum.IsDefined(payload.ReceiptType)
            || realtimeEvent.TargetUserId <= 0
            || realtimeEvent.ActorUserId != payload.ReceiverUserId
            || !string.Equals(
                realtimeEvent.MessageId,
                payload.MessageId,
                StringComparison.Ordinal))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.MessageReceiptUpdated,
            _messageReceiptCodec,
            new MessageReceiptUpdate
            {
                MessageId = payload.MessageId,
                ReceiverUserId = payload.ReceiverUserId,
                State = (MessageReceiptState)(byte)payload.ReceiptType,
                OccurredUtc = _timestampConverter.ToUtc(payload.OccurredAtMs)
            },
            skipOriginSession: false);
    }
}
