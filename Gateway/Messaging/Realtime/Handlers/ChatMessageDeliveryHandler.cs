using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Messaging;
using ChatApp.TcpGateway.Observability.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 聊天消息下发处理器（MessageReceived）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。包含两条 fanout 路径：
/// </para>
/// <list type="bullet">
///   <item>聚合多目标：envelope.TargetUserIds 非空时遍历所有目标本机会话（群聊扇出），记录放大系数指标。</item>
///   <item>单目标：走 <see cref="RealtimeEventDeliveryHelper.Deliver{TUpdate}"/>。</item>
/// </list>
/// <para>
/// 跳过来源会话条件：isSenderEcho || isGroup（与原 DispatchChatMessage 完全等价）。
/// </para>
/// </summary>
internal sealed class ChatMessageDeliveryHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<ChatMessage> _chatMessageCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;
    private readonly RealtimeTimestampConverter _timestampConverter;

    public ChatMessageDeliveryHandler(
        IPayloadCodec<ChatMessage> chatMessageCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection,
        RealtimeTimestampConverter timestampConverter)
    {
        _chatMessageCodec = chatMessageCodec;
        _delivery = delivery;
        _rejection = rejection;
        _timestampConverter = timestampConverter;
    }

    public async ValueTask HandleAsync(
        RealtimeEvent realtimeEvent,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeChatMessagePayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeChatMessage(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.MessageId)
            || string.IsNullOrWhiteSpace(payload.ClientMessageId)
            || payload.SenderUserId <= 0
            || (string.IsNullOrWhiteSpace(payload.Content)
                && payload.Attachments is not { Count: > 0 }))
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

        // P1-2：会话级广播（AudienceKind=Conversation）时 TargetUserId 可为 0，
        // 成员集合由 ConversationAudienceCache 解析，跳过按 TargetUserId 的语义校验。
        var isConversationAudience = realtimeEvent.AudienceKind == AudienceKind.Conversation;
        var isSenderEcho = false;
        if (!isConversationAudience)
        {
            var isReceiverTarget = payload.ReceiverUserId == realtimeEvent.TargetUserId;
            isSenderEcho = payload.SenderUserId == realtimeEvent.TargetUserId;
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
        }

        var message = new ChatMessage
        {
            MessageId = payload.MessageId,
            ClientMessageId = payload.ClientMessageId,
            ConversationId = payload.ConversationId,
            TargetUserId = isGroup ? realtimeEvent.TargetUserId : payload.ReceiverUserId,
            SenderUserId = payload.SenderUserId,
            Content = payload.Content,
            Attachments = AttachmentWireMapper.Map(payload.Attachments),
            ReplyToMessageId = payload.ReplyToMessageId,
            ReplyToSenderUserId = payload.ReplyToSenderUserId,
            ReplyToPreview = payload.ReplyToPreview,
            ForwardedFromMessageId = payload.ForwardedFromMessageId,
            ForwardedFromSenderUserId = payload.ForwardedFromSenderUserId,
            ForwardedFromPreview = payload.ForwardedFromPreview,
            MentionedUserIds = payload.MentionedUserIds,
            MentionedRoles = payload.MentionedRoles,
            SentUtc = _timestampConverter.ToUtc(payload.ReceivedAtMs)
        };

        var skipOriginSession = isSenderEcho || isGroup;

        // P1-2：会话级广播优先走 audience 解析投递。
        if (isConversationAudience)
        {
            await _delivery.DeliverToConversationAudienceAsync(
                realtimeEvent,
                PacketCommand.ChatMessage,
                _chatMessageCodec,
                message,
                skipOriginSession,
                ct)
                .ConfigureAwait(false);
            return;
        }

        // 聚合群聊事件优先：envelope.TargetUserIds 非空时走多目标 fanout。
        if (realtimeEvent.TargetUserIds is { Length: > 0 })
        {
            _delivery.DeliverAggregated(
                realtimeEvent,
                PacketCommand.ChatMessage,
                _chatMessageCodec,
                message,
                payload.SenderUserId);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.ChatMessage,
            _chatMessageCodec,
            message,
            skipOriginSession);
    }
}
