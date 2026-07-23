using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Diagnostics;
using ChatApp.TcpGateway.Networking.Buffers;
using ChatApp.TcpGateway.Networking.Sessions;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Messaging;

internal sealed partial class RealtimeEventDispatcher
{
    private readonly UserSessionRegistry _userSessions;
    private readonly IPayloadCodec<ChatMessage> _chatMessageCodec;
    private readonly IPayloadCodec<MessageReceiptUpdate> _messageReceiptUpdateCodec;
    private readonly IPayloadCodec<ConversationChanged> _conversationChangedCodec;
    private readonly IPayloadCodec<UnreadCountChanged> _unreadCountChangedCodec;
    private readonly IPayloadCodec<MessageRecalledUpdate> _messageRecalledUpdateCodec;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RealtimeEventDispatcher> _logger;

    public RealtimeEventDispatcher(
        UserSessionRegistry userSessions,
        IPayloadCodec<ChatMessage> chatMessageCodec,
        IPayloadCodec<MessageReceiptUpdate> messageReceiptUpdateCodec,
        IPayloadCodec<ConversationChanged> conversationChangedCodec,
        IPayloadCodec<UnreadCountChanged> unreadCountChangedCodec,
        IPayloadCodec<MessageRecalledUpdate> messageRecalledUpdateCodec,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger<RealtimeEventDispatcher> logger)
    {
        _userSessions = userSessions;
        _chatMessageCodec = chatMessageCodec;
        _messageReceiptUpdateCodec = messageReceiptUpdateCodec;
        _conversationChangedCodec = conversationChangedCodec;
        _unreadCountChangedCodec = unreadCountChangedCodec;
        _messageRecalledUpdateCodec = messageRecalledUpdateCodec;
        _metrics = metrics;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public void Dispatch(RealtimeEvent realtimeEvent)
    {
        ArgumentNullException.ThrowIfNull(realtimeEvent);

        switch (realtimeEvent.Type)
        {
            case RealtimeEventType.MessageReceived:
                DispatchChatMessage(realtimeEvent);
                break;

            case RealtimeEventType.MessageReceiptUpdated:
                DispatchMessageReceipt(realtimeEvent);
                break;

            case RealtimeEventType.SessionRevoked:
                DispatchSessionRevocation(realtimeEvent);
                break;

            case RealtimeEventType.ConversationListChanged:
                DispatchConversationChanged(realtimeEvent);
                break;

            case RealtimeEventType.UnreadCountChanged:
                DispatchUnreadCountChanged(realtimeEvent);
                break;

            case RealtimeEventType.MessageRecalled:
                DispatchMessageRecalled(realtimeEvent);
                break;

            default:
                _metrics.RealtimeEventHandled(queuedDeliveries: 0);
                LogUnsupportedEvent(
                    _logger,
                    realtimeEvent.EventId,
                    realtimeEvent.Type);
                break;
        }
    }

    private void DispatchChatMessage(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        ChatApp.Realtime.Abstractions.Messaging.RealtimeChatMessagePayload?
            payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeChatMessage(
                realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-payload-json");
            return;
        }

        if (payload is null ||
            string.IsNullOrWhiteSpace(payload.MessageId) ||
            payload.SenderUserId <= 0 ||
            payload.ReceiverUserId <= 0 ||
            (string.IsNullOrWhiteSpace(payload.Content)
             && payload.Attachments is not { Count: > 0 }))
        {
            RejectEvent(realtimeEvent, "invalid-message-payload");
            return;
        }

        var isReceiverTarget = payload.ReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.SenderUserId == realtimeEvent.TargetUserId;
        if (!isReceiverTarget && !isSenderEcho)
        {
            RejectEvent(realtimeEvent, "invalid-message-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(
            realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var message = new ChatMessage
        {
            MessageId = payload.MessageId,
            ConversationId = payload.ConversationId,
            TargetUserId = payload.ReceiverUserId,
            SenderUserId = payload.SenderUserId,
            Content = payload.Content,
            Attachments = AttachmentWireMapper.Map(payload.Attachments),
            ReplyToMessageId = payload.ReplyToMessageId,
            ReplyToSenderUserId = payload.ReplyToSenderUserId,
            ReplyToPreview = payload.ReplyToPreview,
            ForwardedFromMessageId = payload.ForwardedFromMessageId,
            ForwardedFromSenderUserId = payload.ForwardedFromSenderUserId,
            ForwardedFromPreview = payload.ForwardedFromPreview,
            SentUtc = GetSentUtc(payload.ReceivedAtMs)
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ChatMessage,
            _chatMessageCodec,
            message);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            // 发送方回声：跳过产生该消息的来源会话，避免本机重复。
            if (isSenderEcho
                && !string.IsNullOrWhiteSpace(realtimeEvent.SessionId)
                && string.Equals(
                    target.SessionId,
                    realtimeEvent.SessionId,
                    StringComparison.Ordinal))
            {
                continue;
            }

            if (target.TryQueue(outboundFrame))
            {
                queuedDeliveries++;
            }
        }

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchMessageRecalled(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        ChatApp.Realtime.Abstractions.Messaging.RealtimeMessageRecalledPayload?
            payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMessageRecalled(
                realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-payload-json");
            return;
        }

        if (payload is null ||
            string.IsNullOrWhiteSpace(payload.MessageId) ||
            payload.SenderUserId <= 0 ||
            payload.ReceiverUserId <= 0 ||
            payload.RecalledAtMs <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-message-recalled-payload");
            return;
        }

        var isReceiverTarget = payload.ReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.SenderUserId == realtimeEvent.TargetUserId;
        if (!isReceiverTarget && !isSenderEcho)
        {
            RejectEvent(realtimeEvent, "invalid-message-recalled-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(
            realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var update = new MessageRecalledUpdate
        {
            MessageId = payload.MessageId,
            ConversationId = payload.ConversationId,
            SenderUserId = payload.SenderUserId,
            ReceiverUserId = payload.ReceiverUserId,
            RecalledAtMs = payload.RecalledAtMs
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageRecalled,
            _messageRecalledUpdateCodec,
            update);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            // 发送方回声：跳过产生该撤回的来源会话，避免本机重复。
            if (isSenderEcho
                && !string.IsNullOrWhiteSpace(realtimeEvent.SessionId)
                && string.Equals(
                    target.SessionId,
                    realtimeEvent.SessionId,
                    StringComparison.Ordinal))
            {
                continue;
            }

            if (target.TryQueue(outboundFrame))
            {
                queuedDeliveries++;
            }
        }

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchMessageReceipt(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-receipt-payload");
            return;
        }

        ChatApp.Realtime.Abstractions.Messaging.RealtimeMessageReceiptPayload?
            payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMessageReceipt(
                realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-receipt-json");
            return;
        }

        if (payload is null ||
            string.IsNullOrWhiteSpace(payload.MessageId) ||
            payload.ReceiverUserId <= 0 ||
            !Enum.IsDefined(payload.ReceiptType) ||
            realtimeEvent.TargetUserId <= 0 ||
            realtimeEvent.ActorUserId != payload.ReceiverUserId ||
            !string.Equals(
                realtimeEvent.MessageId,
                payload.MessageId,
                StringComparison.Ordinal))
        {
            RejectEvent(realtimeEvent, "invalid-receipt-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(
            realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var update = new MessageReceiptUpdate
        {
            MessageId = payload.MessageId,
            ReceiverUserId = payload.ReceiverUserId,
            State = (MessageReceiptState)(byte)payload.ReceiptType,
            OccurredUtc = GetSentUtc(payload.OccurredAtMs)
        };
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageReceiptUpdated,
            _messageReceiptUpdateCodec,
            update);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            if (target.TryQueue(outboundFrame))
                queuedDeliveries++;
        }

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchConversationChanged(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-conversation-payload");
            return;
        }

        ChatApp.Realtime.Abstractions.Conversations.RealtimeConversationChangedPayload?
            payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeConversationChanged(
                realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-conversation-json");
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || realtimeEvent.TargetUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-conversation-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var update = new ConversationChanged
        {
            ConversationId = payload.ConversationId,
            Type = (ConversationType)(byte)payload.Type,
            PeerUserId = payload.PeerUserId,
            LastMessageId = payload.LastMessageId,
            LastMessagePreview = payload.LastMessagePreview,
            LastMessageAtMs = payload.LastMessageAtMs,
            LastSenderUserId = payload.LastSenderUserId,
            IsPinned = payload.IsPinned,
            IsMuted = payload.IsMuted,
            MutedUntilMs = payload.MutedUntilMs
        };
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ConversationChanged,
            _conversationChangedCodec,
            update);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            if (target.TryQueue(outboundFrame))
                queuedDeliveries++;
        }

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchUnreadCountChanged(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-unread-payload");
            return;
        }

        ChatApp.Realtime.Abstractions.Conversations.RealtimeUnreadCountChangedPayload?
            payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeUnreadCountChanged(
                realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-unread-json");
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || realtimeEvent.TargetUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-unread-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var update = new UnreadCountChanged
        {
            ConversationId = payload.ConversationId,
            UnreadCount = payload.UnreadCount,
            LastReadMessageId = payload.LastReadMessageId,
            LastReadAtMs = payload.LastReadAtMs
        };
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.UnreadCountChanged,
            _unreadCountChangedCodec,
            update);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            if (target.TryQueue(outboundFrame))
                queuedDeliveries++;
        }

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchSessionRevocation(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.SessionId))
        {
            RejectEvent(realtimeEvent, "missing-session-id");
            return;
        }

        var closedSessions = 0;
        foreach (var session in _userSessions.GetSnapshot(
                     realtimeEvent.TargetUserId))
        {
            if (!string.Equals(
                    session.SessionId,
                    realtimeEvent.SessionId,
                    StringComparison.Ordinal))
            {
                continue;
            }

            session.Close(SessionCloseReason.SessionRevoked);
            closedSessions++;
        }

        _metrics.RealtimeEventHandled(closedSessions);
    }

    private DateTime GetSentUtc(long receivedAtMs)
    {
        try
        {
            return DateTimeOffset
                .FromUnixTimeMilliseconds(receivedAtMs)
                .UtcDateTime;
        }
        catch (ArgumentOutOfRangeException)
        {
            return _timeProvider.GetUtcNow().UtcDateTime;
        }
    }

    private void RejectEvent(
        RealtimeEvent realtimeEvent,
        string reason)
    {
        _metrics.RealtimeEventRejected(reason);
        LogRejectedEvent(
            _logger,
            realtimeEvent.EventId,
            realtimeEvent.Type,
            reason);
    }

    [LoggerMessage(
        EventId = 40,
        Level = LogLevel.Warning,
        Message = "Rejected realtime event {EventId} ({EventType}); reason: {Reason}.")]
    private static partial void LogRejectedEvent(
        ILogger logger,
        string eventId,
        RealtimeEventType eventType,
        string reason);

    [LoggerMessage(
        EventId = 41,
        Level = LogLevel.Debug,
        Message = "Realtime event {EventId} ({EventType}) has no TCP wire mapping and was acknowledged.")]
    private static partial void LogUnsupportedEvent(
        ILogger logger,
        string eventId,
        RealtimeEventType eventType);
}
