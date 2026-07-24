using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Networking.Sessions;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging;

internal sealed partial class RealtimeEventDispatcher
{
    private readonly UserSessionRegistry _userSessions;
    private readonly IPayloadCodec<ChatMessage> _chatMessageCodec;
    private readonly IPayloadCodec<MessageReceiptUpdate> _messageReceiptUpdateCodec;
    private readonly IPayloadCodec<ConversationChanged> _conversationChangedCodec;
    private readonly IPayloadCodec<UnreadCountChanged> _unreadCountChangedCodec;
    private readonly IPayloadCodec<ConversationReadUpdate> _conversationReadUpdateCodec;
    private readonly IPayloadCodec<MessageRecalledUpdate> _messageRecalledUpdateCodec;
    private readonly IPayloadCodec<MessageEditedUpdate> _messageEditedUpdateCodec;
    private readonly IPayloadCodec<ReactionAddedUpdate> _reactionAddedUpdateCodec;
    private readonly IPayloadCodec<ReactionRemovedUpdate> _reactionRemovedUpdateCodec;
    private readonly IPayloadCodec<MemberJoinedUpdate> _memberJoinedUpdateCodec;
    private readonly IPayloadCodec<MemberLeftUpdate> _memberLeftUpdateCodec;
    private readonly IPayloadCodec<MemberRemovedUpdate> _memberRemovedUpdateCodec;
    private readonly IPayloadCodec<RoleChangedUpdate> _roleChangedUpdateCodec;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RealtimeEventDispatcher> _logger;

    public RealtimeEventDispatcher(
        UserSessionRegistry userSessions,
        IPayloadCodec<ChatMessage> chatMessageCodec,
        IPayloadCodec<MessageReceiptUpdate> messageReceiptUpdateCodec,
        IPayloadCodec<ConversationChanged> conversationChangedCodec,
        IPayloadCodec<UnreadCountChanged> unreadCountChangedCodec,
        IPayloadCodec<ConversationReadUpdate> conversationReadUpdateCodec,
        IPayloadCodec<MessageRecalledUpdate> messageRecalledUpdateCodec,
        IPayloadCodec<MessageEditedUpdate> messageEditedUpdateCodec,
        IPayloadCodec<ReactionAddedUpdate> reactionAddedUpdateCodec,
        IPayloadCodec<ReactionRemovedUpdate> reactionRemovedUpdateCodec,
        IPayloadCodec<MemberJoinedUpdate> memberJoinedUpdateCodec,
        IPayloadCodec<MemberLeftUpdate> memberLeftUpdateCodec,
        IPayloadCodec<MemberRemovedUpdate> memberRemovedUpdateCodec,
        IPayloadCodec<RoleChangedUpdate> roleChangedUpdateCodec,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger<RealtimeEventDispatcher> logger)
    {
        _userSessions = userSessions;
        _chatMessageCodec = chatMessageCodec;
        _messageReceiptUpdateCodec = messageReceiptUpdateCodec;
        _conversationChangedCodec = conversationChangedCodec;
        _unreadCountChangedCodec = unreadCountChangedCodec;
        _conversationReadUpdateCodec = conversationReadUpdateCodec;
        _messageRecalledUpdateCodec = messageRecalledUpdateCodec;
        _messageEditedUpdateCodec = messageEditedUpdateCodec;
        _reactionAddedUpdateCodec = reactionAddedUpdateCodec;
        _reactionRemovedUpdateCodec = reactionRemovedUpdateCodec;
        _memberJoinedUpdateCodec = memberJoinedUpdateCodec;
        _memberLeftUpdateCodec = memberLeftUpdateCodec;
        _memberRemovedUpdateCodec = memberRemovedUpdateCodec;
        _roleChangedUpdateCodec = roleChangedUpdateCodec;
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

            case RealtimeEventType.ConversationRead:
                DispatchConversationRead(realtimeEvent);
                break;

            case RealtimeEventType.MessageRecalled:
                DispatchMessageRecalled(realtimeEvent);
                break;

            case RealtimeEventType.MessageEdited:
                DispatchMessageEdited(realtimeEvent);
                break;

            case RealtimeEventType.ReactionAdded:
                DispatchReactionAdded(realtimeEvent);
                break;

            case RealtimeEventType.ReactionRemoved:
                DispatchReactionRemoved(realtimeEvent);
                break;

            case RealtimeEventType.MemberJoined:
                DispatchMemberJoined(realtimeEvent);
                break;

            case RealtimeEventType.MemberLeft:
                DispatchMemberLeft(realtimeEvent);
                break;

            case RealtimeEventType.MemberRemoved:
                DispatchMemberRemoved(realtimeEvent);
                break;

            case RealtimeEventType.RoleChanged:
                DispatchRoleChanged(realtimeEvent);
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
            (string.IsNullOrWhiteSpace(payload.Content)
             && payload.Attachments is not { Count: > 0 }))
        {
            RejectEvent(realtimeEvent, "invalid-message-payload");
            return;
        }

        var isGroup = !string.IsNullOrWhiteSpace(payload.ConversationId)
                      && Realtime.Abstractions.Conversations.ConversationId.IsGroup(
                          payload.ConversationId);
        if (!isGroup && payload.ReceiverUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-message-payload");
            return;
        }

        var isReceiverTarget = payload.ReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.SenderUserId == realtimeEvent.TargetUserId;
        if (!isGroup && !isReceiverTarget && !isSenderEcho)
        {
            RejectEvent(realtimeEvent, "invalid-message-payload");
            return;
        }

        if (isGroup && realtimeEvent.TargetUserId <= 0)
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
            SentUtc = GetSentUtc(payload.ReceivedAtMs)
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ChatMessage,
            _chatMessageCodec,
            message);

        var skipOriginSession = isSenderEcho || isGroup;
        var queuedDeliveries = targets.Where(target => !skipOriginSession || string.IsNullOrWhiteSpace(realtimeEvent.SessionId) || !string.Equals(target.SessionId, realtimeEvent.SessionId, StringComparison.Ordinal)).Count(target => target.TryQueue(outboundFrame));

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
            payload.RecalledAtMs <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-message-recalled-payload");
            return;
        }

        var isGroup = !string.IsNullOrWhiteSpace(payload.ConversationId)
                      && Realtime.Abstractions.Conversations.ConversationId.IsGroup(
                          payload.ConversationId);
        if (!isGroup && payload.ReceiverUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-message-recalled-payload");
            return;
        }

        var isReceiverTarget = payload.ReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.SenderUserId == realtimeEvent.TargetUserId;
        if (!isGroup && !isReceiverTarget && !isSenderEcho)
        {
            RejectEvent(realtimeEvent, "invalid-message-recalled-payload");
            return;
        }

        if (isGroup && realtimeEvent.TargetUserId <= 0)
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

        var skipOrigin = isSenderEcho || isGroup;
        var queuedDeliveries = targets.Where(target => !skipOrigin || string.IsNullOrWhiteSpace(realtimeEvent.SessionId) || !string.Equals(target.SessionId, realtimeEvent.SessionId, StringComparison.Ordinal)).Count(target => target.TryQueue(outboundFrame));

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchMessageEdited(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        ChatApp.Realtime.Abstractions.Messaging.RealtimeMessageEditedPayload?
            payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMessageEdited(
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
            payload.EditVersion < 1 ||
            payload.EditedAtMs <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-message-edited-payload");
            return;
        }

        var isGroup = !string.IsNullOrWhiteSpace(payload.ConversationId)
                      && Realtime.Abstractions.Conversations.ConversationId.IsGroup(
                          payload.ConversationId);
        if (!isGroup && payload.ReceiverUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-message-edited-payload");
            return;
        }

        var isReceiverTarget = payload.ReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.SenderUserId == realtimeEvent.TargetUserId;
        if (!isGroup && !isReceiverTarget && !isSenderEcho)
        {
            RejectEvent(realtimeEvent, "invalid-message-edited-payload");
            return;
        }

        if (isGroup && realtimeEvent.TargetUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-message-edited-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(
            realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var update = new MessageEditedUpdate
        {
            MessageId = payload.MessageId,
            ConversationId = payload.ConversationId,
            SenderUserId = payload.SenderUserId,
            ReceiverUserId = payload.ReceiverUserId,
            Content = payload.Content,
            EditVersion = payload.EditVersion,
            EditedAtMs = payload.EditedAtMs
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.MessageEdited,
            _messageEditedUpdateCodec,
            update);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            if ((isSenderEcho || isGroup)
                && !string.IsNullOrWhiteSpace(realtimeEvent.SessionId)
                && string.Equals(
                    target.SessionId,
                    realtimeEvent.SessionId,
                    StringComparison.Ordinal))
            {
                continue;
            }

            if (target.TryQueue(outboundFrame))
                queuedDeliveries++;
        }

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchReactionAdded(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        ChatApp.Realtime.Abstractions.Messaging.RealtimeReactionAddedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeReactionAdded(
                realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-payload-json");
            return;
        }

        if (payload is null ||
            string.IsNullOrWhiteSpace(payload.MessageId) ||
            string.IsNullOrWhiteSpace(payload.Emoji) ||
            payload.ReactorUserId <= 0 ||
            payload.MessageSenderUserId <= 0 ||
            payload.MessageReceiverUserId <= 0 ||
            payload.OccurredAtMs <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-reaction-added-payload");
            return;
        }

        var isReceiverTarget = payload.MessageReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.MessageSenderUserId == realtimeEvent.TargetUserId;
        if (!isReceiverTarget && !isSenderEcho)
        {
            RejectEvent(realtimeEvent, "invalid-reaction-added-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var update = new ReactionAddedUpdate
        {
            MessageId = payload.MessageId,
            ConversationId = payload.ConversationId,
            ReactorUserId = payload.ReactorUserId,
            MessageSenderUserId = payload.MessageSenderUserId,
            MessageReceiverUserId = payload.MessageReceiverUserId,
            Emoji = payload.Emoji,
            EmojiCount = payload.EmojiCount,
            OccurredAtMs = payload.OccurredAtMs
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ReactionAdded,
            _reactionAddedUpdateCodec,
            update);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            if (payload.ReactorUserId == realtimeEvent.TargetUserId
                && !string.IsNullOrWhiteSpace(realtimeEvent.SessionId)
                && string.Equals(
                    target.SessionId,
                    realtimeEvent.SessionId,
                    StringComparison.Ordinal))
            {
                continue;
            }

            if (target.TryQueue(outboundFrame))
                queuedDeliveries++;
        }

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchReactionRemoved(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        ChatApp.Realtime.Abstractions.Messaging.RealtimeReactionRemovedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeReactionRemoved(
                realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-payload-json");
            return;
        }

        if (payload is null ||
            string.IsNullOrWhiteSpace(payload.MessageId) ||
            string.IsNullOrWhiteSpace(payload.Emoji) ||
            payload.ReactorUserId <= 0 ||
            payload.MessageSenderUserId <= 0 ||
            payload.MessageReceiverUserId <= 0 ||
            payload.OccurredAtMs <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-reaction-removed-payload");
            return;
        }

        var isReceiverTarget = payload.MessageReceiverUserId == realtimeEvent.TargetUserId;
        var isSenderEcho = payload.MessageSenderUserId == realtimeEvent.TargetUserId;
        if (!isReceiverTarget && !isSenderEcho)
        {
            RejectEvent(realtimeEvent, "invalid-reaction-removed-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var update = new ReactionRemovedUpdate
        {
            MessageId = payload.MessageId,
            ConversationId = payload.ConversationId,
            ReactorUserId = payload.ReactorUserId,
            MessageSenderUserId = payload.MessageSenderUserId,
            MessageReceiverUserId = payload.MessageReceiverUserId,
            Emoji = payload.Emoji,
            EmojiCount = payload.EmojiCount,
            OccurredAtMs = payload.OccurredAtMs
        };

        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ReactionRemoved,
            _reactionRemovedUpdateCodec,
            update);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            if (payload.ReactorUserId == realtimeEvent.TargetUserId
                && !string.IsNullOrWhiteSpace(realtimeEvent.SessionId)
                && string.Equals(
                    target.SessionId,
                    realtimeEvent.SessionId,
                    StringComparison.Ordinal))
            {
                continue;
            }

            if (target.TryQueue(outboundFrame))
                queuedDeliveries++;
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

        var queuedDeliveries = targets.Count(target => target.TryQueue(outboundFrame));

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchConversationChanged(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-conversation-payload");
            return;
        }

        Realtime.Abstractions.Conversations.RealtimeConversationChangedPayload?
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
            Title = payload.Title,
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

        Realtime.Abstractions.Conversations.RealtimeUnreadCountChangedPayload?
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

    private void DispatchConversationRead(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-conversation-read-payload");
            return;
        }

        Realtime.Abstractions.Conversations.RealtimeConversationReadPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeConversationRead(
                realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-conversation-read-json");
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || string.IsNullOrWhiteSpace(payload.LastReadMessageId)
            || payload.ReaderUserId <= 0
            || payload.LastReadAtMs <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-conversation-read-payload");
            return;
        }

        var targets = _userSessions.GetSnapshot(realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        var update = new ConversationReadUpdate
        {
            ConversationId = payload.ConversationId,
            ReaderUserId = payload.ReaderUserId,
            LastReadMessageId = payload.LastReadMessageId,
            LastReadAtMs = payload.LastReadAtMs
        };
        using var outboundFrame = OutboundFrameFactory.Create(
            PacketCommand.ConversationRead,
            _conversationReadUpdateCodec,
            update);

        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            if (target.TryQueue(outboundFrame))
                queuedDeliveries++;
        }

        _metrics.RealtimeEventHandled(queuedDeliveries);
    }

    private void DispatchMemberJoined(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        Realtime.Abstractions.Conversations.RealtimeMemberJoinedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMemberJoined(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-payload-json");
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.UserId <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-member-joined-payload");
            return;
        }

        DispatchMemberFrame(
            realtimeEvent,
            PacketCommand.MemberJoined,
            _memberJoinedUpdateCodec,
            new MemberJoinedUpdate
            {
                ConversationId = payload.ConversationId,
                UserId = payload.UserId,
                Role = (ConversationMemberRole)(byte)payload.Role,
                ActorUserId = payload.ActorUserId,
                Title = payload.Title,
                OccurredAtMs = payload.OccurredAtMs
            });
    }

    private void DispatchMemberLeft(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        Realtime.Abstractions.Conversations.RealtimeMemberLeftPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMemberLeft(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-payload-json");
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.UserId <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-member-left-payload");
            return;
        }

        DispatchMemberFrame(
            realtimeEvent,
            PacketCommand.MemberLeft,
            _memberLeftUpdateCodec,
            new MemberLeftUpdate
            {
                ConversationId = payload.ConversationId,
                UserId = payload.UserId,
                OccurredAtMs = payload.OccurredAtMs
            });
    }

    private void DispatchMemberRemoved(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        Realtime.Abstractions.Conversations.RealtimeMemberRemovedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeMemberRemoved(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-payload-json");
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.UserId <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-member-removed-payload");
            return;
        }

        DispatchMemberFrame(
            realtimeEvent,
            PacketCommand.MemberRemoved,
            _memberRemovedUpdateCodec,
            new MemberRemovedUpdate
            {
                ConversationId = payload.ConversationId,
                UserId = payload.UserId,
                ActorUserId = payload.ActorUserId,
                OccurredAtMs = payload.OccurredAtMs
            });
    }

    private void DispatchRoleChanged(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            RejectEvent(realtimeEvent, "missing-payload");
            return;
        }

        Realtime.Abstractions.Conversations.RealtimeRoleChangedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeRoleChanged(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            RejectEvent(realtimeEvent, "invalid-payload-json");
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || payload.UserId <= 0
            || realtimeEvent.TargetUserId <= 0)
        {
            RejectEvent(realtimeEvent, "invalid-role-changed-payload");
            return;
        }

        DispatchMemberFrame(
            realtimeEvent,
            PacketCommand.RoleChanged,
            _roleChangedUpdateCodec,
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
            });
    }

    private void DispatchMemberFrame<T>(
        RealtimeEvent realtimeEvent,
        PacketCommand command,
        IPayloadCodec<T> codec,
        T update)
    {
        var targets = _userSessions.GetSnapshot(realtimeEvent.TargetUserId);
        if (targets.Length == 0)
        {
            _metrics.RealtimeEventHandled(queuedDeliveries: 0);
            return;
        }

        using var outboundFrame = OutboundFrameFactory.Create(command, codec, update);
        var queuedDeliveries = 0;
        foreach (var target in targets)
        {
            if (!string.IsNullOrWhiteSpace(realtimeEvent.SessionId)
                && string.Equals(target.SessionId, realtimeEvent.SessionId, StringComparison.Ordinal))
            {
                continue;
            }

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
