using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Serialization;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Observability.Logging;
namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime.Handlers;

/// <summary>
/// 会话列表/未读/已读通知事件处理器（ConversationListChanged / UnreadCountChanged / ConversationRead）。
/// <para>
/// 从 <c>RealtimeEventDispatcher</c> 抽取。3 个事件共用 <see cref="Delivery.Deliver{TUpdate}"/>
/// 走"不跳过来源"的单目标 fanout，与原实现完全等价。
/// </para>
/// </summary>
internal sealed class ConversationNotificationHandler : IRealtimeEventHandler
{
    private readonly IPayloadCodec<ConversationChanged> _conversationChangedCodec;
    private readonly IPayloadCodec<UnreadCountChanged> _unreadCountChangedCodec;
    private readonly IPayloadCodec<ConversationReadUpdate> _conversationReadCodec;
    private readonly RealtimeEventDeliveryHelper _delivery;
    private readonly RealtimeEventRejectionSink _rejection;

    public ConversationNotificationHandler(
        IPayloadCodec<ConversationChanged> conversationChangedCodec,
        IPayloadCodec<UnreadCountChanged> unreadCountChangedCodec,
        IPayloadCodec<ConversationReadUpdate> conversationReadCodec,
        RealtimeEventDeliveryHelper delivery,
        RealtimeEventRejectionSink rejection)
    {
        _conversationChangedCodec = conversationChangedCodec;
        _unreadCountChangedCodec = unreadCountChangedCodec;
        _conversationReadCodec = conversationReadCodec;
        _delivery = delivery;
        _rejection = rejection;
    }

    public async ValueTask HandleAsync(
        RealtimeEvent realtimeEvent,
        CancellationToken ct = default)
    {
        switch (realtimeEvent.Type)
        {
            case RealtimeEventType.ConversationListChanged:
                HandleConversationChanged(realtimeEvent);
                return;
            case RealtimeEventType.UnreadCountChanged:
                HandleUnreadCountChanged(realtimeEvent);
                return;
            case RealtimeEventType.ConversationRead:
                await HandleConversationReadAsync(realtimeEvent, ct).ConfigureAwait(false);
                return;
        }
    }

    private void HandleConversationChanged(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeConversationChangedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeConversationChanged(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.ConversationChanged,
            _conversationChangedCodec,
            new ConversationChanged
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
            },
            skipOriginSession: false);
    }

    private void HandleUnreadCountChanged(RealtimeEvent realtimeEvent)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeUnreadCountChangedPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeUnreadCountChanged(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.UnreadCountChanged,
            _unreadCountChangedCodec,
            new UnreadCountChanged
            {
                ConversationId = payload.ConversationId,
                UnreadCount = payload.UnreadCount,
                LastReadMessageId = payload.LastReadMessageId,
                LastReadAtMs = payload.LastReadAtMs
            },
            skipOriginSession: false);
    }

    private async ValueTask HandleConversationReadAsync(
        RealtimeEvent realtimeEvent,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(realtimeEvent.PayloadJson))
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.MissingPayload);
            return;
        }

        RealtimeConversationReadPayload? payload;
        try
        {
            payload = RealtimeWireSerializer.DeserializeConversationRead(realtimeEvent.PayloadJson);
        }
        catch (JsonException)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidJson);
            return;
        }

        if (payload is null
            || string.IsNullOrWhiteSpace(payload.ConversationId)
            || string.IsNullOrWhiteSpace(payload.LastReadMessageId)
            || payload.ReaderUserId <= 0
            || payload.LastReadAtMs <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        var conversationRead = new ConversationReadUpdate
        {
            ConversationId = payload.ConversationId,
            ReaderUserId = payload.ReaderUserId,
            LastReadMessageId = payload.LastReadMessageId,
            LastReadAtMs = payload.LastReadAtMs
        };

        // P1-2：群已读回执走会话级广播（AudienceKind=Conversation），成员集合由
        // ConversationAudienceCache 解析，并跳过 ExcludeUserId（读者本人）。
        if (realtimeEvent.AudienceKind == AudienceKind.Conversation)
        {
            await _delivery.DeliverToConversationAudienceAsync(
                realtimeEvent,
                PacketCommand.ConversationRead,
                _conversationReadCodec,
                conversationRead,
                skipOriginSession: false,
                ct)
                .ConfigureAwait(false);
            return;
        }

        if (realtimeEvent.TargetUserId <= 0)
        {
            _rejection.Reject(realtimeEvent, RealtimeRejectReason.InvalidPayload);
            return;
        }

        _delivery.Deliver(
            realtimeEvent,
            PacketCommand.ConversationRead,
            _conversationReadCodec,
            conversationRead,
            skipOriginSession: false);
    }
}
