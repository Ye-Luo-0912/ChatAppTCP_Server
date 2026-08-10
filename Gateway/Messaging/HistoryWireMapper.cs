using SharedAttachmentRef = ChatApp.Shared.Protocol.Tcp.TcpAttachmentRef;
using SharedReactionSummary = ChatApp.Shared.Protocol.Tcp.MessageReactionSummary;
using RealtimeAttachmentRef = ChatApp.Realtime.Abstractions.Messaging.AttachmentRef;
using RealtimeReactionSummary =
    ChatApp.Realtime.Abstractions.Messaging.History.MessageReactionSummary;
using SharedConversationItem = ChatApp.Shared.Protocol.Tcp.TcpConversationListItem;
using SharedConversationCursor = ChatApp.Shared.Protocol.Tcp.TcpConversationListCursor;
using SharedRelationshipCatchUp = ChatApp.Shared.Protocol.Tcp.RelationshipCatchUp;
using SharedRelationshipChange = ChatApp.Shared.Protocol.Tcp.RelationshipChangeLogEntry;
using RealtimeConversationItem = ChatApp.Realtime.Abstractions.Conversations.ConversationListItem;
using RealtimeConversationCursor = ChatApp.Realtime.Abstractions.Conversations.ConversationListCursor;
using RealtimeRelationshipCatchUp = ChatApp.Realtime.Abstractions.Sync.RelationshipCatchUp;

namespace ChatApp.TcpGateway.Gateway.Messaging;

/// <summary>
/// Explicitly maps the Realtime-owned model to the Client/Gateway TCP schema.
/// The two contracts may evolve independently.
/// </summary>
internal static class HistoryWireMapper
{
    public static IReadOnlyList<SharedAttachmentRef>? MapAttachments(
        IReadOnlyList<RealtimeAttachmentRef>? source)
    {
        if (source is null || source.Count == 0)
        {
            return null;
        }

        var result = new SharedAttachmentRef[source.Count];
        for (var i = 0; i < source.Count; i++)
        {
            var item = source[i];
            result[i] = new SharedAttachmentRef
            {
                RefVersion = item.RefVersion,
                AttachmentId = item.AttachmentId,
                FileName = item.FileName,
                ContentType = item.ContentType,
                SizeBytes = item.SizeBytes,
                Status = (short)item.Status,
                DownloadApiHint = item.DownloadApiHint,
                DownloadToken = item.DownloadToken,
                ThumbnailApiHint = item.ThumbnailApiHint
            };
        }

        return result;
    }

    public static IReadOnlyList<SharedReactionSummary>? MapReactions(
        IReadOnlyList<RealtimeReactionSummary>? source)
    {
        if (source is null || source.Count == 0)
        {
            return null;
        }

        var result = new SharedReactionSummary[source.Count];
        for (var i = 0; i < source.Count; i++)
        {
            var item = source[i];
            result[i] = new SharedReactionSummary
            {
                Emoji = item.Emoji,
                Count = item.Count,
                ReactedByMe = item.ReactedByMe
            };
        }

        return result;
    }

    public static SharedConversationItem[] MapConversations(
        IReadOnlyList<RealtimeConversationItem> source)
    {
        var result = new SharedConversationItem[source.Count];
        for (var i = 0; i < source.Count; i++)
        {
            var item = source[i];
            result[i] = new SharedConversationItem
            {
                ConversationId = item.ConversationId,
                Type = (ChatApp.Shared.Protocol.Tcp.TcpConversationType)item.Type,
                PeerUserId = item.PeerUserId,
                Title = item.Title,
                LastMessageId = item.LastMessageId,
                LastMessagePreview = item.LastMessagePreview,
                LastMessageAtMs = item.LastMessageAtMs,
                LastSenderUserId = item.LastSenderUserId,
                UnreadCount = item.UnreadCount,
                LastReadMessageId = item.LastReadMessageId,
                LastReadAtMs = item.LastReadAtMs,
                IsPinned = item.IsPinned,
                PinnedAtMs = item.PinnedAtMs,
                IsMuted = item.IsMuted,
                MutedUntilMs = item.MutedUntilMs
            };
        }

        return result;
    }

    public static SharedConversationCursor? MapConversationCursor(
        RealtimeConversationCursor? source) => source is null
        ? null
        : new SharedConversationCursor
        {
            IsPinned = source.IsPinned,
            PinnedAtMs = source.PinnedAtMs,
            LastMessageAtMs = source.LastMessageAtMs,
            ConversationId = source.ConversationId
        };

    public static SharedRelationshipCatchUp[]? MapRelationshipCatchUps(
        IReadOnlyList<RealtimeRelationshipCatchUp>? source)
    {
        if (source is null || source.Count == 0)
        {
            return null;
        }

        var result = new SharedRelationshipCatchUp[source.Count];
        for (var i = 0; i < source.Count; i++)
        {
            var catchUp = source[i];
            var changes = new SharedRelationshipChange[catchUp.Changes.Count];
            for (var j = 0; j < catchUp.Changes.Count; j++)
            {
                var change = catchUp.Changes[j];
                changes[j] = new SharedRelationshipChange
                {
                    ChangeSequence = change.ChangeSequence,
                    Operation = (ChatApp.Shared.Protocol.Tcp.TcpRelationshipChangeOperation)change.Operation,
                    ResourceId = change.ResourceId,
                    UserId = change.UserId,
                    Status = change.Status,
                    Message = change.Message,
                    CreatedAtMs = change.CreatedAtMs,
                    OccurredAtMs = change.OccurredAtMs,
                    RequestId = change.RequestId
                };
            }

            result[i] = new SharedRelationshipCatchUp
            {
                ListType = (ChatApp.Shared.Protocol.Tcp.TcpRelationshipListType)catchUp.ListType,
                Changes = changes,
                HasMore = catchUp.HasMore,
                NextCursor = catchUp.NextCursor,
                NextSequence = catchUp.NextSequence,
                RetentionFloorSequence = catchUp.RetentionFloorSequence,
                ResetRequired = catchUp.ResetRequired,
                ResetReason = catchUp.ResetReason
            };
        }

        return result;
    }
}
