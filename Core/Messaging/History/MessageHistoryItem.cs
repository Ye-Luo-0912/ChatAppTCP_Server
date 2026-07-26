namespace ChatApp.TcpGateway.Core.Messaging.History;

public sealed class MessageHistoryItem
{
    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public long SenderUserId { get; init; }
    public long ReceiverUserId { get; init; }
    public string? ConversationId { get; init; }
    public required string Content { get; init; }
    public long ReceivedAtMs { get; init; }
    public long? DeliveredAtMs { get; init; }
    public long? ReadAtMs { get; init; }
    public long? RecalledAtMs { get; init; }
    public int EditVersion { get; init; } = 1;
    public long? EditedAtMs { get; init; }
    public long ChangedAtMs { get; init; }
    public bool IsEdited => EditVersion > 1 || EditedAtMs is > 0;

    public IReadOnlyList<AttachmentRef>? Attachments { get; init; }
    public IReadOnlyList<MessageReactionSummary>? Reactions { get; init; }

    public string? ReplyToMessageId { get; init; }
    public long? ReplyToSenderUserId { get; init; }
    public string? ReplyToPreview { get; init; }

    public string? ForwardedFromMessageId { get; init; }
    public long? ForwardedFromSenderUserId { get; init; }
    public string? ForwardedFromPreview { get; init; }

    /// <summary>@提到的用户 Id 列表（群聊场景下使用）。</summary>
    public IReadOnlyList<long>? MentionedUserIds { get; init; }

    /// <summary>@提到的角色（如 "all"、"admin"）；目前仅供展示，无强校验。</summary>
    public IReadOnlyList<string>? MentionedRoles { get; init; }
}
