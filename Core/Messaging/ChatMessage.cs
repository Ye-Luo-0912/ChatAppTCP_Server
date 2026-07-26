namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class ChatMessage
{
    public string? MessageId { get; set; }

    /// <summary>
    /// 稳定会话编号。下行由 Realtime 填充；上行可缺省（服务端按双方用户派生）。
    /// </summary>
    public string? ConversationId { get; set; }

    public long TargetUserId { get; set; }
    public long SenderUserId { get; set; }
    public string? Content { get; set; }
    public DateTime SentUtc { get; set; }

    /// <summary>上行：已确认附件 Id（绑定到本条消息）。</summary>
    public IReadOnlyList<string>? AttachmentIds { get; set; }

    /// <summary>下行：附件引用（含 DownloadApiHint，非公网 URL）。</summary>
    public IReadOnlyList<AttachmentRef>? Attachments { get; set; }

    /// <summary>被回复消息 Id（上下行）。</summary>
    public string? ReplyToMessageId { get; set; }

    /// <summary>被回复消息发送方（上下行）。</summary>
    public long? ReplyToSenderUserId { get; set; }

    /// <summary>被回复内容预览（上下行，最长 256）。</summary>
    public string? ReplyToPreview { get; set; }

    /// <summary>被转发消息 Id（上下行）。</summary>
    public string? ForwardedFromMessageId { get; set; }

    /// <summary>被转发消息原发送方（上下行）。</summary>
    public long? ForwardedFromSenderUserId { get; set; }

    /// <summary>被转发内容预览（上下行，最长 256）。</summary>
    public string? ForwardedFromPreview { get; set; }

    /// <summary>@提到的用户 Id 列表（群聊场景下使用）。</summary>
    public IReadOnlyList<long>? MentionedUserIds { get; set; }

    /// <summary>@提到的角色（如 "all"、"admin"）；目前仅供展示，无强校验。</summary>
    public IReadOnlyList<string>? MentionedRoles { get; set; }
}
