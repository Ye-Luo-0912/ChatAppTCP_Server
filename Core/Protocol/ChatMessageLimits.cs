namespace ChatApp.TcpGateway.Core.Protocol;

/// <summary>Chat 上行载荷的廉价结构上限（完整语义校验仍在反序列化之后）。</summary>
public static class ChatMessageLimits
{
    public const int MaxAttachments = 32;
    public const int MaxAttachmentIdLength = 64;
    public const int MaxClientMessageIdLength = 128;
    public const int MaxReplyToMessageIdLength = 64;
    public const int MaxReplyPreviewLength = 256;
    public const int MaxForwardedFromMessageIdLength = 64;
    public const int MaxForwardedFromPreviewLength = 256;

    /// <summary>单条消息 @ 用户数上限（仅群聊生效）。</summary>
    public const int MaxMentionedUserIds = 50;

    /// <summary>单个 @ 角色字符串长度上限（如 "all"、"admin"）。</summary>
    public const int MaxMentionedRoleLength = 32;

    /// <summary>单条消息 @ 角色数上限。</summary>
    public const int MaxMentionedRoles = 10;
}
