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
}
