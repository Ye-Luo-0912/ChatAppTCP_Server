namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>输入状态推送（网关 → 客户端）。</summary>
public sealed class TypingUpdate
{
    public long SenderUserId { get; set; }
    public string? ConversationId { get; set; }
    public bool IsTyping { get; set; }
}
