namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// 输入状态通知（客户端 → 网关）。
/// 本机 UserSessionRegistry 扇出 + NATS Core ephemeral 跨网关（不进 Outbox）。
/// </summary>
public sealed class TypingNotify
{
    /// <summary>私聊会话 Id（dm:lo:hi）；服务端以此为权威源解析目标用户并校验发送方为会话成员。</summary>
    public string? ConversationId { get; set; }

    public bool IsTyping { get; set; }
}
