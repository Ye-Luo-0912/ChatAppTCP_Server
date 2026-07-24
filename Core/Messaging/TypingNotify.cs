namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// 输入状态通知（客户端 → 网关）。
/// 本机 UserSessionRegistry 扇出 + NATS Core ephemeral 跨网关（不进 Outbox）。
/// </summary>
public sealed class TypingNotify
{
    public long TargetUserId { get; set; }

    /// <summary>私聊会话 Id（dm:lo:hi）；服务端校验发送方与目标均为会话成员。</summary>
    public string? ConversationId { get; set; }

    public bool IsTyping { get; set; }
}
