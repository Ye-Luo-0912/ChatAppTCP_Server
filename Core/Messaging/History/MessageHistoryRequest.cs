namespace ChatApp.TcpGateway.Core.Messaging.History;

public sealed class MessageHistoryRequest
{
    public string? RequestId { get; init; }

    /// <summary>
    /// 非空时按会话查询；空则用户级全量历史（兼容旧客户端）。
    /// </summary>
    public string? ConversationId { get; init; }

    public long? BeforeReceivedAtMs { get; init; }
    public string? BeforeMessageId { get; init; }
    public long? AfterReceivedAtMs { get; init; }
    public string? AfterMessageId { get; init; }
    public int Limit { get; init; } = 50;
}