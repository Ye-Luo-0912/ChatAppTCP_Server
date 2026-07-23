namespace ChatApp.TcpGateway.Core.Messaging.Conversations;

public sealed class ConversationSetPrefsRequest
{
    public string? RequestId { get; init; }
    public required string ConversationId { get; init; }

    /// <summary>true 置顶；false 取消置顶；null 不变。</summary>
    public bool? Pinned { get; init; }

    /// <summary>true 免打扰；false 取消；null 不变。</summary>
    public bool? Muted { get; init; }

    /// <summary>
    /// 免打扰截止（Unix ms）。仅在 <see cref="Muted"/>=true 时生效；null 表示永久。
    /// </summary>
    public long? MutedUntilMs { get; init; }
}
