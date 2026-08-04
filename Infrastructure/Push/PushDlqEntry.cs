namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// DLQ 中单条失败 Token 记录。
/// </summary>
public sealed record PushDlqFailedToken
{
    /// <summary>令牌指纹（SHA256 前 8 字节 hex，不泄露完整令牌）。</summary>
    public required string TokenFingerprint { get; init; }

    /// <summary>平台（1=Fcm, 2=Apns, 3=WebPush）。</summary>
    public required byte Platform { get; init; }

    /// <summary>失败原因代码（invalid_token / payload_too_large / provider_unavailable / rate_limited / unknown）。</summary>
    public required string ErrorCode { get; init; }

    /// <summary>重试建议间隔（仅 rate_limited / provider_unavailable 有值）。</summary>
    public TimeSpan? RetryAfter { get; init; }
}

/// <summary>
/// 推送投递死信队列（DLQ）条目。
/// <para>
/// 记录已永久失败（重投无意义）或异常终止的推送投递，供人工排查与重放。
/// </para>
/// </summary>
public sealed record PushDlqEntry
{
    /// <summary>投递标识（目标用户 + 消息 Id 派生）。</summary>
    public required string DeliveryId { get; init; }

    /// <summary>目标用户 Id。</summary>
    public required long TargetUserId { get; init; }

    /// <summary>失败 Token 子集（含 Provider / ErrorCode / RetryAfter）。</summary>
    public required IReadOnlyList<PushDlqFailedToken> FailedTokens { get; init; }

    /// <summary>JetStream 投递次数（redelivery 计数）。</summary>
    public ulong? DeliveryCount { get; init; }

    /// <summary>原始 Trace Context 的 traceparent。</summary>
    public string? TraceParent { get; init; }

    /// <summary>原始 Trace Context 的 tracestate。</summary>
    public string? TraceState { get; init; }

    /// <summary>最后异常类别（如 HttpRequestException / TaskCanceledException），异常路径记录。</summary>
    public string? LastExceptionCategory { get; init; }

    /// <summary>记录时间（Unix ms）。</summary>
    public long RecordedAtMs { get; init; }
}