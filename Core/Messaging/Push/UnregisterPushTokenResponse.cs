namespace ChatApp.TcpGateway.Core.Messaging.Push;

public sealed class UnregisterPushTokenResponse
{
    public required string RequestId { get; init; }

    public bool Succeeded { get; init; }

    public string? ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    /// <summary>注销后剩余的活跃推送令牌数。</summary>
    public int ActiveTokenCount { get; init; }
}
