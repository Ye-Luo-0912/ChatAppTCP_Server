namespace ChatApp.TcpGateway.Core.Messaging.Push;

public sealed class RegisterPushTokenResponse
{
    public required string RequestId { get; init; }

    public bool Succeeded { get; init; }

    public string? ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    /// <summary>当前用户已注册的推送令牌数（含本次）。</summary>
    public int ActiveTokenCount { get; init; }
}
