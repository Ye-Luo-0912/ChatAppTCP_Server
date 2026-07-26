namespace ChatApp.TcpGateway.Core.Authentication;

public sealed record RealtimeAuthenticationResult
{
    public required bool Succeeded { get; init; }
    public AuthenticationFailureKind FailureKind { get; init; }
    public long UserId { get; init; }
    public string? SessionId { get; init; }
    public string? UserName { get; init; }
    public ulong? DeviceIdHash { get; init; }

    /// <summary>
    /// 来自 Token 的服务器签发设备标识（权威身份）。
    /// </summary>
    public string? DeviceId { get; init; }

    public IReadOnlyList<string> Roles { get; init; } = [];
    public string? ErrorMessage { get; init; }

    public static RealtimeAuthenticationResult Failure(
        string message,
        AuthenticationFailureKind kind = AuthenticationFailureKind.InvalidCredentials) =>
        new()
        {
            Succeeded = false,
            FailureKind = kind,
            ErrorMessage = message
        };

    public static RealtimeAuthenticationResult Success(
        long userId,
        string? sessionId,
        string? userName,
        ulong? deviceIdHash,
        string? deviceId = null,
        IReadOnlyList<string>? roles = null) =>
        new()
        {
            Succeeded = true,
            FailureKind = AuthenticationFailureKind.None,
            UserId = userId,
            SessionId = sessionId,
            UserName = userName,
            DeviceIdHash = deviceIdHash,
            DeviceId = deviceId,
            Roles = roles ?? []
        };
}
