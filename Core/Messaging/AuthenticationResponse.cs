namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class AuthenticationResponse
{
    public bool Success { get; set; }
    public long UserId { get; set; }
    public string? ErrorMessage { get; set; }
    public string? SessionId { get; set; }
    public ulong? DeviceIdHash { get; set; }

    /// <summary>
    /// 服务器签发的设备标识（权威身份），回传客户端确认。
    /// </summary>
    public string? DeviceId { get; set; }

    /// <summary>断线重连令牌。客户端断线后短时间内可凭此 Token 恢复会话。</summary>
    public string? ResumeToken { get; set; }
}
