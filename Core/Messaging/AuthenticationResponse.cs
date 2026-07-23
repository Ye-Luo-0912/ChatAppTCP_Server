namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class AuthenticationResponse
{
    public bool Success { get; set; }
    public long UserId { get; set; }
    public string? ErrorMessage { get; set; }
    public string? SessionId { get; set; }
    public ulong? DeviceIdHash { get; set; }
}
