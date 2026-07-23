namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class AuthenticationRequest
{
    public string AccessToken { get; set; } = string.Empty;
    public ulong? DeviceIdHash { get; set; }
}
