namespace ChatApp.TcpGateway.Core.Authentication;

public interface IRealtimeAuthenticator
{
    ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
        string accessToken,
        ulong? deviceIdHash,
        CancellationToken cancellationToken = default);
}
