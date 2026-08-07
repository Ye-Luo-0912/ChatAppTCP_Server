using ChatApp.Auth.Contracts;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

internal interface IAccessTokenStore
{
    ValueTask<AccessTokenCacheRecord?> FindAsync(
        string accessToken,
        CancellationToken cancellationToken);
}
