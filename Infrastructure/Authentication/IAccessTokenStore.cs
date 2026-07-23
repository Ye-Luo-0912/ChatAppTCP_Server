using ChatApp.TcpGateway.Infrastructure.Authentication.Models;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

internal interface IAccessTokenStore
{
    ValueTask<AccessTokenRecord?> FindAsync(
        string accessToken,
        CancellationToken cancellationToken);
}
