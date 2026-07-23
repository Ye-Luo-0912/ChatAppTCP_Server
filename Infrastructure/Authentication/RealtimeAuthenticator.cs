using ChatApp.TcpGateway.Core.Authentication;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

internal sealed class RealtimeAuthenticator(IAccessTokenStore tokenStore)
    : IRealtimeAuthenticator
{
    public async ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
        string accessToken,
        ulong? deviceIdHash,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(accessToken))
        {
            return RealtimeAuthenticationResult.Failure("AccessToken 为空");
        }

        try
        {
            var record = await tokenStore
                .FindAsync(accessToken, cancellationToken)
                .ConfigureAwait(false);

            if (record is null)
            {
                return RealtimeAuthenticationResult.Failure("AccessToken 无效或已过期");
            }

            if (record.DeviceIdHash is not null &&
                record.DeviceIdHash != deviceIdHash)
            {
                return RealtimeAuthenticationResult.Failure(
                    "设备不匹配",
                    AuthenticationFailureKind.DeviceMismatch);
            }

            return RealtimeAuthenticationResult.Success(
                record.UserId,
                record.SessionId,
                record.UserName,
                record.DeviceIdHash,
                record.Roles);
        }
        catch (AuthenticationStoreUnavailableException)
        {
            return RealtimeAuthenticationResult.Failure(
                "鉴权服务暂时不可用",
                AuthenticationFailureKind.DependencyUnavailable);
        }
    }
}

