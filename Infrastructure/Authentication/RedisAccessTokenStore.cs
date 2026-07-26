using System.Text.Json;
using ChatApp.TcpGateway.Infrastructure.Authentication.Models;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

internal sealed class RedisAccessTokenStore(
    RedisConnectionProvider connectionProvider,
    ILogger<RedisAccessTokenStore> logger)
    : IAccessTokenStore
{
    private const string ValueField = "value";
    private const string NullValueMarker = "__NULL__";

    public async ValueTask<AccessTokenRecord?> FindAsync(
        string accessToken,
        CancellationToken cancellationToken)
    {
        var key = AccessTokenCacheKey.Create(accessToken);

        RedisValue value;
        try
        {
            value = await connectionProvider.Database
                .HashGetAsync(key, ValueField)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RedisException exception)
        {
            logger.DependencyUnavailable(
                GatewayDependency.Redis,
                GatewayDependencyOperation.AccessTokenLookup,
                exception);
            throw new AuthenticationStoreUnavailableException(
                "The authentication store is unavailable.",
                exception);
        }

        if (!value.HasValue || value.IsNullOrEmpty || value == NullValueMarker)
        {
            return null;
        }

        try
        {
            var utf8 = (byte[]?)value;
            if (utf8 is null)
            {
                return null;
            }

            var record = JsonSerializer.Deserialize(
                utf8,
                GatewayJsonSerializerContext.Default.AccessTokenRecord);

            if (record is null)
            {
                return null;
            }

            var nowMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            return record.ExpiresAtMs <= nowMilliseconds ? null : record;
        }
        catch (JsonException exception)
        {
            logger.DependencyDataInvalid(
                GatewayDependency.Redis,
                GatewayDependencyOperation.AccessTokenLookup,
                exception);
            return null;
        }
    }
}
