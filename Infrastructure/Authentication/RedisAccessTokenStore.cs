using System.Text.Json;
using ChatApp.Auth.Contracts;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

internal sealed class RedisAccessTokenStore : IAccessTokenStore
{
    private const string NullValueMarker = "__NULL__";
    private readonly Func<IDatabase> _databaseAccessor;
    private readonly ILogger<RedisAccessTokenStore> _logger;

    public RedisAccessTokenStore(
        RedisConnectionProvider connectionProvider,
        ILogger<RedisAccessTokenStore> logger)
        : this(() => connectionProvider.Database, logger)
    {
    }

    internal RedisAccessTokenStore(
        Func<IDatabase> databaseAccessor,
        ILogger<RedisAccessTokenStore> logger)
    {
        ArgumentNullException.ThrowIfNull(databaseAccessor);
        ArgumentNullException.ThrowIfNull(logger);
        _databaseAccessor = databaseAccessor;
        _logger = logger;
    }

    public async ValueTask<AccessTokenCacheRecord?> FindAsync(
        string accessToken,
        CancellationToken cancellationToken)
    {
        var key = AccessTokenCacheKey.Create(accessToken);

        RedisValue value;
        try
        {
            value = await _databaseAccessor()
                .StringGetAsync(key)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RedisException exception)
        {
            _logger.DependencyUnavailable(
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
                AuthContractsJsonSerializerContext.Default.AccessTokenCacheRecord);

            if (record is null)
            {
                return null;
            }

            var nowMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            return record.ExpiresAtMs <= nowMilliseconds ? null : record;
        }
        catch (JsonException exception)
        {
            _logger.DependencyDataInvalid(
                GatewayDependency.Redis,
                GatewayDependencyOperation.AccessTokenLookup,
                exception);
            return null;
        }
    }
}
