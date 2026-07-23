using System.Text.Json;
using ChatApp.TcpGateway.Infrastructure.Authentication.Models;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

internal sealed partial class RedisAccessTokenStore(
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
            LogLookupFailed(exception);
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
            LogInvalidRecord(exception);
            return null;
        }
    }

    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Error,
        Message = "Access-token lookup failed.")]
    private partial void LogLookupFailed(Exception exception);

    [LoggerMessage(
        EventId = 11,
        Level = LogLevel.Warning,
        Message = "Invalid access-token record encountered.")]
    private partial void LogInvalidRecord(Exception exception);
}
