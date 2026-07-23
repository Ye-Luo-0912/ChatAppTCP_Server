using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// Redis/Garnet 设备租约：key = tcp:devlease:{userId}:{deviceIdHash} → sessionId。
/// </summary>
internal sealed partial class RedisDeviceSessionLeaseStore(
    RedisConnectionProvider connectionProvider,
    ILogger<RedisDeviceSessionLeaseStore> logger)
    : IDeviceSessionLeaseStore
{
    private const string KeyPrefix = "tcp:devlease:";

    // GET old; SET new with TTL; return old if different (else empty string).
    private static readonly LuaScript TakeOverScript = LuaScript.Prepare(
        """
        local previous = redis.call('GET', @key)
        redis.call('SET', @key, @sessionId, 'PX', tonumber(@ttlMs))
        if previous and previous ~= false and previous ~= @sessionId then
          return previous
        end
        return ''
        """);

    // DEL only if value matches sessionId.
    private static readonly LuaScript ReleaseIfOwnerScript = LuaScript.Prepare(
        """
        local current = redis.call('GET', @key)
        if current and current == @sessionId then
          return redis.call('DEL', @key)
        end
        return 0
        """);

    public async ValueTask<string?> TakeOverAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        if (ttl <= TimeSpan.Zero)
            ttl = TimeSpan.FromHours(24);

        var key = CreateKey(userId, deviceIdHash);
        var ttlMs = (long)Math.Clamp(ttl.TotalMilliseconds, 1_000, 7 * 24 * 60 * 60 * 1000d);

        try
        {
            var result = await TakeOverScript
                .EvaluateAsync(
                    connectionProvider.Database,
                    new { key = (RedisKey)key, sessionId, ttlMs })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            if (result.IsNull)
                return null;

            var previous = (string?)result;
            return string.IsNullOrWhiteSpace(previous) ? null : previous;
        }
        catch (RedisException exception)
        {
            LogTakeOverFailed(exception, userId, deviceIdHash);
            // 租约不可用时退化为仅本机替换，避免阻断登录。
            return null;
        }
    }

    public async ValueTask ReleaseIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        CancellationToken cancellationToken)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(sessionId))
            return;

        var key = CreateKey(userId, deviceIdHash);
        try
        {
            await ReleaseIfOwnerScript
                .EvaluateAsync(
                    connectionProvider.Database,
                    new { key = (RedisKey)key, sessionId })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RedisException exception)
        {
            LogReleaseFailed(exception, userId, deviceIdHash);
        }
    }

    private static string CreateKey(long userId, ulong deviceIdHash) =>
        string.Concat(
            KeyPrefix,
            userId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ":",
            deviceIdHash.ToString(System.Globalization.CultureInfo.InvariantCulture));

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Device session lease take-over failed for user {UserId} device {DeviceIdHash}.")]
    private partial void LogTakeOverFailed(Exception exception, long userId, ulong deviceIdHash);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Warning,
        Message = "Device session lease release failed for user {UserId} device {DeviceIdHash}.")]
    private partial void LogReleaseFailed(Exception exception, long userId, ulong deviceIdHash);
}
