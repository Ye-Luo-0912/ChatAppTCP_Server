using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// Redis/Garnet 设备租约：key = tcp:devlease:{userId}:{deviceIdHash} → value = connectionLeaseId\nsessionId。
/// </summary>
/// <remarks>
/// 租约值拆分为 connectionLeaseId（所有权令牌）与 sessionId（路由标识）。
/// </remarks>
internal sealed class RedisDeviceSessionLeaseStore(
    RedisConnectionProvider connectionProvider,
    ILogger<RedisDeviceSessionLeaseStore> logger)
    : IDeviceSessionLeaseStore
{
    private const string KeyPrefix = "tcp:devlease:";

    // 值格式：connectionLeaseId\nsessionId
    // GET old; SET new with TTL; return old sessionId if connectionLeaseId differs (else empty string).
    private static readonly LuaScript TakeOverScript = LuaScript.Prepare(
        """
        local previous = redis.call('GET', @key)
        local newvalue = @connectionLeaseId .. '\n' .. @sessionId
        redis.call('SET', @key, newvalue, 'PX', tonumber(@ttlMs))
        if previous and previous ~= false then
          local sep = string.find(previous, '\n')
          if sep then
            local prevLease = string.sub(previous, 1, sep - 1)
            local prevSession = string.sub(previous, sep + 1)
            if prevLease ~= @connectionLeaseId then
              return prevSession
            end
          else
            return previous
          end
        end
        return ''
        """);

    // DEL only if connectionLeaseId matches.
    private static readonly LuaScript ReleaseIfOwnerScript = LuaScript.Prepare(
        """
        local current = redis.call('GET', @key)
        if current then
          local sep = string.find(current, '\n')
          local leaseId
          if sep then
            leaseId = string.sub(current, 1, sep - 1)
          else
            leaseId = current
          end
          if leaseId == @connectionLeaseId then
            return redis.call('DEL', @key)
          end
        end
        return 0
        """);

    // PEXPIRE only if connectionLeaseId matches.
    private static readonly LuaScript RefreshIfOwnerScript = LuaScript.Prepare(
        """
        local current = redis.call('GET', @key)
        if current then
          local sep = string.find(current, '\n')
          local leaseId
          if sep then
            leaseId = string.sub(current, 1, sep - 1)
          else
            leaseId = current
          end
          if leaseId == @connectionLeaseId then
            redis.call('PEXPIRE', @key, tonumber(@ttlMs))
            return 1
          end
        end
        return 0
        """);

    public async ValueTask<string?> TakeOverAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        string connectionLeaseId,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionLeaseId);
        if (ttl <= TimeSpan.Zero)
            ttl = TimeSpan.FromHours(24);

        var key = CreateKey(userId, deviceIdHash);
        var ttlMs = (long)Math.Clamp(ttl.TotalMilliseconds, 1_000, 7 * 24 * 60 * 60 * 1000d);

        try
        {
            var result = await TakeOverScript
                .EvaluateAsync(
                    connectionProvider.Database,
                    new { key = (RedisKey)key, sessionId, connectionLeaseId, ttlMs })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            if (result.IsNull)
                return null;

            var previous = (string?)result;
            return string.IsNullOrWhiteSpace(previous) ? null : previous;
        }
        catch (RedisException exception)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseTakeOver,
                exception);
            // 租约不可用时退化为仅本机替换，避免阻断登录。
            return null;
        }
    }

    public async ValueTask ReleaseIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string connectionLeaseId,
        CancellationToken cancellationToken)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(connectionLeaseId))
            return;

        var key = CreateKey(userId, deviceIdHash);
        try
        {
            await ReleaseIfOwnerScript
                .EvaluateAsync(
                    connectionProvider.Database,
                    new { key = (RedisKey)key, connectionLeaseId })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RedisException exception)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseRelease,
                exception);
        }
    }

    public async ValueTask<bool> RefreshIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string connectionLeaseId,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(connectionLeaseId))
            return false;
        if (ttl <= TimeSpan.Zero)
            return false;

        var key = CreateKey(userId, deviceIdHash);
        var ttlMs = (long)Math.Clamp(ttl.TotalMilliseconds, 1_000, 7 * 24 * 60 * 60 * 1000d);

        try
        {
            var result = await RefreshIfOwnerScript
                .EvaluateAsync(
                    connectionProvider.Database,
                    new { key = (RedisKey)key, connectionLeaseId, ttlMs })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return result.IsNull ? false : (long)result == 1;
        }
        catch (RedisException exception)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseRefresh,
                exception);
            return false;
        }
    }

    private static string CreateKey(long userId, ulong deviceIdHash) =>
        string.Concat(
            KeyPrefix,
            userId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ":",
            deviceIdHash.ToString(System.Globalization.CultureInfo.InvariantCulture));
}
