using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// Redis/Garnet 设备租约：key = tcp:devlease:{userId}:{deviceIdHash} → value = connectionLeaseId\nsessionId。
/// <para>
/// 可选注入 <see cref="IRedisCircuitBreaker"/>：Redis 故障期间快速失败，避免心跳刷新与
/// TakeOver 串行触发超时。未注入时跳过熔断器逻辑（兼容旧测试）。
/// </para>
/// </summary>
/// <remarks>
/// 租约值拆分为 connectionLeaseId（所有权令牌）与 sessionId（路由标识）。
/// </remarks>
internal sealed class RedisDeviceSessionLeaseStore : IDeviceSessionLeaseStore
{
    private const string KeyPrefix = "tcp:devlease:";
    private readonly RedisConnectionProvider _connectionProvider;
    private readonly ILogger<RedisDeviceSessionLeaseStore> _logger;
    private readonly IRedisCircuitBreaker? _circuitBreaker;

    public RedisDeviceSessionLeaseStore(
        RedisConnectionProvider connectionProvider,
        ILogger<RedisDeviceSessionLeaseStore> logger,
        IRedisCircuitBreaker? circuitBreaker = null)
    {
        _connectionProvider = connectionProvider;
        _logger = logger;
        _circuitBreaker = circuitBreaker;
    }

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

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：退化为仅本机替换，避免阻断登录。
            return null;
        }

        var key = CreateKey(userId, deviceIdHash);
        var ttlMs = (long)Math.Clamp(ttl.TotalMilliseconds, 1_000, 7 * 24 * 60 * 60 * 1000d);

        try
        {
            var result = await TakeOverScript
                .EvaluateAsync(
                    _connectionProvider.Database,
                    new { key = (RedisKey)key, sessionId, connectionLeaseId, ttlMs })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();

            if (result.IsNull)
                return null;

            var previous = (string?)result;
            return string.IsNullOrWhiteSpace(previous) ? null : previous;
        }
        catch (RedisException exception)
        {
            _circuitBreaker?.RecordFailure();
            _logger.DependencyOperationFailed(
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

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：跳过 Release，依赖租约 TTL 自然失效。
            return;
        }

        var key = CreateKey(userId, deviceIdHash);
        try
        {
            await ReleaseIfOwnerScript
                .EvaluateAsync(
                    _connectionProvider.Database,
                    new { key = (RedisKey)key, connectionLeaseId })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
        }
        catch (RedisException exception)
        {
            _circuitBreaker?.RecordFailure();
            _logger.DependencyOperationFailed(
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

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：刷新失败但不关闭连接（与原 RedisException 路径一致）。
            return false;
        }

        var key = CreateKey(userId, deviceIdHash);
        var ttlMs = (long)Math.Clamp(ttl.TotalMilliseconds, 1_000, 7 * 24 * 60 * 60 * 1000d);

        try
        {
            var result = await RefreshIfOwnerScript
                .EvaluateAsync(
                    _connectionProvider.Database,
                    new { key = (RedisKey)key, connectionLeaseId, ttlMs })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();

            return result.IsNull ? false : (long)result == 1;
        }
        catch (RedisException exception)
        {
            _circuitBreaker?.RecordFailure();
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseRefresh,
                exception);
            return false;
        }
    }

    public async ValueTask<string?> GetCurrentSessionIdAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        if (userId <= 0)
            return null;

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：查询失败返回 null，退化为不拦截（与原 RedisException 路径一致），
            // 由 RevokeAsync 兜底防止旧 Token 复活。
            return null;
        }

        var key = CreateKey(userId, deviceIdHash);
        try
        {
            var current = await _connectionProvider.Database
                .StringGetAsync(key)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();

            if (current.IsNullOrEmpty)
                return null;

            var value = (string)current!;
            var sep = value.IndexOf('\n');
            // 值格式：connectionLeaseId\nsessionId。仅返回 sessionId 部分。
            return sep >= 0 ? value[(sep + 1)..] : value;
        }
        catch (RedisException exception)
        {
            _circuitBreaker?.RecordFailure();
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseQuery,
                exception);
            // 查询失败时返回 null，退化为不拦截（与无租约等同），避免阻断恢复。
            return null;
        }
    }

    private static string CreateKey(long userId, ulong deviceIdHash) =>
        string.Concat(
            KeyPrefix,
            userId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ":",
            deviceIdHash.ToString(System.Globalization.CultureInfo.InvariantCulture));
}
