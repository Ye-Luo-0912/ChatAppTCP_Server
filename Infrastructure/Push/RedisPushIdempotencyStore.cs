using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 主线一5：Redis Push 投递幂等存储。
/// <para>
/// 使用 SET NX EX 原子操作，TTL 默认 5 分钟（覆盖 JetStream 重投窗口）。
/// key 格式：<c>push:idem:{targetUserId}:{messageId}</c>。
/// </para>
/// <para>
/// Redis 异常时 fail-open（返回 true 允许投递），避免 Redis 故障阻断推送。
/// 重复推送比丢失推送更安全（客户端可去重）。
/// </para>
/// </summary>
internal sealed class RedisPushIdempotencyStore : IPushIdempotencyStore
{
    private const string KeyPrefix = "push:idem:";
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(5);

    private readonly RedisConnectionProvider _connectionProvider;
    private readonly TimeSpan _ttl;
    private readonly ILogger<RedisPushIdempotencyStore> _logger;

    public RedisPushIdempotencyStore(
        RedisConnectionProvider connectionProvider,
        ILogger<RedisPushIdempotencyStore> logger,
        TimeSpan? ttl = null)
    {
        _connectionProvider = connectionProvider;
        _ttl = ttl ?? DefaultTtl;
        _logger = logger;
    }

    public async ValueTask<bool> TryMarkProcessedAsync(
        long targetUserId,
        string messageId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(messageId))
            return true; // 无 MessageId 时不做幂等，允许投递。

        var key = $"{KeyPrefix}{targetUserId}:{messageId}";
        try
        {
            var db = _connectionProvider.Database;
            // SET NX EX：仅当 key 不存在时设置，返回 true 表示首次标记。
            var result = (bool)await db.StringSetAsync(
                key,
                "1",
                _ttl,
                When.NotExists,
                CommandFlags.None).WaitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch (RedisException ex)
        {
            // Redis 异常时 fail-open（允许投递），避免 Redis 故障阻断推送。
            // 重复推送比丢失推送更安全（客户端可去重）。
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushIdempotencyCheck,
                ex);
            return true;
        }
        catch (Exception ex)
        {
            // 其他异常（如连接未启动）也 fail-open，但记录为不可达。
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushIdempotencyCheck,
                ex);
            return true;
        }
    }
}
