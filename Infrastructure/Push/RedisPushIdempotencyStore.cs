using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// Redis Push 投递幂等存储（Token 级）。
/// <para>
/// key 格式：<c>push:idem:{deliveryId}:{tokenFingerprint}</c>。
/// <see cref="IsSentAsync"/> 用 EXISTS，<see cref="MarkSentAsync"/> 用 SET EX（幂等）。
/// TTL 默认 5 分钟（覆盖 JetStream 重投窗口）。
/// </para>
/// <para>
/// Redis 故障时 fail-open：IsSentAsync 返回 false（允许发送），避免幂等检查阻断推送。
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

    public async ValueTask<bool> IsSentAsync(
        string deliveryId,
        string tokenFingerprint,
        CancellationToken cancellationToken = default)
    {
        var key = $"{KeyPrefix}{deliveryId}:{tokenFingerprint}";
        try
        {
            var db = _connectionProvider.Database;
            return await db.KeyExistsAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushIdempotencyCheck,
                ex);
            return false; // fail-open：允许发送，避免 Redis 故障阻断推送。
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushIdempotencyCheck,
                ex);
            return false;
        }
    }

    public async ValueTask MarkSentAsync(
        string deliveryId,
        string tokenFingerprint,
        CancellationToken cancellationToken = default)
    {
        var key = $"{KeyPrefix}{deliveryId}:{tokenFingerprint}";
        try
        {
            var db = _connectionProvider.Database;
            await db.StringSetAsync(key, "1", _ttl).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushIdempotencyCheck,
                ex);
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PushIdempotencyCheck,
                ex);
        }
    }
}