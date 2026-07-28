using System.Text.Json;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// Redis 实现的 ResumeToken 存储。
/// Key 格式：tcp:resume:{token}
/// Value 格式：JSON 序列化的 <see cref="ResumeContext"/>
/// TTL 由 Redis PEXPIRE 自动管理。
/// <para>
/// 可选注入 <see cref="IRedisCircuitBreaker"/>：Redis 故障期间快速失败返回 null，
/// 避免跨 Gateway 重连风暴串行触发 Redis 超时。未注入时跳过熔断器逻辑（兼容旧测试）。
/// </para>
/// </summary>
internal sealed class RedisResumeTokenStore : IResumeTokenStore
{
    private const string KeyPrefix = "tcp:resume:";
    private readonly RedisConnectionProvider _connectionProvider;
    private readonly IRedisCircuitBreaker? _circuitBreaker;

    public RedisResumeTokenStore(
        RedisConnectionProvider connectionProvider,
        IRedisCircuitBreaker? circuitBreaker = null)
    {
        _connectionProvider = connectionProvider;
        _circuitBreaker = circuitBreaker;
    }

    public async Task<string> IssueAsync(
        ResumeContext context,
        TimeSpan ttl,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, TimeSpan.Zero);

        // Issue 熔断器开路时抛异常，调用方（OnAuthenticatedAsync）已有 try/catch 兜底，
        // 返回 null token，客户端下次重连走完整认证。
        if (_circuitBreaker is { IsAvailable: false })
            throw new RedisException("Redis circuit breaker is open");

        var token = Guid.NewGuid().ToString("N");
        var key = new RedisKey(KeyPrefix + token);
        var value = JsonSerializer.SerializeToUtf8Bytes(
            context,
            GatewayJsonSerializerContext.Default.ResumeContext);

        try
        {
            await _connectionProvider.Database.StringSetAsync(
                key,
                value,
                ttl,
                When.NotExists,
                CommandFlags.DemandMaster).ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
            return token;
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            throw;
        }
    }

    public async Task<ResumeContext?> TryValidateAsync(
        string resumeToken,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(resumeToken))
            return null;

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：拒绝 Resume，客户端走完整认证。
            return null;
        }

        var key = new RedisKey(KeyPrefix + resumeToken.Trim());

        // GETDEL：消费式读取，Token 一次性使用。
        // 即使客户端误重放，第二次返回 null。
        // DemandMaster：GETDEL 必须落在主节点——若读副本返回 null 但未删除主节点 key，
        // 并发重放会拿到同一 Token，破坏一次性消费语义。
        try
        {
            var value = (byte[]?)await _connectionProvider.Database
                .StringGetDeleteAsync(key, CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();

            if (value is null || value.Length == 0)
                return null;

            try
            {
                return JsonSerializer.Deserialize(
                    value,
                    GatewayJsonSerializerContext.Default.ResumeContext);
            }
            catch (JsonException)
            {
                return null;
            }
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            // 不抛——Resume 路径已有 try/catch 兜底；这里返回 null 让客户端走完整认证。
            return null;
        }
    }

    public async Task RevokeAsync(string resumeToken, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(resumeToken))
            return;

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：撤销跳过，依赖 Token TTL 自然失效（默认 30s）。
            return;
        }

        var key = new RedisKey(KeyPrefix + resumeToken.Trim());
        // DemandMaster：与 IssueAsync 一致，确保主从切换期间撤销操作写入主节点，
        // 避免撤销失效导致旧 Token 在 TTL 窗口内被用于跨 Gateway 恢复。
        try
        {
            await _connectionProvider.Database
                .KeyDeleteAsync(key, CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            // 不抛——撤销失败仅记日志，依赖 TTL 兜底。
        }
    }
}
