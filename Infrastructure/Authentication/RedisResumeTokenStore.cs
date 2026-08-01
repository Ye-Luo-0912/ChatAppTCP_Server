using System.Text.Json;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// Redis 实现的 ResumeToken 存储。
/// Key 格式：tcp:resume:{token}（{token} 为 Redis Hash Tag，确保原 Key 与 claim Key 共槽）
/// Value 格式：JSON 序列化的 <see cref="ResumeContext"/>
/// TTL 由 Redis PEXPIRE 自动管理。
/// <para>
/// P0-3：实现 Claim/Commit/Release 原子模式替代破坏性 GETDEL。
/// Claim 使用 Lua 原子脚本：PTTL + GETDEL 原 Key + SET claim Key（保存上下文与 attemptId），单次往返原子。
/// Commit 验证 attemptId 后 DEL claim Key；Release 验证 attemptId 后还原原 Key。
/// Abort 时 Token 可恢复，客户端可重试。
/// </para>
/// <para>
/// 可选注入 <see cref="IRedisCircuitBreaker"/>：Redis 故障期间快速失败返回 null，
/// 避免跨 Gateway 重连风暴串行触发 Redis 超时。未注入时跳过熔断器逻辑（兼容旧测试）。
/// </para>
/// </summary>
internal sealed class RedisResumeTokenStore : IResumeTokenStore
{
    // P0-3 缺口1：Hash Tag {token} 确保 tcp:resume:{token} 与 tcp:resume:claim:{token} 共槽，
    // Redis Cluster 下 Claim/Commit/Release 的多 Key Lua 操作可跨 Key 原子执行。
    private const string KeyPrefix = "tcp:resume:{";
    private const string ClaimKeyPrefix = "tcp:resume:claim:{";
    private const string KeySuffix = "}";

    /// <summary>
    /// Claim 占用窗口：超过此时间未 Commit/Release，claim Key 过期，Token 不可恢复。
    /// 取 10s：覆盖 Commit 阶段所有外部调用（TakeOver、Issue、Watermark）+ 网络往返。
    /// </summary>
    private static readonly TimeSpan ClaimTtl = TimeSpan.FromSeconds(10);

    private readonly RedisConnectionProvider _connectionProvider;
    private readonly IRedisCircuitBreaker? _circuitBreaker;

    // P0-3 缺口2：Lua 脚本原子 Claim——PTTL + GETDEL 原 Key + HSET claim Key（单次 Redis 往返）。
    // KEYS[1] = tcp:resume:{token}, KEYS[2] = tcp:resume:claim:{token}
    // ARGV[1] = attemptId, ARGV[2] = claimTtlMs
    // 返回原 context bytes（false 表示 Token 无效/已过期/无 TTL）。
    private const string ClaimLuaScript = @"
local ttlMs = redis.call('PTTL', KEYS[1])
if ttlMs < 0 then return false end
local ctx = redis.call('GETDEL', KEYS[1])
if not ctx then return false end
redis.call('HSET', KEYS[2], 'attemptId', ARGV[1], 'context', ctx, 'ttlMs', ttlMs)
redis.call('PEXPIRE', KEYS[2], ARGV[2])
return ctx
";

    // P0-3：Lua 脚本原子 Commit——验证 attemptId 后 DEL claim Key。
    // KEYS[2] = tcp:resume:claim:{token}
    // ARGV[1] = attemptId
    // 返回 true/false。
    private const string CommitLuaScript = @"
local stored = redis.call('HGET', KEYS[1], 'attemptId')
if not stored or stored ~= ARGV[1] then return false end
redis.call('DEL', KEYS[1])
return true
";

    // P0-3：Lua 脚本原子 Release——验证 attemptId 后还原原 Key。
    // KEYS[1] = tcp:resume:{token}, KEYS[2] = tcp:resume:claim:{token}
    // ARGV[1] = attemptId
    // 返回 true/false。
    private const string ReleaseLuaScript = @"
local stored = redis.call('HGET', KEYS[2], 'attemptId')
if not stored or stored ~= ARGV[1] then return false end
local ctx = redis.call('HGET', KEYS[2], 'context')
local ttlMs = redis.call('HGET', KEYS[2], 'ttlMs')
if not ctx or not ttlMs then return false end
redis.call('SET', KEYS[1], ctx, 'PX', ttlMs)
redis.call('DEL', KEYS[2])
return true
";

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
        var key = new RedisKey(KeyPrefix + token + KeySuffix);
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

        // 不在此处检查熔断器 IsAvailable：Coordinator.TryResumeAsync 入口已检查过，
        // 此处二次检查会消耗 Half-Open Probe Lease，导致同一 Resume 请求被拦截。
        // 熔断器的 RecordSuccess/RecordFailure 仍由此处记录（基于实际 Redis 操作结果）。
        // IssueAsync/RevokeAsync 保留各自独立的 IsAvailable 检查，因为它们的调用方
        // （OnAuthenticatedAsync）不在 Resume 路径上，不存在二次获取 Probe 的问题。

        var key = new RedisKey(KeyPrefix + resumeToken.Trim() + KeySuffix);

        // GETDEL：消费式读取，Token 一次性使用。
        // 即使客户端误重放，第二次返回 null。
        // DemandMaster：GETDEL 必须落在主节点——若读副本返回 null 但未删除主节点 key，
        // 并发重放会拿到同一 Token，破坏一次性消费语义。
        byte[]? value;
        try
        {
            value = (byte[]?)await _connectionProvider.Database
                .StringGetDeleteAsync(key, CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            // 抛异常让 Coordinator 归因为 RedisFailure，而非误记为 InvalidToken。
            // 旧实现返回 null 导致 Redis 故障被污染到 InvalidToken 指标桶。
            throw;
        }

        if (value is null || value.Length is 0)
            return null;

        // 无效 JSON 等同于 Token 损坏，返回 null（InvalidToken）是正确的——
        // 这不是 Redis 故障，而是 Token 内容无效。
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

    public async Task RevokeAsync(string resumeToken, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(resumeToken))
            return;

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：撤销跳过，依赖 Token TTL 自然失效（默认 30s）。
            return;
        }

        var key = new RedisKey(KeyPrefix + resumeToken.Trim() + KeySuffix);
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

    /// <summary>
    /// P0-3：原子占用 ResumeToken。
    /// <para>
    /// Lua 脚本原子执行：GETDEL 原 Key（防止并发 Claim）+ HSET claim Key（保存上下文与 attemptId）。
    /// Claim Key TTL 为 <see cref="ClaimTtl"/>（10s），超时未 Commit/Release 则 Token 不可恢复。
    /// </para>
    /// </summary>
    public async Task<ResumeClaimResult?> TryClaimAsync(
        string resumeToken,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(resumeToken))
            return null;

        if (_circuitBreaker is { IsAvailable: false })
            throw new RedisException("Redis circuit breaker is open");

        var token = resumeToken.Trim();
        var key = new RedisKey(KeyPrefix + token + KeySuffix);
        var claimKey = new RedisKey(ClaimKeyPrefix + token + KeySuffix);
        var attemptId = Guid.NewGuid().ToString("N");

        // P0-3 缺口2：PTTL 已移入 ClaimLuaScript 内部，避免 PTTL + GETDEL 两次往返的非原子竞态。
        // Lua 脚本原子执行：PTTL（-2/-1 返回 false）→ GETDEL → HSET claim Key → PEXPIRE。
        byte[]? contextBytes;
        try
        {
            var result = (byte[]?)await _connectionProvider.Database
                .ScriptEvaluateAsync(
                    ClaimLuaScript,
                    new RedisKey[] { key, claimKey },
                    new RedisValue[]
                    {
                        attemptId,
                        (long)ClaimTtl.TotalMilliseconds
                    },
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
            contextBytes = result;
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            throw;
        }

        if (contextBytes is null || contextBytes.Length is 0)
            return null;

        ResumeContext? context;
        try
        {
            context = JsonSerializer.Deserialize(
                contextBytes,
                GatewayJsonSerializerContext.Default.ResumeContext);
        }
        catch (JsonException)
        {
            return null;
        }

        if (context is null)
            return null;

        return new ResumeClaimResult
        {
            Context = context,
            AttemptId = attemptId
        };
    }

    /// <summary>
    /// P0-3：Commit 成功后最终消费已占用的 Token。
    /// 验证 attemptId 后 DEL claim Key。
    /// </summary>
    public async Task<bool> CommitClaimAsync(
        string resumeToken,
        string attemptId,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(resumeToken) || string.IsNullOrEmpty(attemptId))
            return false;

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：claim Key 依赖 TTL 过期，无法主动消费。
            return false;
        }

        var claimKey = new RedisKey(ClaimKeyPrefix + resumeToken.Trim() + KeySuffix);
        try
        {
            var result = (bool?)await _connectionProvider.Database
                .ScriptEvaluateAsync(
                    CommitLuaScript,
                    new RedisKey[] { claimKey },
                    new RedisValue[] { attemptId },
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
            return result ?? false;
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            // 不抛——Commit 失败时 claim Key 依赖 TTL 过期，不会泄漏 Token 复活。
            return false;
        }
    }

    /// <summary>
    /// P0-3：Abort 时归还已占用的 Token，允许客户端重试。
    /// 验证 attemptId 后还原原 Key（保留剩余 TTL）。
    /// </summary>
    public async Task ReleaseClaimAsync(
        string resumeToken,
        string attemptId,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(resumeToken) || string.IsNullOrEmpty(attemptId))
            return;

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：无法归还，Token 依赖 claim Key TTL 过期后不可恢复。
            return;
        }

        var key = new RedisKey(KeyPrefix + resumeToken.Trim() + KeySuffix);
        var claimKey = new RedisKey(ClaimKeyPrefix + resumeToken.Trim() + KeySuffix);
        try
        {
            await _connectionProvider.Database
                .ScriptEvaluateAsync(
                    ReleaseLuaScript,
                    new RedisKey[] { key, claimKey },
                    new RedisValue[] { attemptId },
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            // 不抛——Release 失败时 claim Key 依赖 TTL 过期。
        }
    }
}

