using System.Text.Json;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// Redis 实现的 ResumeToken 存储。
/// Key 格式：tcp:resume:{token}（{token} 为 Redis Hash Tag，确保单 Key 操作在 Cluster 下共槽）
/// <para>
/// P0-4：单 Key 状态机替代"分离 claim Key"。
/// 一个 Hash Key 承载完整状态，避免把 Context 移到一个会独立过期的 Claim Key：
/// <list type="bullet">
/// <item><c>state</c>：Available / Claimed / Consumed</item>
/// <item><c>context</c>：JSON 序列化的 <see cref="ResumeContext"/></item>
/// <item><c>claimAttemptId</c>：当前 Claim 的 attemptId（仅 Claimed 时有效）</item>
/// <item><c>claimUntil</c>：Claim 占用截止的绝对毫秒时间戳（仅 Claimed 时有效）</item>
/// <item><c>ttlMs</c>：原始 TTL（发布时设置；Release 时用于恢复）</item>
/// </list>
/// </para>
/// <para>
/// Lua 原子行为：
/// <list type="number">
/// <item><b>Claim</b>（<see cref="TryClaimAsync"/>）：
///   Available → Claimed(attemptId, claimUntil)；Claimed 且 claimUntil 已过期 → 新 attempt 原子接管；
///   其它情况返回 false（占用中）。Context 保留在原 Key，不迁移。</item>
/// <item><b>Commit</b>（<see cref="CommitClaimAsync"/>）：
///   Claimed + 当前 attempt → Consumed（DEL），返回 true。</item>
/// <item><b>Release</b>（<see cref="ReleaseClaimAsync"/>）：
///   Claimed + 当前 attempt → Available（清除 claim 字段，恢复原始 TTL），返回 true。</item>
/// </list>
/// </para>
/// <para>
/// 崩溃恢复语义：Gateway 在 Claim 成功后崩溃（未 Commit/Release），
/// claimUntil 到期后新 attempt 可原子接管同一 Key，原 Token 得以恢复，客户端可真正重试。
/// </para>
/// <para>
/// 可选注入 <see cref="IRedisCircuitBreaker"/>：Redis 故障期间快速失败返回 null，
/// 避免跨 Gateway 重连风暴串行触发 Redis 超时。未注入时跳过熔断器逻辑（兼容旧测试）。
/// </para>
/// </summary>
internal sealed class RedisResumeTokenStore : IResumeTokenStore
{
    // P0-4 缺口1：Hash Tag {token} 确保单 Key 在 Redis Cluster 下可原子操作。
    private const string KeyPrefix = "tcp:resume:{";
    private const string KeySuffix = "}";

    /// <summary>
    /// Claim 占用窗口：超过此时间未 Commit/Release，claimUntil 到期，新 attempt 可接管。
    /// 取 10s：覆盖 Commit 阶段所有外部调用（TakeOver、Issue、Watermark）+ 网络往返。
    /// </summary>
    private static readonly TimeSpan ClaimTtl = TimeSpan.FromSeconds(10);

    // P0-4 缺口2：Lua 原子发布——NotExists 语义 + HSET 状态字段 + PEXPIRE。
    // KEYS[1] = tcp:resume:{token}
    // ARGV[1] = context(json), ARGV[2] = ttlMs
    // 返回 true（成功）/ false（Key 已存在，防碰撞）。
    private const string IssueLuaScript = @"
if redis.call('EXISTS', KEYS[1]) == 1 then return false end
redis.call('HSET', KEYS[1], 'state', 'Available', 'context', ARGV[1], 'ttlMs', ARGV[2])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
return true
";

    // P0-4 缺口3：Lua 原子 Claim——单 Key 状态机，Context 保留在原 Key。
    // KEYS[1] = tcp:resume:{token}
    // ARGV[1] = attemptId, ARGV[2] = claimWindowMs
    // 返回 context bytes（false 表示占用中/Key 不存在/损坏）。
    private const string ClaimLuaScript = @"
local state = redis.call('HGET', KEYS[1], 'state')
if state == false then return false end
if state == 'Claimed' then
  local claimUntil = redis.call('HGET', KEYS[1], 'claimUntil')
  local nowMs = tonumber(redis.call('TIME')[1]) * 1000
  if claimUntil and tonumber(claimUntil) > nowMs then
    return false
  end
end
local ctx = redis.call('HGET', KEYS[1], 'context')
if not ctx then return false end
local nowMs = tonumber(redis.call('TIME')[1]) * 1000
local claimUntil = nowMs + tonumber(ARGV[2])
redis.call('HSET', KEYS[1], 'state', 'Claimed', 'claimAttemptId', ARGV[1], 'claimUntil', claimUntil)
if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[2]) then
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return ctx
";

    // P0-4 缺口4：Lua 原子 Commit——验证 state + attemptId 后 DEL（Consumed）。
    // KEYS[1] = tcp:resume:{token}
    // ARGV[1] = attemptId
    // 返回 true/false。
    private const string CommitLuaScript = @"
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'Claimed' then return false end
local stored = redis.call('HGET', KEYS[1], 'claimAttemptId')
if not stored or stored ~= ARGV[1] then return false end
redis.call('DEL', KEYS[1])
return true
";

    // P0-4 缺口5：Lua 原子 Release——验证 state + attemptId 后还原 Available，恢复原始 TTL。
    // KEYS[1] = tcp:resume:{token}
    // ARGV[1] = attemptId
    // 返回 true/false。
    private const string ReleaseLuaScript = @"
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'Claimed' then return false end
local stored = redis.call('HGET', KEYS[1], 'claimAttemptId')
if not stored or stored ~= ARGV[1] then return false end
redis.call('HSET', KEYS[1], 'state', 'Available')
redis.call('HDEL', KEYS[1], 'claimAttemptId', 'claimUntil')
local ttlMs = redis.call('HGET', KEYS[1], 'ttlMs')
if ttlMs then
  redis.call('PEXPIRE', KEYS[1], tonumber(ttlMs))
end
return true
";

    // P0-4：旧 GETDEL 兼容——Available 时消费返回 context，否则返回 false。
    private const string TryValidateLegacyLuaScript = @"
local state = redis.call('HGET', KEYS[1], 'state')
if state ~= 'Available' then return false end
local ctx = redis.call('HGET', KEYS[1], 'context')
redis.call('DEL', KEYS[1])
return ctx
";

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
        var key = new RedisKey(KeyPrefix + token + KeySuffix);
        var ttlMs = (long)ttl.TotalMilliseconds;
        var value = JsonSerializer.SerializeToUtf8Bytes(
            context,
            GatewayJsonSerializerContext.Default.ResumeContext);

        try
        {
            var result = (bool?)await _connectionProvider.Database
                .ScriptEvaluateAsync(
                    IssueLuaScript,
                    new RedisKey[] { key },
                    new RedisValue[] { value, ttlMs },
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();

            // NotExists 语义：GUID 碰撞近乎不可能，若仍发生则视为一致性问题。
            if (result is not true)
                throw new RedisException("Resume token key already exists (unexpected collision)");

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

        var key = new RedisKey(KeyPrefix + resumeToken.Trim() + KeySuffix);

        // P0-4：旧 GETDEL 语义改为单 Key 状态机的"Available 时消费"。
        // DemandMaster：消费必须落在主节点——若读副本返回 null 但未删除主节点 key，
        // 并发重放会拿到同一 Token，破坏一次性消费语义。
        byte[]? value;
        try
        {
            var result = await _connectionProvider.Database
                .ScriptEvaluateAsync(
                    TryValidateLegacyLuaScript,
                    new RedisKey[] { key },
                    null,
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
            value = result.IsNull ? null : (byte[]?)result;
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            // 抛异常让 Coordinator 归因为 RedisFailure，而非误记为 InvalidToken。
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
    /// P0-4：原子占用 ResumeToken（单 Key 状态机）。
    /// <para>
    /// Lua 脚本原子执行：Available → Claimed(attemptId, claimUntil)；
    /// Claimed 且 claimUntil 已过期 → 新 attempt 原子接管。
    /// Context 保留在原 Key，不迁移到独立 Claim Key。
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
        var attemptId = Guid.NewGuid().ToString("N");

        byte[]? contextBytes;
        try
        {
            var result = await _connectionProvider.Database
                .ScriptEvaluateAsync(
                    ClaimLuaScript,
                    new RedisKey[] { key },
                    new RedisValue[]
                    {
                        attemptId,
                        (long)ClaimTtl.TotalMilliseconds
                    },
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
            contextBytes = result.IsNull ? null : (byte[]?)result;
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
    /// P0-4：Commit 成功后最终消费已占用的 Token（Consumed / DEL）。
    /// 验证 state + attemptId 后 DEL。
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
            // 熔断器开路：claim 依赖 claimUntil 到期后被接管，无法主动消费。
            return false;
        }

        var key = new RedisKey(KeyPrefix + resumeToken.Trim() + KeySuffix);
        try
        {
            var result = (bool?)await _connectionProvider.Database
                .ScriptEvaluateAsync(
                    CommitLuaScript,
                    new RedisKey[] { key },
                    new RedisValue[] { attemptId },
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
            return result ?? false;
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            // 不抛——Commit 失败时 claim 依赖 claimUntil 到期后被接管，不会泄漏 Token 复活。
            return false;
        }
    }

    /// <summary>
    /// P0-4：Abort 时归还已占用的 Token（Available），允许客户端重试。
    /// 验证 state + attemptId 后还原 Available，并恢复原始 TTL。
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
            // 熔断器开路：无法归还，claim 依赖 claimUntil 到期后被接管。
            return;
        }

        var key = new RedisKey(KeyPrefix + resumeToken.Trim() + KeySuffix);
        try
        {
            await _connectionProvider.Database
                .ScriptEvaluateAsync(
                    ReleaseLuaScript,
                    new RedisKey[] { key },
                    new RedisValue[] { attemptId },
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
        }
        catch (RedisException)
        {
            _circuitBreaker?.RecordFailure();
            // 不抛——Release 失败时 claim 依赖 claimUntil 到期后被接管。
        }
    }
}