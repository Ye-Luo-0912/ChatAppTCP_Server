using System.Globalization;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.GroupIdempotency;

/// <summary>
/// Redis L2 实现的群组命令幂等存储。
/// <para>
/// Key 格式：<c>group:idem:{userId}:{operation}:{requestId}</c>（均为字符串）。
/// Value 为 Redis HASH，包含 <c>payloadHash</c>（五.1：SHA-256 hex 字符串）、
/// <c>result</c>（JSON 字符串）与 <c>status</c>（<c>completed</c>）三个字段。
/// TTL 默认 30 秒（与 L1 一致），通过 Lua 内 <c>PEXPIRE</c> 原子设置。
/// </para>
/// <para>
/// 失败开放（fail-open）：Redis 异常或熔断器开路时，<see cref="TryGetAsync"/> 返回 Miss，
/// <see cref="TryAddAsync"/> 静默跳过。不抛异常——幂等缓存为前置快速路径，缺失时回退到 Realtime 调用。
/// </para>
/// <para>
/// 可选注入 <see cref="IRedisCircuitBreaker"/>：Redis 故障期间快速失败返回 Miss，
/// 避免跨 Gateway 重试风暴串行触发 Redis 超时。未注入时跳过熔断器逻辑。
/// </para>
/// <para>
/// 五.2：TryAdd 改为条件写——仅当 key 不存在或已存指纹相同时写入，不覆盖不同指纹的既有结果。
/// 旧实现无条件 HSET 导致两个 Gateway 并发 Miss 后最后写入者覆盖前者。真正的幂等主防线
/// 是 RealtimeServices 数据库中的不可变幂等 Ledger，L2 仅作跨 Gateway 短缓存。
/// </para>
/// </summary>
internal sealed class RedisGroupIdempotencyStore(
    RedisConnectionProvider connectionProvider,
    GatewayMetrics metrics,
    ILogger<RedisGroupIdempotencyStore> logger,
    IRedisCircuitBreaker? circuitBreaker = null) : IGroupIdempotencyStore
{
    private const string KeyPrefix = "group:idem:";
    private const long DefaultTtlMs = 30L * 1000; // 30 秒，与 L1 默认 TTL 一致
    private const string ConflictMarker = "__CONFLICT__";

    /// <summary>
    /// TryGet Lua 脚本：HMGET key payloadHash result，校验 payloadHash 匹配。
    /// <para>
    /// ARGV 顺序：expectedPayloadHash（SHA-256 hex 字符串）。
    /// 返回：
    /// <list type="bullet">
    /// <item>Redis nil（false）→ Miss（key 不存在或字段缺失）</item>
    /// <item>字符串 <c>__CONFLICT__</c> → Conflict（同一 RequestId 但负载指纹不匹配）</item>
    /// <item>其他字符串 → Hit（result JSON）</item>
    /// </list>
    /// </para>
    /// </summary>
    private const string TryGetScript = @"
local key = KEYS[1]
local expectedHash = ARGV[1]
local values = redis.call('HMGET', key, 'payloadHash', 'result')
local storedHash = values[1]
local resultJson = values[2]

if storedHash == false then
    return false
end

if storedHash ~= expectedHash then
    return '__CONFLICT__'
end

return resultJson
";

    /// <summary>
    /// 五.2：TryAdd 条件写 Lua 脚本。
    /// <para>
    /// ARGV 顺序：payloadHash, resultJson, ttlMs。
    /// 语义：
    /// <list type="bullet">
    /// <item>key 不存在 → 写入 payloadHash/result/status=completed + PEXPIRE（首次缓存）。</item>
    /// <item>已存指纹相同 → 仅刷新 TTL，不覆盖 result（同请求重试，幂等）。</item>
    /// <item>已存指纹不同 → 不写入，返回 false（避免并发 Miss 后最后写入者覆盖前者）。</item>
    /// </list>
    /// </para>
    /// </summary>
    private const string TryAddScript = @"
local key = KEYS[1]
local payloadHash = ARGV[1]
local resultJson = ARGV[2]
local ttl = tonumber(ARGV[3])

local storedHash = redis.call('HGET', key, 'payloadHash')
if storedHash == false then
    redis.call('HSET', key, 'payloadHash', payloadHash, 'result', resultJson, 'status', 'completed')
    redis.call('PEXPIRE', key, ttl)
    return true
end

if storedHash == payloadHash then
    redis.call('PEXPIRE', key, ttl)
    return true
end

return false
";

    public async ValueTask<GroupIdempotencyLookup> TryGetAsync(
        long userId,
        int operation,
        string requestId,
        string payloadHash,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(requestId))
            return GroupIdempotencyLookup.Miss;

        if (circuitBreaker is { IsAvailable: false })
        {
            metrics.GroupIdempotentRedisFailure();
            return GroupIdempotencyLookup.Miss;
        }

        var key = CreateKey(userId, operation, requestId);

        try
        {
            var result = await connectionProvider.Database
                .ScriptEvaluateAsync(
                    TryGetScript,
                    new RedisKey[] { key },
                    new RedisValue[] { payloadHash })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            circuitBreaker?.RecordSuccess();

            if (result.IsNull)
                return GroupIdempotencyLookup.Miss;

            var str = (string?)result;
            if (str == ConflictMarker)
                return GroupIdempotencyLookup.Conflict;

            if (string.IsNullOrEmpty(str))
                return GroupIdempotencyLookup.Miss;

            var cached = JsonSerializer.Deserialize(
                str,
                GatewayJsonSerializerContext.Default.RealtimeGroupConversationResult);
            return cached is null
                ? GroupIdempotencyLookup.Miss
                : GroupIdempotencyLookup.Hit(cached);
        }
        catch (OperationCanceledException)
        {
            // 取消视为 fail-open Miss，不记录 Redis 故障。
            return GroupIdempotencyLookup.Miss;
        }
        catch (RedisException exception)
        {
            circuitBreaker?.RecordFailure();
            metrics.GroupIdempotentRedisFailure();
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.GroupIdempotencyLookup,
                exception);
            return GroupIdempotencyLookup.Miss;
        }
        catch (JsonException exception)
        {
            // 损坏的 JSON 数据：视为 Miss，不记录 Redis 故障（非 Redis 层面问题）。
            logger.DependencyDataInvalid(
                GatewayDependency.Redis,
                GatewayDependencyOperation.GroupIdempotencyLookup,
                exception);
            return GroupIdempotencyLookup.Miss;
        }
    }

    public async ValueTask TryAddAsync(
        long userId,
        int operation,
        string requestId,
        string payloadHash,
        GroupConversationResult result,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(requestId) || result is null)
            return;

        if (circuitBreaker is { IsAvailable: false })
        {
            metrics.GroupIdempotentRedisFailure();
            return;
        }

        var key = CreateKey(userId, operation, requestId);
        var resultJson = JsonSerializer.Serialize(
            result,
            GatewayJsonSerializerContext.Default.RealtimeGroupConversationResult);

        try
        {
            await connectionProvider.Database
                .ScriptEvaluateAsync(
                    TryAddScript,
                    new RedisKey[] { key },
                    new RedisValue[]
                    {
                        payloadHash,
                        resultJson,
                        DefaultTtlMs
                    })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            circuitBreaker?.RecordSuccess();
        }
        catch (OperationCanceledException)
        {
            // 取消时静默跳过。
        }
        catch (RedisException exception)
        {
            circuitBreaker?.RecordFailure();
            metrics.GroupIdempotentRedisFailure();
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.GroupIdempotencyStore,
                exception);
        }
    }

    /// <summary>
    /// 移除指定用户的全部缓存条目。使用 SCAN + 批量 DEL，尽力而为。
    /// 失败时静默跳过，依赖 30 秒 TTL 兜底。
    /// </summary>
    public void EvictUser(long userId)
    {
        if (circuitBreaker is { IsAvailable: false })
            return;

        var pattern = KeyPrefix + userId.ToString(CultureInfo.InvariantCulture) + ":*";

        try
        {
            var db = connectionProvider.Database;
            var keysToDelete = new List<RedisKey>();
            long cursor = 0;
            do
            {
                var scanResult = db.Execute(
                    "SCAN",
                    cursor.ToString(CultureInfo.InvariantCulture),
                    "MATCH",
                    pattern,
                    "COUNT",
                    100);

                var arr = (RedisResult[])scanResult!;
                cursor = long.Parse((string)arr[0]!, CultureInfo.InvariantCulture);
                var keys = (RedisResult[])arr[1]!;
                foreach (var k in keys)
                    keysToDelete.Add((RedisKey)k);
            } while (cursor != 0);

            if (keysToDelete.Count > 0)
            {
                db.KeyDelete(keysToDelete.ToArray(), CommandFlags.FireAndForget);
            }

            circuitBreaker?.RecordSuccess();
        }
        catch (RedisException exception)
        {
            circuitBreaker?.RecordFailure();
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.GroupIdempotencyStore,
                exception);
        }
    }

    private static RedisKey CreateKey(long userId, int operation, string requestId)
    {
        // 不使用 hash tag：幂等 key 按 (userId, operation, requestId) 分散，
        // 无需跨字段原子操作，Cluster 环境下自然分散到不同 slot。
        return new RedisKey(string.Concat(
            KeyPrefix,
            userId.ToString("D", CultureInfo.InvariantCulture),
            ":",
            operation.ToString(CultureInfo.InvariantCulture),
            ":",
            requestId));
    }
}