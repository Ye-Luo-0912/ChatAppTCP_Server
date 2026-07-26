using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// Redis 全局在线：ZSET 多实例模型。
/// <para>
/// key = presence:{userId}:instances（ZSET），member = GatewayInstanceId，
/// score = ExpiresAtUnixMs。支持用户同时登录多个 Gateway 实例，互不覆盖。
/// 上线/下线/刷新均用 Lua 原子完成，返回全局状态转换（0&lt;-&gt;1）。
/// </para>
/// </summary>
internal sealed class RedisGlobalPresenceStore(
    RedisConnectionProvider connectionProvider,
    TimeProvider timeProvider,
    GatewayMetrics metrics,
    ILogger<RedisGlobalPresenceStore> logger) : IGlobalPresenceStore
{
    public static readonly TimeSpan OnlineTtl = TimeSpan.FromMinutes(5);

    /// <summary>
    /// 上线/刷新 Lua 脚本。
    /// <para>
    /// 原子完成：清理过期成员 -&gt; 查询清理后成员数(before) -&gt; ZADD 当前实例 -&gt; 查询成员数(after) -&gt; 设置 key TTL。
    /// 返回 {before, after}，调用方据此判断 0-&gt;1 转换。
    /// </para>
    /// </summary>
    private const string SetOnlineScript = @"
local key = KEYS[1]
local instanceId = ARGV[1]
local expiresAt = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local before = redis.call('ZCARD', key)
redis.call('ZADD', key, expiresAt, instanceId)
local after = redis.call('ZCARD', key)
local keyTtl = math.ceil((expiresAt - now) / 1000) + 60
if keyTtl > 0 then
    redis.call('EXPIRE', key, keyTtl)
end
return {before, after}
";

    /// <summary>
    /// 下线 Lua 脚本。
    /// <para>
    /// 原子完成：清理过期成员 -&gt; 查询清理后成员数(before) -&gt; ZREM 当前实例 -&gt; 查询成员数(after) -&gt; 空则 DEL。
    /// 返回 {before, after}，调用方据此判断 1-&gt;0 转换。
    /// </para>
    /// </summary>
    private const string SetOfflineScript = @"
local key = KEYS[1]
local instanceId = ARGV[1]
local now = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local before = redis.call('ZCARD', key)
redis.call('ZREM', key, instanceId)
local after = redis.call('ZCARD', key)
if after == 0 then
    redis.call('DEL', key)
end
return {before, after}
";

    /// <summary>
    /// 刷新 Lua 脚本。
    /// <para>
    /// 原子完成：清理过期成员 -&gt; 检查当前实例是否仍为成员 -&gt; 若是则 ZADD 刷新 score + 设置 TTL。
    /// 不返回转换（刷新不触发事件）。
    /// </para>
    /// </summary>
    private const string RefreshScript = @"
local key = KEYS[1]
local instanceId = ARGV[1]
local expiresAt = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local score = redis.call('ZSCORE', key, instanceId)
if score then
    redis.call('ZADD', key, expiresAt, instanceId)
    local keyTtl = math.ceil((expiresAt - now) / 1000) + 60
    if keyTtl > 0 then
        redis.call('EXPIRE', key, keyTtl)
    end
end
return score and 1 or 0
";

    private static RedisKey Key(long userId) => $"presence:{userId}:instances";

    public async Task<PresenceTransition> SetOnlineAsync(
        long userId,
        string instanceId,
        CancellationToken ct = default)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(instanceId))
            return PresenceTransition.None;

        try
        {
            var now = timeProvider.GetUtcNow();
            var expiresAt = now + OnlineTtl;
            var nowMs = now.ToUnixTimeMilliseconds();
            var expiresAtMs = expiresAt.ToUnixTimeMilliseconds();

            var result = await connectionProvider.Database
                .ScriptEvaluateAsync(
                    SetOnlineScript,
                    new RedisKey[] { Key(userId) },
                    new RedisValue[]
                    {
                        instanceId,
                        expiresAtMs,
                        nowMs
                    })
                .WaitAsync(ct)
                .ConfigureAwait(false);

            var arr = (RedisResult[])result!;
            var before = (long)arr[0];
            var after = (long)arr[1];

            // 0 -> 1：清理后无任何成员（before==0），ZADD 后有成员（after>0）。
            return before == 0 && after > 0
                ? PresenceTransition.WentOnline
                : PresenceTransition.None;
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PresenceSetOnline,
                ex);
            return PresenceTransition.None;
        }
    }

    public async Task<PresenceTransition> SetOfflineAsync(
        long userId,
        string instanceId,
        CancellationToken ct = default)
    {
        if (userId <= 0)
            return PresenceTransition.None;

        try
        {
            var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

            var result = await connectionProvider.Database
                .ScriptEvaluateAsync(
                    SetOfflineScript,
                    new RedisKey[] { Key(userId) },
                    new RedisValue[]
                    {
                        instanceId ?? string.Empty,
                        nowMs
                    })
                .WaitAsync(ct)
                .ConfigureAwait(false);

            var arr = (RedisResult[])result!;
            var before = (long)arr[0];
            var after = (long)arr[1];

            // 1 -> 0：清理后有成员（before>0），ZREM 后无成员（after==0）。
            return before > 0 && after == 0
                ? PresenceTransition.WentOffline
                : PresenceTransition.None;
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PresenceSetOffline,
                ex);
            return PresenceTransition.None;
        }
    }

    public async Task RefreshOnlineAsync(
        long userId,
        string instanceId,
        CancellationToken ct = default)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(instanceId))
            return;

        try
        {
            var now = timeProvider.GetUtcNow();
            var expiresAt = now + OnlineTtl;
            var nowMs = now.ToUnixTimeMilliseconds();
            var expiresAtMs = expiresAt.ToUnixTimeMilliseconds();

            await connectionProvider.Database
                .ScriptEvaluateAsync(
                    RefreshScript,
                    new RedisKey[] { Key(userId) },
                    new RedisValue[]
                    {
                        instanceId,
                        expiresAtMs,
                        nowMs
                    })
                .WaitAsync(ct)
                .ConfigureAwait(false);
        }
        catch (Exception)
        {
            // 高频瞬态故障：仅计数，不写日志。
            // Redis 连接级别的故障与恢复由 RedisConnectionProvider 统一报告。
            metrics.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PresenceRefresh);
        }
    }

    public async Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default)
    {
        if (userId <= 0)
            return false;

        try
        {
            var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            // 清理过期成员后查询成员数 > 0。
            var db = connectionProvider.Database;
            var key = Key(userId);
            await db.SortedSetRemoveRangeByScoreAsync(
                key, double.NegativeInfinity, nowMs)
                .WaitAsync(ct).ConfigureAwait(false);
            var count = await db.SortedSetLengthAsync(key)
                .WaitAsync(ct).ConfigureAwait(false);
            return count > 0;
        }
        catch (Exception)
        {
            // 高频瞬态故障：仅计数，不写日志。
            metrics.PresenceQueryFailed();
            return false;
        }
    }

    public async Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken ct = default)
    {
        var result = new Dictionary<long, bool>(userIds.Count);
        if (userIds.Count == 0)
            return result;

        try
        {
            var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            var db = connectionProvider.Database;
            var batch = db.CreateBatch();

            var tasks = new Task<long>[userIds.Count];
            for (var i = 0; i < userIds.Count; i++)
            {
                var key = Key(userIds[i]);
                _ = batch.SortedSetRemoveRangeByScoreAsync(
                    key, double.NegativeInfinity, nowMs);
                tasks[i] = batch.SortedSetLengthAsync(key);
            }

            batch.Execute();
            var counts = await Task.WhenAll(tasks).WaitAsync(ct).ConfigureAwait(false);
            for (var i = 0; i < userIds.Count; i++)
                result[userIds[i]] = counts[i] > 0;
        }
        catch (Exception)
        {
            // 高频瞬态故障：仅计数，不写日志。
            metrics.PresenceQueryFailed();
            foreach (var id in userIds)
                result[id] = false;
        }

        return result;
    }
}
