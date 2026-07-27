using System.Collections.Concurrent;
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
/// <para>
/// 热路径不做 ZREMRANGEBYSCORE 清理：使用 ZCOUNT key (now +inf) 仅统计未过期成员，
/// 过期成员不影响查询正确性，由 <see cref="RunMaintenanceAsync"/> 低频回收内存。
/// 这样将清理开销从每秒数百次刷新移到每 5 分钟一次的维护扫描。
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
    /// 上线 Lua 脚本（热路径，不清理过期成员）。
    /// <para>
    /// ZCOUNT key (now +inf) 统计未过期成员数(before) -&gt; ZADD 当前实例 -&gt; ZCOUNT key (now +inf) 统计未过期成员数(after) -&gt; 设置 key TTL。
    /// 返回 {before, after}，调用方据此判断 0-&gt;1 转换。
    /// </para>
    /// </summary>
    private const string SetOnlineScript = @"
local key = KEYS[1]
local instanceId = ARGV[1]
local expiresAt = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local before = redis.call('ZCOUNT', key, now, '+inf')
redis.call('ZADD', key, expiresAt, instanceId)
local after = redis.call('ZCOUNT', key, now, '+inf')
local keyTtl = math.ceil((expiresAt - now) / 1000) + 60
if keyTtl > 0 then
    redis.call('EXPIRE', key, keyTtl)
end
return {before, after}
";

    /// <summary>
    /// 下线 Lua 脚本（热路径，不清理过期成员）。
    /// <para>
    /// ZCOUNT key (now +inf) 统计未过期成员数(before) -&gt; ZREM 当前实例 -&gt; ZCOUNT key (now +inf) 统计未过期成员数(after) -&gt; 无未过期成员则 DEL。
    /// 返回 {before, after}，调用方据此判断 1-&gt;0 转换。
    /// </para>
    /// </summary>
    private const string SetOfflineScript = @"
local key = KEYS[1]
local instanceId = ARGV[1]
local now = tonumber(ARGV[2])
local before = redis.call('ZCOUNT', key, now, '+inf')
redis.call('ZREM', key, instanceId)
local after = redis.call('ZCOUNT', key, now, '+inf')
if after == 0 then
    redis.call('DEL', key)
end
return {before, after}
";

    /// <summary>
    /// 刷新 Lua 脚本（热路径，不清理过期成员）。
    /// <para>
    /// ZSCORE 检查当前实例是否仍为成员（含过期 score 的成员）-&gt; 若是则 ZADD 刷新 score + 设置 TTL。
    /// 不返回转换（刷新不触发事件）。
    /// </para>
    /// </summary>
    private const string RefreshScript = @"
local key = KEYS[1]
local instanceId = ARGV[1]
local expiresAt = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
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

    /// <summary>
    /// 曾上线用户集合：维护路径据此避免 SCAN，仅清理本实例管理过的 presence key。
    /// 用户完全下线后（DEL）由维护路径移除条目。
    /// </summary>
    private readonly ConcurrentDictionary<long, byte> _activeUsers = new();

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

            _activeUsers.TryAdd(userId, 0);

            var arr = (RedisResult[])result!;
            var before = (long)arr[0];
            var after = (long)arr[1];

            // 0 -> 1：无未过期成员（before==0），ZADD 后有未过期成员（after>0）。
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

            // 1 -> 0：有未过期成员（before>0），ZREM 后无未过期成员（after==0）。
            // after==0 时脚本已 DEL key，维护路径后续移除 _activeUsers 条目。
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

    /// <summary>
    /// 查询用户是否在线：ZCOUNT key (now +inf) &gt; 0。
    /// <para>
    /// 不清理过期成员（ZREMRANGEBYSCORE），仅统计 score &gt;= now 的未过期成员数。
    /// 单次 Redis 往返，热路径无写操作。
    /// </para>
    /// </summary>
    public async Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default)
    {
        if (userId <= 0)
            return false;

        try
        {
            var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            // ZCOUNT key (now +inf)：统计未过期成员数，不清理、不写。
            var count = await connectionProvider.Database
                .SortedSetLengthByValueAsync(Key(userId), nowMs, double.PositiveInfinity)
                .WaitAsync(ct)
                .ConfigureAwait(false);
            return count > 0;
        }
        catch (Exception)
        {
            // 高频瞬态故障：仅计数，不写日志。
            metrics.PresenceQueryFailed();
            return false;
        }
    }

    /// <summary>
    /// 批量查询用户在线状态：每用户 ZCOUNT key (now +inf)。
    /// <para>
    /// 单次 batch 往返，每用户仅 1 条命令（原为 2 条：ZREMRANGEBYSCORE + ZCARD）。
    /// 不清理过期成员，热路径无写操作。
    /// </para>
    /// </summary>
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
                // ZCOUNT key (now +inf)：仅统计未过期成员，不清理。
                tasks[i] = batch.SortedSetLengthByValueAsync(
                    Key(userIds[i]),
                    nowMs,
                    double.PositiveInfinity);
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

    /// <summary>
    /// 低频维护：批量清理过期 ZSET 成员。
    /// <para>
    /// 遍历本实例管理过的活跃用户（_activeUsers），对每个 presence key 执行
    /// ZREMRANGEBYSCORE key -inf now（清理过期成员）+ ZCARD（检查剩余），
    /// 若 key 完全为空则从 _activeUsers 移除（后续维护跳过）。
    /// </para>
    /// <para>
    /// 由后台服务定期调用（默认 5 分钟），回收崩溃实例残留的 ZSET 成员内存。
    /// 热路径不依赖此方法完成清理：ZCOUNT key (now +inf) 已排除过期成员。
    /// </para>
    /// </summary>
    public async Task RunMaintenanceAsync(CancellationToken ct = default)
    {
        if (_activeUsers.IsEmpty)
            return;

        try
        {
            var nowMs = timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            var db = connectionProvider.Database;
            var batch = db.CreateBatch();

            // 收集快照后构造任务，避免在 await 期间遍历 ConcurrentDictionary。
            var userIds = _activeUsers.Keys.ToArray();
            var cleanupTasks = new Task<long>[userIds.Length];
            var countTasks = new Task<long>[userIds.Length];

            for (var i = 0; i < userIds.Length; i++)
            {
                var key = Key(userIds[i]);
                // ZREMRANGEBYSCORE key -inf now：清理过期成员。
                cleanupTasks[i] = batch.SortedSetRemoveRangeByScoreAsync(
                    key, double.NegativeInfinity, nowMs);
                // ZCARD：检查剩余成员（含未过期）。
                countTasks[i] = batch.SortedSetLengthAsync(key);
            }

            batch.Execute();
            var counts = await Task.WhenAll(countTasks).WaitAsync(ct).ConfigureAwait(false);

            // key 完全为空（无任何成员）时从 _activeUsers 移除，后续维护跳过此用户。
            for (var i = 0; i < userIds.Length; i++)
            {
                if (counts[i] == 0)
                {
                    _activeUsers.TryRemove(userIds[i], out _);
                }
            }
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PresenceRefresh,
                ex);
        }
    }
}
