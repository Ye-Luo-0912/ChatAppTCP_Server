using System.Globalization;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Routing;

/// <summary>
/// Redis 实现：使用 HASH 存储被观察用户的 watcher 路由。
/// <para>
/// key = <c>pw:{watchedUserId}</c>，field = <c>{instanceId}:{watcherUserId}</c>，value = 引用计数。
/// 新接口移除了 <c>gatewaySessionId</c> 参数，改用引用计数维持多会话隔离：
/// 同一 (watchedUserId, watcherUserId, instanceId) 上的多个并发会话各自 Register +1、Unregister -1，
/// 计数归零时移除 field。
/// </para>
/// <para>
/// 复合 field 保证 watcher 维度幂等：相同 (watchedUserId, watcherUserId, instanceId) 重复 Register
/// 累加计数而非产生重复条目，重复 Unregister 减少计数。
/// </para>
/// <para>
/// 注册时通过 PEXPIRE 续期，Gateway 异常退出后整条 HASH 自然过期。
/// 查询时 HKEYS 返回全部复合 field，应用层解析出唯一 instanceId 集合。
/// 写操作通过 Lua 脚本保证 HINCRBY 与 PEXPIRE 的原子性，并在 HASH 清空后自动 DEL。
/// </para>
/// <para>
/// 新接口语义：查询失败时返回空集合，调用方据此回退到 fallback broadcast subject；不抛异常。
/// </para>
/// </summary>
internal sealed class RedisWatcherGatewayDirectory(
    RedisConnectionProvider connectionProvider,
    ILogger<RedisWatcherGatewayDirectory> logger) : IWatcherGatewayDirectory
{
    private const string KeyPrefix = "pw:";
    private const string FieldSeparator = ":";
    private const string ActiveShardsKey = "watchers:__active_shards__";

    // HINCRBY key field 1; PEXPIRE key ttl。返回当前 field 数量。
    // 引用计数：同 watcher+instance 的多次 Register 累加计数。
    private static readonly LuaScript RegisterScript = LuaScript.Prepare(
        """
        redis.call('HINCRBY', @key, @field, 1)
        redis.call('PEXPIRE', @key, tonumber(@ttlMs))
        return redis.call('HLEN', @key)
        """);

    // HINCRBY key field -1; 若计数 <= 0 则 HDEL；HASH 为空则 DEL，否则 PEXPIRE。
    // 引用计数：归零时移除 field，保证任一会话注销不影响其它并发会话条目。
    private static readonly LuaScript UnregisterScript = LuaScript.Prepare(
        """
        local n = redis.call('HINCRBY', @key, @field, -1)
        if tonumber(n) <= 0 then
          redis.call('HDEL', @key, @field)
        end
        if redis.call('HLEN', @key) == 0 then
          redis.call('DEL', @key)
        else
          redis.call('PEXPIRE', @key, tonumber(@ttlMs))
        end
        return redis.call('HLEN', @key)
        """);

    // watcher 路由条目过期时间：与 PresenceWatcherRegistry.DefaultWatchTtl 对齐（30 分钟）。
    private static readonly long DefaultTtlMs =
        (long)TimeSpan.FromMinutes(30).TotalMilliseconds;

    public async Task<IReadOnlyList<string>> GetWatcherGatewaysAsync(
        long watchedUserId,
        CancellationToken cancellationToken = default)
    {
        if (watchedUserId <= 0)
            return Array.Empty<string>();

        try
        {
            var fields = await connectionProvider.Database
                .HashKeysAsync(Key(watchedUserId))
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return ExtractInstances(fields);
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.WatcherDirectoryQuery,
                ex);
            return Array.Empty<string>();
        }
    }

    public async Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetWatcherGatewaysManyAsync(
        IReadOnlyList<long> watchedUserIds,
        CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<long, IReadOnlyList<string>>(watchedUserIds.Count);
        if (watchedUserIds.Count == 0)
            return result;

        // 默认填充空集合（无人观察），仅当批量查询抛异常时整体保持空集合（回退广播）。
        foreach (var userId in watchedUserIds)
            result[userId] = Array.Empty<string>();

        try
        {
            var batch = connectionProvider.Database.CreateBatch();
            var tasks = new Task<RedisValue[]>[watchedUserIds.Count];

            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var userId = watchedUserIds[i];
                if (userId <= 0)
                {
                    tasks[i] = Task.FromResult(Array.Empty<RedisValue>());
                    continue;
                }

                tasks[i] = batch.HashKeysAsync(Key(userId));
            }

            batch.Execute();
            var results = await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var userId = watchedUserIds[i];
                if (userId <= 0)
                    continue;

                var gateways = ExtractInstances(results[i]);
                if (gateways.Count > 0)
                    result[userId] = gateways;
            }
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.WatcherDirectoryQuery,
                ex);
        }

        return result;
    }

    public async Task RegisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (watcherUserId <= 0 || watchedUserIds.Count == 0)
            return;

        var field = BuildField(instanceId, watcherUserId);
        try
        {
            // 批量执行：N 个 watchedUserId 合并为单次 batch 往返，避免 N 次串行 EVAL。
            var db = connectionProvider.Database;
            var batch = db.CreateBatch();
            var tasks = new List<Task<RedisResult>>(watchedUserIds.Count);

            foreach (var watchedUserId in watchedUserIds)
            {
                if (watchedUserId <= 0)
                    continue;

                tasks.Add(RegisterScript.EvaluateAsync(
                    batch,
                    new { key = (RedisKey)Key(watchedUserId), field, ttlMs = DefaultTtlMs }));
            }

            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
            var expiresAt = DateTimeOffset.UtcNow
                .AddMilliseconds(DefaultTtlMs)
                .ToUnixTimeMilliseconds();
            await db.SortedSetAddAsync(
                    ActiveShardsKey,
                    instanceId,
                    expiresAt)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.WatcherDirectoryQuery,
                ex);
        }
    }

    public async Task UnregisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (watcherUserId <= 0 || watchedUserIds.Count == 0)
            return;

        var field = BuildField(instanceId, watcherUserId);
        try
        {
            // 批量执行：N 个 watchedUserId 合并为单次 batch 往返，避免 N 次串行 EVAL。
            var db = connectionProvider.Database;
            var batch = db.CreateBatch();
            var tasks = new List<Task<RedisResult>>(watchedUserIds.Count);

            foreach (var watchedUserId in watchedUserIds)
            {
                if (watchedUserId <= 0)
                    continue;

                tasks.Add(UnregisterScript.EvaluateAsync(
                    batch,
                    new { key = (RedisKey)Key(watchedUserId), field, ttlMs = DefaultTtlMs }));
            }

            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.WatcherDirectoryQuery,
                ex);
        }
    }

    public async Task<IReadOnlyList<string>> ListActiveShardsAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var db = connectionProvider.Database;
            var members = await db.SortedSetRangeByScoreAsync(
                    ActiveShardsKey,
                    nowMs,
                    double.PositiveInfinity,
                    Exclude.Start)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            if (members.Length == 0)
                return Array.Empty<string>();

            var result = new string[members.Length];
            for (var i = 0; i < members.Length; i++)
                result[i] = members[i].ToString();
            return result;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.WatcherDirectoryQuery,
                ex);
            return Array.Empty<string>();
        }
    }

    // 四-4：Gateway 实例注册自身活跃状态（ZADD ActiveShardsKey，score = 租约到期 Unix 毫秒）。
    public async Task RegisterGatewayInstanceAsync(
        string instanceId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        try
        {
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var expiryMs = nowMs + (long)leaseDuration.TotalMilliseconds;
            await connectionProvider.Database
                .SortedSetAddAsync(ActiveShardsKey, instanceId, expiryMs)
                .WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.WatcherDirectoryQuery,
                ex);
        }
    }

    // 四-4：Gateway 实例注销自身活跃状态（ZREM ActiveShardsKey）。
    public async Task UnregisterGatewayInstanceAsync(
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        try
        {
            await connectionProvider.Database
                .SortedSetRemoveAsync(ActiveShardsKey, instanceId)
                .WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.WatcherDirectoryQuery,
                ex);
        }
    }

    private static RedisKey Key(long watchedUserId) =>
        string.Concat(KeyPrefix, watchedUserId.ToString(CultureInfo.InvariantCulture));

    private static string BuildField(string instanceId, long watcherUserId) =>
        string.Concat(
            instanceId,
            FieldSeparator,
            watcherUserId.ToString(CultureInfo.InvariantCulture));

    // 从 HASH field（格式 "instanceId:watcherUserId"）中提取唯一 instanceId。
    // 仅取第一个分隔符之前的部分；watcherUserId 中若包含 ':' 也不影响 instanceId 提取。
    private static IReadOnlyList<string> ExtractInstances(RedisValue[] fields)
    {
        if (fields.Length == 0)
            return Array.Empty<string>();

        HashSet<string>? instances = null;
        foreach (var value in fields)
        {
            var field = (string?)value;
            if (string.IsNullOrEmpty(field))
                continue;

            var sep = field.IndexOf(FieldSeparator[0]);
            if (sep <= 0)
                continue;

            (instances ??= new HashSet<string>(StringComparer.Ordinal))
                .Add(field[..sep]);
        }

        return instances is null
            ? Array.Empty<string>()
            : new List<string>(instances);
    }
}
