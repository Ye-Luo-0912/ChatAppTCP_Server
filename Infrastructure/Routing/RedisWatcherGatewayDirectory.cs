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
/// key = <c>pw:{watchedUserId}</c>，field = <c>{instanceId}:{watcherUserId}</c>，value = <c>1</c>。
/// 复合 field 天然幂等：重复 RegisterWatchers 不会产生重复计数，重复 UnregisterWatchers 为无操作。
/// </para>
/// <para>
/// 查询时 HKEYS 返回全部复合 field，应用层解析出唯一 instanceId 集合。
/// 写操作通过 Lua 脚本保证 HSET/HDEL 与 PEXPIRE 的原子性，并在 HASH 清空后自动 DEL。
/// </para>
/// <para>
/// 查询失败时返回空集合（不抛异常），调用方据此回退到广播模式。
/// </para>
/// </summary>
internal sealed class RedisWatcherGatewayDirectory(
    RedisConnectionProvider connectionProvider,
    ILogger<RedisWatcherGatewayDirectory> logger) : IWatcherGatewayDirectory
{
    private const string KeyPrefix = "pw:";
    private const string FieldSeparator = ":";

    // HSET key field 1; PEXPIRE key ttl。返回当前 field 数量。
    private static readonly LuaScript RegisterScript = LuaScript.Prepare(
        """
        redis.call('HSET', @key, @field, '1')
        redis.call('PEXPIRE', @key, tonumber(@ttlMs))
        return redis.call('HLEN', @key)
        """);

    // HDEL key field; 若 HASH 为空则 DEL。返回剩余 field 数量。
    private static readonly LuaScript UnregisterScript = LuaScript.Prepare(
        """
        local removed = redis.call('HDEL', @key, @field)
        if removed > 0 and redis.call('HLEN', @key) == 0 then
          redis.call('DEL', @key)
        elseif removed > 0 then
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

        try
        {
            var batch = connectionProvider.Database.CreateBatch();
            var tasks = new Task<RedisValue[]>[watchedUserIds.Count];

            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var userId = watchedUserIds[i];
                if (userId <= 0)
                {
                    result[userId] = Array.Empty<string>();
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

                result[userId] = ExtractInstances(results[i]);
            }
        }
        catch (Exception ex)
        {
            logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.WatcherDirectoryQuery,
                ex);

            foreach (var userId in watchedUserIds)
                result.TryAdd(userId, Array.Empty<string>());
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
            var db = connectionProvider.Database;
            foreach (var watchedUserId in watchedUserIds)
            {
                if (watchedUserId <= 0)
                    continue;

                await RegisterScript
                    .EvaluateAsync(
                        db,
                        new { key = (RedisKey)Key(watchedUserId), field, ttlMs = DefaultTtlMs })
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
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
            var db = connectionProvider.Database;
            foreach (var watchedUserId in watchedUserIds)
            {
                if (watchedUserId <= 0)
                    continue;

                await UnregisterScript
                    .EvaluateAsync(
                        db,
                        new { key = (RedisKey)Key(watchedUserId), field, ttlMs = DefaultTtlMs })
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
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
        string.Concat(instanceId, FieldSeparator, watcherUserId.ToString(CultureInfo.InvariantCulture));

    // 从 HASH field（格式 "instanceId:watcherUserId"）中提取唯一 instanceId。
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
