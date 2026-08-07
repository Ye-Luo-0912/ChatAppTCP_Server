using System.Globalization;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Routing;

/// <summary>
/// Redis watcher 路由目录。
/// <para>
/// canonical schema 与 RealtimeServices 保持一致：
/// <c>watchers:{watchedUserId}:instances</c> 是 ZSET，member 为
/// <c>{watcherUserId}:{instanceId}</c>、score 为租约到期 Unix 毫秒；
/// <c>watchers:{watchedUserId}:gateways</c> 是去重后的 Gateway instance SET。
/// </para>
/// <para>
/// 为支持从旧 Gateway 的 <c>pw:{watchedUserId}</c> HASH 滚动升级，查询暂时合并旧 HASH，
/// 写入暂时双写旧 HASH。旧结构仅作为迁移兼容层；canonical ZSET + SET 是跨进程真值。
/// </para>
/// <para>
/// 查询失败返回空集合，使调用方回退到 broadcast；主动取消仍向上传播。
/// </para>
/// </summary>
internal sealed class RedisWatcherGatewayDirectory(
    RedisConnectionProvider connectionProvider,
    ILogger<RedisWatcherGatewayDirectory> logger) : IWatcherGatewayDirectory
{
    internal const long WatcherLeaseMs = 300_000;
    internal const string ActiveShardsKey = "watchers:__active_shards__";
    internal const string GatewayInstancesKey = "gateway_instances:__active__";

    private const long LegacyTtlMs = 1_800_000;
    private const string KeyPrefix = "watchers:";
    private const string InstancesKeySuffix = ":instances";
    private const string GatewaysKeySuffix = ":gateways";
    private const string LegacyKeyPrefix = "pw:";
    private const char MemberSeparator = ':';

    // KEYS[1] = watchers:{watchedUserId}:instances
    // KEYS[2] = watchers:{watchedUserId}:gateways
    // ARGV[1] = {watcherUserId}:{instanceId}; ARGV[2] = expiresAtMs; ARGV[3] = instanceId
    private const string AddWatcherScript = """
        redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
        redis.call('SADD', KEYS[2], ARGV[3])
        return 1
        """;

    // 移除关系后，仅当同一 instance 已无其它 watcher 时才从聚合 SET 移除。
    // watcherUserId 固定为十进制数字，因此第一个 ':' 后的全部内容均为 instanceId。
    private const string RemoveWatcherScript = """
        redis.call('ZREM', KEYS[1], ARGV[1])
        local remaining = redis.call('ZRANGE', KEYS[1], 0, -1)
        local hasGateway = false
        for _, m in ipairs(remaining) do
          local sep = string.find(m, ':', 1, true)
          if sep and sep < #m and string.sub(m, sep + 1) == ARGV[2] then
            hasGateway = true
            break
          end
        end
        if not hasGateway then
          redis.call('SREM', KEYS[2], ARGV[2])
        end
        return 1
        """;

    // 查询时原子清理过期关系，并同步修剪聚合 SET 中的陈旧 instance。
    private const string QueryWatcherGatewaysScript = """
        redis.call('ZREMRANGEBYSCORE', KEYS[1], -1, ARGV[1])
        local members = redis.call('ZRANGE', KEYS[1], 0, -1)
        local found = {}
        for _, m in ipairs(members) do
          local sep = string.find(m, ':', 1, true)
          if sep and sep < #m then
            found[string.sub(m, sep + 1)] = true
          end
        end
        local gateways = redis.call('SMEMBERS', KEYS[2])
        local result = {}
        for _, gw in ipairs(gateways) do
          if found[gw] then
            table.insert(result, gw)
          else
            redis.call('SREM', KEYS[2], gw)
          end
        end
        return result
        """;

    // 旧 HASH 只用于迁移兼容。HSET 而非 HINCRBY，保持接口要求的幂等注册语义。
    private const string LegacyWriteScript = """
        redis.call('HSET', KEYS[1], ARGV[1], 1)
        redis.call('PEXPIRE', KEYS[1], ARGV[2])
        return 1
        """;

    public async Task<IReadOnlyList<string>> GetWatcherGatewaysAsync(
        long watchedUserId,
        CancellationToken cancellationToken = default)
    {
        if (watchedUserId <= 0)
            return Array.Empty<string>();

        try
        {
            var db = connectionProvider.Database;
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var canonicalTask = db.ScriptEvaluateAsync(
                QueryWatcherGatewaysScript,
                [InstancesKey(watchedUserId), GatewaysKey(watchedUserId)],
                [nowMs]);
            var legacyTask = db.HashKeysAsync(LegacyKey(watchedUserId));

            await Task.WhenAll(canonicalTask, legacyTask)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return MergeInstances(
                ConvertRedisResultToStrings(await canonicalTask.ConfigureAwait(false)),
                await legacyTask.ConfigureAwait(false));
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

        foreach (var userId in watchedUserIds)
            result[userId] = Array.Empty<string>();

        try
        {
            var db = connectionProvider.Database;
            var batch = db.CreateBatch();
            var canonicalTasks = new Task<RedisValue[]>[watchedUserIds.Count];
            var legacyTasks = new Task<RedisValue[]>[watchedUserIds.Count];
            var allTasks = new Task[watchedUserIds.Count * 2];

            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var userId = watchedUserIds[i];
                canonicalTasks[i] = userId <= 0
                    ? Task.FromResult(Array.Empty<RedisValue>())
                    : batch.SetMembersAsync(GatewaysKey(userId));
                legacyTasks[i] = userId <= 0
                    ? Task.FromResult(Array.Empty<RedisValue>())
                    : batch.HashKeysAsync(LegacyKey(userId));
                allTasks[i] = canonicalTasks[i];
                allTasks[watchedUserIds.Count + i] = legacyTasks[i];
            }

            batch.Execute();
            await Task.WhenAll(allTasks).WaitAsync(cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < watchedUserIds.Count; i++)
            {
                var userId = watchedUserIds[i];
                if (userId <= 0)
                    continue;

                result[userId] = MergeInstances(
                    ConvertRedisValuesToStrings(await canonicalTasks[i].ConfigureAwait(false)),
                    await legacyTasks[i].ConfigureAwait(false));
            }
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

        var member = Member(watcherUserId, instanceId);
        var legacyField = LegacyField(instanceId, watcherUserId);

        try
        {
            var db = connectionProvider.Database;
            var batch = db.CreateBatch();
            var expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + WatcherLeaseMs;
            var tasks = new List<Task>((watchedUserIds.Count * 2) + 1);

            foreach (var watchedUserId in watchedUserIds)
            {
                if (watchedUserId <= 0)
                    continue;

                tasks.Add(batch.ScriptEvaluateAsync(
                    AddWatcherScript,
                    [InstancesKey(watchedUserId), GatewaysKey(watchedUserId)],
                    [member, expiryMs, instanceId]));
                tasks.Add(batch.ScriptEvaluateAsync(
                    LegacyWriteScript,
                    [LegacyKey(watchedUserId)],
                    [legacyField, LegacyTtlMs]));
            }

            if (tasks.Count == 0)
                return;

            tasks.Add(batch.SortedSetAddAsync(ActiveShardsKey, instanceId, expiryMs));
            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
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

    public async Task UnregisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (watcherUserId <= 0 || watchedUserIds.Count == 0)
            return;

        var member = Member(watcherUserId, instanceId);
        var legacyField = LegacyField(instanceId, watcherUserId);

        try
        {
            var db = connectionProvider.Database;
            var batch = db.CreateBatch();
            var tasks = new List<Task>(watchedUserIds.Count * 2);

            foreach (var watchedUserId in watchedUserIds)
            {
                if (watchedUserId <= 0)
                    continue;

                tasks.Add(batch.ScriptEvaluateAsync(
                    RemoveWatcherScript,
                    [InstancesKey(watchedUserId), GatewaysKey(watchedUserId)],
                    [member, instanceId]));
                tasks.Add(batch.HashDeleteAsync(LegacyKey(watchedUserId), legacyField));
            }

            if (tasks.Count == 0)
                return;

            batch.Execute();
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
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

    public async Task<IReadOnlyList<string>> ListActiveShardsAsync(
        CancellationToken cancellationToken = default)
    {
        try
        {
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var db = connectionProvider.Database;

            var watcherCleanup = db.SortedSetRemoveRangeByScoreAsync(ActiveShardsKey, -1, nowMs);
            var gatewayCleanup = db.SortedSetRemoveRangeByScoreAsync(GatewayInstancesKey, -1, nowMs);
            await Task.WhenAll(watcherCleanup, gatewayCleanup)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            var watcherTask = db.SortedSetRangeByScoreAsync(
                ActiveShardsKey,
                nowMs,
                double.PositiveInfinity,
                Exclude.Start);
            var gatewayTask = db.SortedSetRangeByScoreAsync(
                GatewayInstancesKey,
                nowMs,
                double.PositiveInfinity,
                Exclude.Start);
            await Task.WhenAll(watcherTask, gatewayTask)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            return MergeInstances(
                ConvertRedisValuesToStrings(await watcherTask.ConfigureAwait(false)),
                ConvertRedisValuesToStrings(await gatewayTask.ConfigureAwait(false)));
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
            return Array.Empty<string>();
        }
    }

    public Task RegisterGatewayInstanceAsync(
        string instanceId,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        return RedisSafeOperation.ExecuteAsync(
            async ct =>
            {
                var expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    + (long)leaseDuration.TotalMilliseconds;
                await connectionProvider.Database
                    .SortedSetAddAsync(GatewayInstancesKey, instanceId, expiryMs)
                    .WaitAsync(ct)
                    .ConfigureAwait(false);
            },
            logger,
            GatewayDependencyOperation.WatcherDirectoryQuery,
            cancellationToken);
    }

    public Task UnregisterGatewayInstanceAsync(
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        return RedisSafeOperation.ExecuteAsync(
            async ct => await connectionProvider.Database
                .SortedSetRemoveAsync(GatewayInstancesKey, instanceId)
                .WaitAsync(ct)
                .ConfigureAwait(false),
            logger,
            GatewayDependencyOperation.WatcherDirectoryQuery,
            cancellationToken);
    }

    /// <summary>
    /// 删除 canonical ZSET + SET，同时清除迁移期旧 HASH。失败仅记录日志。
    /// </summary>
    public Task PurgeUserRoutingAsync(
        long watchedUserId,
        CancellationToken cancellationToken = default)
        => RedisSafeOperation.ExecuteAsync(
            async ct =>
            {
                var db = connectionProvider.Database;
                var batch = db.CreateBatch();
                var instancesTask = batch.KeyDeleteAsync(InstancesKey(watchedUserId));
                var gatewaysTask = batch.KeyDeleteAsync(GatewaysKey(watchedUserId));
                var legacyTask = batch.KeyDeleteAsync(LegacyKey(watchedUserId));
                batch.Execute();
                await Task.WhenAll(instancesTask, gatewaysTask, legacyTask)
                    .WaitAsync(ct)
                    .ConfigureAwait(false);
            },
            logger,
            GatewayDependencyOperation.WatcherDirectoryQuery,
            cancellationToken);

    internal static RedisKey InstancesKey(long watchedUserId) =>
        string.Concat(
            KeyPrefix,
            watchedUserId.ToString(CultureInfo.InvariantCulture),
            InstancesKeySuffix);

    internal static RedisKey GatewaysKey(long watchedUserId) =>
        string.Concat(
            KeyPrefix,
            watchedUserId.ToString(CultureInfo.InvariantCulture),
            GatewaysKeySuffix);

    internal static RedisKey LegacyKey(long watchedUserId) =>
        string.Concat(LegacyKeyPrefix, watchedUserId.ToString(CultureInfo.InvariantCulture));

    internal static string Member(long watcherUserId, string instanceId) =>
        string.Concat(
            watcherUserId.ToString(CultureInfo.InvariantCulture),
            MemberSeparator,
            instanceId);

    internal static string LegacyField(string instanceId, long watcherUserId) =>
        string.Concat(
            instanceId,
            MemberSeparator,
            watcherUserId.ToString(CultureInfo.InvariantCulture));

    internal static IReadOnlyList<string> ExtractLegacyInstances(RedisValue[] fields)
    {
        if (fields.Length == 0)
            return Array.Empty<string>();

        HashSet<string>? instances = null;
        foreach (var value in fields)
        {
            var field = (string?)value;
            if (string.IsNullOrEmpty(field))
                continue;

            // 旧格式是 instanceId:watcherUserId；从最后一个 ':' 拆分以兼容含 ':' 的 instanceId。
            var separator = field.LastIndexOf(MemberSeparator);
            if (separator <= 0
                || !long.TryParse(
                    field.AsSpan(separator + 1),
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var watcherUserId)
                || watcherUserId <= 0)
            {
                continue;
            }

            (instances ??= new HashSet<string>(StringComparer.Ordinal)).Add(field[..separator]);
        }

        return instances is null ? Array.Empty<string>() : [.. instances];
    }

    private static IReadOnlyList<string> ConvertRedisResultToStrings(RedisResult result)
    {
        if (result.IsNull)
            return Array.Empty<string>();

        var values = (RedisResult[]?)result;
        if (values is null || values.Length == 0)
            return Array.Empty<string>();

        var strings = new List<string>(values.Length);
        foreach (var value in values)
        {
            if (value.IsNull)
                continue;

            var text = (string?)value;
            if (!string.IsNullOrEmpty(text))
                strings.Add(text);
        }

        return strings;
    }

    private static IReadOnlyList<string> ConvertRedisValuesToStrings(RedisValue[] values)
    {
        if (values.Length == 0)
            return Array.Empty<string>();

        var strings = new List<string>(values.Length);
        foreach (var value in values)
        {
            var text = (string?)value;
            if (!string.IsNullOrEmpty(text))
                strings.Add(text);
        }

        return strings;
    }

    private static IReadOnlyList<string> MergeInstances(
        IReadOnlyList<string> canonical,
        RedisValue[] legacyFields) =>
        MergeInstances(canonical, ExtractLegacyInstances(legacyFields));

    private static IReadOnlyList<string> MergeInstances(
        IReadOnlyList<string> first,
        IReadOnlyList<string> second)
    {
        if (first.Count == 0)
            return second;
        if (second.Count == 0)
            return first;

        var result = new HashSet<string>(first, StringComparer.Ordinal);
        result.UnionWith(second);
        return [.. result];
    }
}
