using System.Collections.Concurrent;
using System.Globalization;
using ChatApp.Realtime.Abstractions.Routing;

namespace ChatApp.TcpGateway.Infrastructure.Routing;

/// <summary>
/// 内存实现：用于测试和开发环境。线程安全，支持手动注册/注销 watcher。
/// <para>
/// 存储 watchedUserId -> {复合键 "instanceId:watcherUserId"}，复合键天然幂等，
/// 查询时聚合出唯一 instanceId 集合。
/// </para>
/// </summary>
public sealed class InMemoryWatcherGatewayDirectory : IWatcherGatewayDirectory
{
    private readonly ConcurrentDictionary<long, ConcurrentDictionary<string, bool>> _store = new();

    public Task<IReadOnlyList<string>> GetWatcherGatewaysAsync(
        long watchedUserId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(GetWatcherGatewaysCore(watchedUserId));
    }

    public Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetWatcherGatewaysManyAsync(
        IReadOnlyList<long> watchedUserIds,
        CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<long, IReadOnlyList<string>>(watchedUserIds.Count);
        foreach (var userId in watchedUserIds)
            result[userId] = GetWatcherGatewaysCore(userId);

        return Task.FromResult<IReadOnlyDictionary<long, IReadOnlyList<string>>>(result);
    }

    public Task RegisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (watcherUserId <= 0 || watchedUserIds.Count == 0)
            return Task.CompletedTask;

        foreach (var watchedUserId in watchedUserIds)
        {
            if (watchedUserId <= 0)
                continue;

            var bucket = _store.GetOrAdd(watchedUserId, _ => new ConcurrentDictionary<string, bool>());
            bucket[BuildField(instanceId, watcherUserId)] = true;
        }

        return Task.CompletedTask;
    }

    public Task UnregisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (watcherUserId <= 0 || watchedUserIds.Count == 0)
            return Task.CompletedTask;

        foreach (var watchedUserId in watchedUserIds)
        {
            if (watchedUserId <= 0)
                continue;

            if (!_store.TryGetValue(watchedUserId, out var bucket))
                continue;

            bucket.TryRemove(BuildField(instanceId, watcherUserId), out _);
            if (bucket.IsEmpty)
                _store.TryRemove(watchedUserId, out _);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// 清空所有 watcher 路由记录。
    /// </summary>
    public void Clear() => _store.Clear();

    private IReadOnlyList<string> GetWatcherGatewaysCore(long watchedUserId)
    {
        if (!_store.TryGetValue(watchedUserId, out var bucket) || bucket.IsEmpty)
            return Array.Empty<string>();

        // 复合键格式 "instanceId:watcherUserId"，提取唯一 instanceId。
        var instances = new HashSet<string>(StringComparer.Ordinal);
        foreach (var field in bucket.Keys)
        {
            var sep = field.IndexOf(':');
            if (sep > 0)
                instances.Add(field[..sep]);
        }

        return instances.Count == 0 ? Array.Empty<string>() : new List<string>(instances);
    }

    private static string BuildField(string instanceId, long watcherUserId) =>
        string.Concat(instanceId, ":", watcherUserId.ToString(CultureInfo.InvariantCulture));
}
