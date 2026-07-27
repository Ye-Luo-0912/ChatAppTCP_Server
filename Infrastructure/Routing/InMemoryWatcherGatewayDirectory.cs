using System.Collections.Concurrent;
using System.Globalization;
using ChatApp.Realtime.Abstractions.Routing;

namespace ChatApp.TcpGateway.Infrastructure.Routing;

/// <summary>
/// 内存实现：用于测试和开发环境。线程安全，支持手动注册/注销 watcher。
/// <para>
/// 存储 watchedUserId -> {复合键 "instanceId:watcherUserId" -> 引用计数}。
/// 新接口移除了 <c>gatewaySessionId</c> 参数，改用引用计数维持多会话隔离：
/// 同一 (watchedUserId, watcherUserId, instanceId) 上的多个并发会话各自 Register +1、Unregister -1，
/// 计数归零时移除条目。这样任一会话注销只减少自身引用，其它会话条目保留。
/// </para>
/// <para>
/// 新接口语义：内存实现永不失败，查询结果恒为非空集合或空集合。
/// </para>
/// </summary>
public sealed class InMemoryWatcherGatewayDirectory : IWatcherGatewayDirectory
{
    private readonly ConcurrentDictionary<long, ConcurrentDictionary<string, int>> _store = new();

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

        var field = BuildField(instanceId, watcherUserId);
        foreach (var watchedUserId in watchedUserIds)
        {
            if (watchedUserId <= 0)
                continue;

            var bucket = _store.GetOrAdd(watchedUserId, _ => new ConcurrentDictionary<string, int>());
            // 引用计数 +1；复合 field 天然幂等（同 watcher+instance 的多次 Register 累加计数）。
            bucket.AddOrUpdate(field, addValue: 1, updateValueFactory: (_, c) => c + 1);
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

        var field = BuildField(instanceId, watcherUserId);
        foreach (var watchedUserId in watchedUserIds)
        {
            if (watchedUserId <= 0)
                continue;

            if (!_store.TryGetValue(watchedUserId, out var bucket))
                continue;

            // 引用计数 -1；归零时移除 field，bucket 为空时移除 watchedUserId 条目。
            // 注销不存在的 field 为无操作（TryUpdate 失败）。
            while (bucket.TryGetValue(field, out var current) && current > 0)
            {
                var decremented = current - 1;
                if (bucket.TryUpdate(field, decremented, current))
                {
                    if (decremented <= 0)
                        bucket.TryRemove(field, out _);
                    break;
                }
                // CAS 失败：其它并发写操作改变了值，重试。
            }

            if (bucket.IsEmpty)
                _store.TryRemove(watchedUserId, out _);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> ListActiveShardsAsync(
        CancellationToken cancellationToken = default)
    {
        var instances = new HashSet<string>(StringComparer.Ordinal);
        foreach (var bucket in _store.Values)
        {
            foreach (var field in bucket.Keys)
            {
                var separator = field.IndexOf(':');
                if (separator > 0)
                    instances.Add(field[..separator]);
            }
        }

        return Task.FromResult<IReadOnlyList<string>>(
            instances.Count == 0
                ? Array.Empty<string>()
                : new List<string>(instances));
    }

    /// <summary>
    /// 清空所有 watcher 路由记录。
    /// </summary>
    public void Clear() => _store.Clear();

    private IReadOnlyList<string> GetWatcherGatewaysCore(long watchedUserId)
    {
        if (watchedUserId <= 0)
            return Array.Empty<string>();

        if (!_store.TryGetValue(watchedUserId, out var bucket) || bucket.IsEmpty)
            return Array.Empty<string>();

        // 复合键格式 "instanceId:watcherUserId"，提取唯一 instanceId。
        // 仅取第一个分隔符之前的部分；后续段中若包含 ':' 也不影响 instanceId 提取。
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
        string.Concat(
            instanceId,
            ":",
            watcherUserId.ToString(CultureInfo.InvariantCulture));
}
