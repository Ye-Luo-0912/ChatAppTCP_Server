using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Routing;

namespace ChatApp.TcpGateway.Infrastructure.Routing;

/// <summary>
/// 内存实现：用于测试和开发环境。线程安全，支持手动注册/注销。
/// <para>
/// 存储 userId -> {instanceId -> expiresAtMs}，查询时过滤已过期成员。
/// </para>
/// </summary>
public sealed class InMemoryGatewayDirectory : IGatewayDirectory
{
    private readonly ConcurrentDictionary<long, ConcurrentDictionary<string, long>> _store = new();
    private readonly TimeProvider _timeProvider;

    public InMemoryGatewayDirectory(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// 标记用户在指定实例上线。
    /// </summary>
    /// <param name="userId">用户 ID。</param>
    /// <param name="instanceId">Gateway 实例 ID。</param>
    /// <param name="expiresAtMs">到期 Unix 时间戳（毫秒）。</param>
    public void SetOnline(long userId, string instanceId, long expiresAtMs)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        var bucket = _store.GetOrAdd(userId, _ => new ConcurrentDictionary<string, long>());
        bucket[instanceId] = expiresAtMs;
    }

    /// <summary>
    /// 标记用户在指定实例下线。
    /// </summary>
    /// <param name="userId">用户 ID。</param>
    /// <param name="instanceId">Gateway 实例 ID。</param>
    public void SetOffline(long userId, string instanceId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        if (_store.TryGetValue(userId, out var bucket))
        {
            bucket.TryRemove(instanceId, out _);
            if (bucket.IsEmpty)
                _store.TryRemove(userId, out _);
        }
    }

    /// <summary>
    /// 清空所有路由记录。
    /// </summary>
    public void Clear() => _store.Clear();

    public Task<IReadOnlyList<string>> GetOnlineGatewaysAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        var nowMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        var result = GetOnlineGatewaysCore(userId, nowMs);
        return Task.FromResult<IReadOnlyList<string>>(result);
    }

    public Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetOnlineGatewaysManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default)
    {
        var nowMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        var result = new Dictionary<long, IReadOnlyList<string>>(userIds.Count);

        foreach (var userId in userIds)
            result[userId] = GetOnlineGatewaysCore(userId, nowMs);

        return Task.FromResult<IReadOnlyDictionary<long, IReadOnlyList<string>>>(result);
    }

    private IReadOnlyList<string> GetOnlineGatewaysCore(long userId, long nowMs)
    {
        if (!_store.TryGetValue(userId, out var bucket))
            return Array.Empty<string>();

        if (bucket.IsEmpty)
            return Array.Empty<string>();

        // 过滤过期成员。
        var result = new List<string>(bucket.Count);
        foreach (var (instanceId, expiresAtMs) in bucket)
        {
            if (expiresAtMs > nowMs)
                result.Add(instanceId);
        }

        return result;
    }
}
