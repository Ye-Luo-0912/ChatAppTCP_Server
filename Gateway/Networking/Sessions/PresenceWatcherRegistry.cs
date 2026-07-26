using System.Collections.Concurrent;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 在线状态观察者：PresenceQuery 时本机登记；用户上/下线时通知观察者。
/// 跨 Gateway 在线态由 NATS Core ephemeral + Redis 全局键同步；本表仍只存本机 watcher 订阅关系（不进 Outbox）。
/// </summary>
internal sealed class PresenceWatcherRegistry
{
    public const int DefaultMaxWatchesPerUser = 200;
    public static readonly TimeSpan DefaultWatchTtl = TimeSpan.FromMinutes(30);

    private readonly ConcurrentDictionary<long, ConcurrentDictionary<long, long>> _watchers = new();
    private readonly ConcurrentDictionary<long, ConcurrentDictionary<long, long>> _watching = new();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _watchTtl;

    public PresenceWatcherRegistry(
        TimeProvider? timeProvider = null,
        TimeSpan? watchTtl = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _watchTtl = watchTtl ?? DefaultWatchTtl;
    }

    public void Watch(long watchedUserId, long watcherUserId, int maxWatchesPerUser = DefaultMaxWatchesPerUser)
    {
        if (watchedUserId <= 0 || watcherUserId <= 0 || watchedUserId == watcherUserId)
            return;

        var now = _timeProvider.GetUtcNow();
        var expireAt = now.Add(_watchTtl).ToUnixTimeMilliseconds();
        var nowMs = now.ToUnixTimeMilliseconds();
        var mine = _watching.GetOrAdd(
            watcherUserId,
            static _ => new ConcurrentDictionary<long, long>());

        // 容量检查前清理当前 watcher 的过期订阅，释放额度。
        // 过期记录仅在对应被观察用户触发 GetWatchers 时被动清理，
        // 长期在线但事件稀少的 watcher 可能被过期项占满 maxWatchesPerUser 限额，
        // 导致新订阅被永久拒绝。
        SweepExpiredForWatcher(watcherUserId, mine, nowMs);

        if (!mine.ContainsKey(watchedUserId) && mine.Count >= Math.Max(1, maxWatchesPerUser))
            return;

        mine[watchedUserId] = expireAt;
        var set = _watchers.GetOrAdd(
            watchedUserId,
            static _ => new ConcurrentDictionary<long, long>());
        set[watcherUserId] = expireAt;
    }

    /// <summary>
    /// 清理指定 watcher 的过期订阅，同步从反向索引 <see cref="_watchers"/> 移除。
    /// </summary>
    private void SweepExpiredForWatcher(
        long watcherUserId,
        ConcurrentDictionary<long, long> mine,
        long nowMs)
    {
        foreach (var pair in mine)
        {
            if (pair.Value <= 0 || pair.Value >= nowMs)
                continue;

            if (!mine.TryRemove(pair.Key, out _))
                continue;

            if (_watchers.TryGetValue(pair.Key, out var set))
            {
                set.TryRemove(watcherUserId, out _);
                if (set.IsEmpty)
                    _watchers.TryRemove(pair.Key, out _);
            }
        }
    }

    public void WatchMany(
        IEnumerable<long> watchedUserIds,
        long watcherUserId,
        int maxWatchesPerUser = DefaultMaxWatchesPerUser)
    {
        foreach (var id in watchedUserIds)
            Watch(id, watcherUserId, maxWatchesPerUser);
    }

    public void Unwatch(long watchedUserId, long watcherUserId)
    {
        if (_watching.TryGetValue(watcherUserId, out var mine))
            mine.TryRemove(watchedUserId, out _);

        if (!_watchers.TryGetValue(watchedUserId, out var set)) 
            return;
        
        set.TryRemove(watcherUserId, out _);
        if (set.IsEmpty)
            _watchers.TryRemove(watchedUserId, out _);
    }

    public void UnwatchMany(IEnumerable<long> watchedUserIds, long watcherUserId)
    {
        foreach (var id in watchedUserIds)
            Unwatch(id, watcherUserId);
    }

    public long[] GetWatchers(long watchedUserId)
    {
        if (!_watchers.TryGetValue(watchedUserId, out var set))
            return [];

        var now = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        List<long>? expired = null;
        foreach (var pair in set)
        {
            if (pair.Value > 0 && pair.Value < now)
                (expired ??= []).Add(pair.Key);
        }

        if (expired is not null)
        {
            foreach (var watcherId in expired)
                Unwatch(watchedUserId, watcherId);
        }

        if (!_watchers.TryGetValue(watchedUserId, out set) || set.IsEmpty)
            return [];

        return [.. set.Keys];
    }

    /// <summary>移除某用户作为观察者（其会话全部断开时）。</summary>
    public void RemoveWatcher(long watcherUserId)
    {
        if (watcherUserId <= 0)
            return;

        if (!_watching.TryRemove(watcherUserId, out var mine)) 
            return;
        
        foreach (var watchedId in mine.Keys)
        {
            if (!_watchers.TryGetValue(watchedId, out var set)) 
                continue;
            
            set.TryRemove(watcherUserId, out _);
            if (set.IsEmpty)
                _watchers.TryRemove(watchedId, out _);
        }
    }
}
