using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// 验证 PresenceWatcherRegistry 的容量管理与过期清理行为。
/// 重点：Watch 在容量检查前必须清理当前 watcher 的过期订阅，
/// 否则长期在线但事件稀少的 watcher 会被过期项永久占满 200 条限额。
/// </summary>
public class PresenceWatcherRegistryTests
{
    [Fact]
    public void Watch_WhenAtCapacity_RejectsNewWatch()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var registry = new PresenceWatcherRegistry(clock, TimeSpan.FromMinutes(30));

        // 填满 3 条额度（未过期）。
        registry.Watch(watchedUserId: 10, watcherUserId: 1, maxWatchesPerUser: 3);
        registry.Watch(watchedUserId: 11, watcherUserId: 1, maxWatchesPerUser: 3);
        registry.Watch(watchedUserId: 12, watcherUserId: 1, maxWatchesPerUser: 3);

        // 第 4 条应被拒绝。
        registry.Watch(watchedUserId: 13, watcherUserId: 1, maxWatchesPerUser: 3);

        // 验证：13 未被加入。
        var watchersOf13 = registry.GetWatchers(13);
        Assert.Empty(watchersOf13);
    }

    [Fact]
    public void Watch_AfterExpiry_SweepsStaleEntriesBeforeCapacityCheck()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var ttl = TimeSpan.FromMinutes(30);
        var registry = new PresenceWatcherRegistry(clock, ttl);

        // 填满 3 条额度。
        registry.Watch(watchedUserId: 10, watcherUserId: 1, maxWatchesPerUser: 3);
        registry.Watch(watchedUserId: 11, watcherUserId: 1, maxWatchesPerUser: 3);
        registry.Watch(watchedUserId: 12, watcherUserId: 1, maxWatchesPerUser: 3);

        // 推进时间使所有订阅过期。
        clock.Advance(ttl + TimeSpan.FromSeconds(1));

        // 新订阅应成功：过期项在容量检查前被清理，释放额度。
        registry.Watch(watchedUserId: 13, watcherUserId: 1, maxWatchesPerUser: 3);

        var watchersOf13 = registry.GetWatchers(13);
        Assert.Single(watchersOf13);
        Assert.Equal(1, watchersOf13[0]);
    }

    [Fact]
    public void Watch_AfterExpiry_RemovesFromReverseIndex()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var ttl = TimeSpan.FromMinutes(30);
        var registry = new PresenceWatcherRegistry(clock, ttl);

        registry.Watch(watchedUserId: 10, watcherUserId: 1, maxWatchesPerUser: 3);

        // 推进时间使订阅过期。
        clock.Advance(ttl + TimeSpan.FromSeconds(1));

        // 触发清理（通过 Watch 另一个 watcher，或直接 GetWatchers）。
        // 这里通过 Watch 触发 watcher=1 的清理。
        registry.Watch(watchedUserId: 20, watcherUserId: 1, maxWatchesPerUser: 3);

        // _watchers[10] 应不再包含 watcher=1（过期清理同步移除反向索引）。
        var watchersOf10 = registry.GetWatchers(10);
        Assert.Empty(watchersOf10);
    }

    [Fact]
    public void Watch_RefreshesExistingSubscriptionWithoutConsumingCapacity()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var registry = new PresenceWatcherRegistry(clock, TimeSpan.FromMinutes(30));

        // 已存在的订阅续期，不应占用新额度。
        registry.Watch(watchedUserId: 10, watcherUserId: 1, maxWatchesPerUser: 1);
        registry.Watch(watchedUserId: 10, watcherUserId: 1, maxWatchesPerUser: 1);

        // 容量仍为 1，但 10 已续期。新的不同用户应被拒绝。
        registry.Watch(watchedUserId: 11, watcherUserId: 1, maxWatchesPerUser: 1);
        Assert.Empty(registry.GetWatchers(11));
        Assert.Single(registry.GetWatchers(10));
    }

    [Fact]
    public void Watch_PartialExpiry_OnlySweepsExpiredEntries()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var ttl = TimeSpan.FromMinutes(30);
        var registry = new PresenceWatcherRegistry(clock, ttl);

        // watcher=1 订阅 10（将过期）和 11（稍后加入，未过期）。
        registry.Watch(watchedUserId: 10, watcherUserId: 1, maxWatchesPerUser: 2);
        clock.Advance(ttl - TimeSpan.FromSeconds(1)); // 10 快过期但还没
        registry.Watch(watchedUserId: 11, watcherUserId: 1, maxWatchesPerUser: 2);

        // 推进时间使 10 过期，11 仍有效。
        clock.Advance(TimeSpan.FromSeconds(2));

        // 新订阅应成功：10 被清理释放额度，11 仍保留。
        registry.Watch(watchedUserId: 12, watcherUserId: 1, maxWatchesPerUser: 2);

        Assert.Empty(registry.GetWatchers(10)); // 已过期清理
        Assert.Single(registry.GetWatchers(11)); // 仍有效
        Assert.Single(registry.GetWatchers(12)); // 新加入
    }

    private sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _utc = start;

        public override DateTimeOffset GetUtcNow() => _utc;

        public void Advance(TimeSpan delta) => _utc += delta;
    }
}
