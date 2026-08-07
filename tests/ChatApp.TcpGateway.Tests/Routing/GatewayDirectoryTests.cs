using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.TcpGateway.Infrastructure.Routing;

namespace ChatApp.TcpGateway.Tests.Routing;

public sealed class GatewayDirectoryTests
{
    private static readonly long FutureMs =
        DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds();
    private static readonly long PastMs =
        DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds();

    // ---- InMemoryGatewayDirectory: GetOnlineGatewaysAsync ----

    [Fact]
    public async Task InMemory_GetOnlineGateways_ReturnsRegisteredInstances()
    {
        var directory = new InMemoryGatewayDirectory();
        directory.SetOnline(1001, "gateway-A", FutureMs);
        directory.SetOnline(1001, "gateway-B", FutureMs);

        var gateways = await directory.GetOnlineGatewaysAsync(1001, CancellationToken.None);

        Assert.Equal(2, gateways.Count);
        Assert.Contains("gateway-A", gateways);
        Assert.Contains("gateway-B", gateways);
    }

    [Fact]
    public async Task InMemory_GetOnlineGateways_FiltersExpired()
    {
        var directory = new InMemoryGatewayDirectory();
        directory.SetOnline(1001, "gateway-A", PastMs); // expired
        directory.SetOnline(1001, "gateway-B", FutureMs); // valid

        var gateways = await directory.GetOnlineGatewaysAsync(1001, CancellationToken.None);

        Assert.Single(gateways);
        Assert.Contains("gateway-B", gateways);
    }

    [Fact]
    public async Task InMemory_GetOnlineGateways_ReturnsEmpty_WhenUserOffline()
    {
        var directory = new InMemoryGatewayDirectory();

        var gateways = await directory.GetOnlineGatewaysAsync(9999, CancellationToken.None);

        // 内存实现永不失败，离线用户返回空集合。
        Assert.Empty(gateways);
    }

    [Fact]
    public async Task InMemory_GetOnlineGateways_ReturnsEmpty_ForNonPositiveUserId()
    {
        var directory = new InMemoryGatewayDirectory();
        directory.SetOnline(1, "gateway-A", FutureMs);

        var gateways = await directory.GetOnlineGatewaysAsync(0, CancellationToken.None);

        Assert.Empty(gateways);
    }

    [Fact]
    public async Task InMemory_SetOffline_RemovesInstance()
    {
        var directory = new InMemoryGatewayDirectory();
        directory.SetOnline(1001, "gateway-A", FutureMs);
        directory.SetOnline(1001, "gateway-B", FutureMs);

        directory.SetOffline(1001, "gateway-A");
        var gateways = await directory.GetOnlineGatewaysAsync(1001, CancellationToken.None);

        Assert.Single(gateways);
        Assert.Contains("gateway-B", gateways);
    }

    [Fact]
    public async Task InMemory_SetOffline_LastInstance_RemovesUserEntry()
    {
        var directory = new InMemoryGatewayDirectory();
        directory.SetOnline(1001, "gateway-A", FutureMs);

        directory.SetOffline(1001, "gateway-A");
        var gateways = await directory.GetOnlineGatewaysAsync(1001, CancellationToken.None);

        Assert.Empty(gateways);
    }

    [Fact]
    public async Task InMemory_SetOnline_OverwritesExpiry()
    {
        var directory = new InMemoryGatewayDirectory();
        directory.SetOnline(1001, "gateway-A", PastMs); // expired

        // Refresh to future.
        directory.SetOnline(1001, "gateway-A", FutureMs);
        var gateways = await directory.GetOnlineGatewaysAsync(1001, CancellationToken.None);

        Assert.Single(gateways);
        Assert.Contains("gateway-A", gateways);
    }

    // ---- InMemoryGatewayDirectory: GetOnlineGatewaysManyAsync ----

    [Fact]
    public async Task InMemory_GetOnlineGatewaysMany_ReturnsAllUsers()
    {
        var directory = new InMemoryGatewayDirectory();
        directory.SetOnline(1001, "gateway-A", FutureMs);
        directory.SetOnline(1002, "gateway-B", FutureMs);
        directory.SetOnline(1002, "gateway-C", FutureMs);

        var result = await directory.GetOnlineGatewaysManyAsync([1001, 1002, 1003], CancellationToken.None);

        Assert.Equal(3, result.Count);
        Assert.Single(result[1001]);
        Assert.Contains("gateway-A", result[1001]);
        Assert.Equal(2, result[1002].Count);
        Assert.Contains("gateway-B", result[1002]);
        Assert.Contains("gateway-C", result[1002]);
        Assert.Empty(result[1003]);
    }

    [Fact]
    public async Task InMemory_GetOnlineGatewaysMany_EmptyInput_ReturnsEmptyDict()
    {
        var directory = new InMemoryGatewayDirectory();

        var result = await directory.GetOnlineGatewaysManyAsync(Array.Empty<long>(), CancellationToken.None);

        Assert.Empty(result);
    }

    [Fact]
    public async Task InMemory_Clear_RemovesAllEntries()
    {
        var directory = new InMemoryGatewayDirectory();
        directory.SetOnline(1001, "gateway-A", FutureMs);
        directory.SetOnline(1002, "gateway-B", FutureMs);

        directory.Clear();

        var result = await directory.GetOnlineGatewaysManyAsync([1001, 1002], CancellationToken.None);
        Assert.Empty(result[1001]);
        Assert.Empty(result[1002]);
    }

    // ---- NullGatewayDirectory ----

    [Fact]
    public async Task NullDirectory_GetOnlineGateways_ReturnsEmpty()
    {
        // NullGatewayDirectory 始终返回空集合，触发调用方回退广播。
        var gateways = await NullGatewayDirectory.Instance.GetOnlineGatewaysAsync(1001, CancellationToken.None);

        Assert.Empty(gateways);
    }

    [Fact]
    public async Task NullDirectory_GetOnlineGatewaysMany_ReturnsEmptyDict()
    {
        var result = await NullGatewayDirectory.Instance
            .GetOnlineGatewaysManyAsync([1001, 1002], CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public void NullDirectory_Instance_IsSingleton()
    {
        Assert.Same(NullGatewayDirectory.Instance, NullGatewayDirectory.Instance);
    }

    // ---- ShardedSubjectFormatter ----

    [Fact]
    public void Formatter_Format_ReplacesPlaceholder()
    {
        var subject = ShardedSubjectFormatter.Format(
            "chat.realtime-events.shards.{0}",
            "gateway-01");

        Assert.Equal("chat.realtime-events.shards.gateway-01", subject);
    }

    [Fact]
    public void Formatter_Format_NoPlaceholder_ReturnsAsIs()
    {
        var subject = ShardedSubjectFormatter.Format(
            "chat.realtime-events",
            "gateway-01");

        Assert.Equal("chat.realtime-events", subject);
    }

    [Fact]
    public void Formatter_IsSharded_True_WhenPlaceholderPresent()
    {
        Assert.True(ShardedSubjectFormatter.IsSharded("chat.realtime-events.shards.{0}"));
    }

    [Fact]
    public void Formatter_IsSharded_False_WhenNoPlaceholder()
    {
        Assert.False(ShardedSubjectFormatter.IsSharded("chat.realtime-events"));
    }

    [Fact]
    public void Formatter_IsSharded_False_WhenNull()
    {
        Assert.False(ShardedSubjectFormatter.IsSharded(null!));
    }

    [Fact]
    public void Formatter_IsSharded_False_WhenEmpty()
    {
        Assert.False(ShardedSubjectFormatter.IsSharded(""));
    }

    [Fact]
    public void Formatter_ToWildcard_ReplacesPlaceholderWithGt()
    {
        var wildcard = ShardedSubjectFormatter.ToWildcard("chat.realtime-events.shards.{0}");

        Assert.Equal("chat.realtime-events.shards.>", wildcard);
    }

    [Fact]
    public void Formatter_ToWildcard_NoPlaceholder_ReturnsAsIs()
    {
        var wildcard = ShardedSubjectFormatter.ToWildcard("chat.realtime-events");

        Assert.Equal("chat.realtime-events", wildcard);
    }

    // ---- InMemoryWatcherGatewayDirectory: RegisterWatchersAsync / GetWatcherGatewaysAsync ----

    [Fact]
    public async Task Watcher_Register_ThenQuery_ReturnsInstance()
    {
        var directory = new InMemoryWatcherGatewayDirectory();

        await directory.RegisterWatchersAsync(
            watcherUserId: 1001,
            watchedUserIds: [2001, 2002],
            instanceId: "gateway-A",
            CancellationToken.None);

        var gatewaysFor2001 = await directory.GetWatcherGatewaysAsync(2001, CancellationToken.None);
        var gatewaysFor2002 = await directory.GetWatcherGatewaysAsync(2002, CancellationToken.None);

        Assert.Single(gatewaysFor2001);
        Assert.Contains("gateway-A", gatewaysFor2001);
        Assert.Single(gatewaysFor2002);
        Assert.Contains("gateway-A", gatewaysFor2002);
    }

    [Fact]
    public async Task Watcher_Register_AggregatesAcrossInstancesAndWatchers()
    {
        var directory = new InMemoryWatcherGatewayDirectory();

        // 两个 watcher 分别在两个实例上观察同一用户。
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);
        await directory.RegisterWatchersAsync(1002, [5001], "gateway-B", CancellationToken.None);

        var gateways = await directory.GetWatcherGatewaysAsync(5001, CancellationToken.None);

        Assert.Equal(2, gateways.Count);
        Assert.Contains("gateway-A", gateways);
        Assert.Contains("gateway-B", gateways);
    }

    [Fact]
    public async Task Watcher_Register_IsIdempotent_ForSameWatcherAndInstance()
    {
        var directory = new InMemoryWatcherGatewayDirectory();

        // 同一 (watcher, instance, watched) 重复注册累加计数，但查询结果仍只出现一个 instance。
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);

        var gateways = await directory.GetWatcherGatewaysAsync(5001, CancellationToken.None);

        Assert.Single(gateways);
        Assert.Contains("gateway-A", gateways);
    }

    [Fact]
    public async Task Watcher_Register_DistinctSessions_AreTrackedViaRefCount()
    {
        // 新接口通过引用计数维持多会话隔离：同一 watcher 在同一 Gateway 上的多次 Register 累加计数，
        // 单次 Unregister 只减少计数（计数 > 0 时 instance 仍出现在查询结果中）。
        var directory = new InMemoryWatcherGatewayDirectory();

        // 模拟两个并发会话各自 Register（计数 = 2）。
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);

        // 注销其中一个会话（计数 = 1，仍 > 0）。
        await directory.UnregisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);

        var gateways = await directory.GetWatcherGatewaysAsync(5001, CancellationToken.None);
        Assert.Single(gateways);
        Assert.Contains("gateway-A", gateways);
    }

    [Fact]
    public async Task Watcher_Unregister_RemovesInstance_WhenNoWatchersRemain()
    {
        var directory = new InMemoryWatcherGatewayDirectory();
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);

        await directory.UnregisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);

        var gateways = await directory.GetWatcherGatewaysAsync(5001, CancellationToken.None);
        Assert.Empty(gateways);
    }

    [Fact]
    public async Task Watcher_Unregister_KeepsInstance_WhenOtherWatchersRemain()
    {
        var directory = new InMemoryWatcherGatewayDirectory();
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);
        await directory.RegisterWatchersAsync(1002, [5001], "gateway-A", CancellationToken.None);

        // 注销其中一个 watcher，另一 watcher 仍使 gateway-A 保留在结果中。
        await directory.UnregisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);

        var gateways = await directory.GetWatcherGatewaysAsync(5001, CancellationToken.None);
        Assert.Single(gateways);
        Assert.Contains("gateway-A", gateways);
    }

    [Fact]
    public async Task Watcher_Unregister_IsIdempotent_ForNonExistentRelation()
    {
        var directory = new InMemoryWatcherGatewayDirectory();

        // 注销不存在的观察关系应为无操作，不抛异常。
        await directory.UnregisterWatchersAsync(9999, [5001], "gateway-A", CancellationToken.None);

        var gateways = await directory.GetWatcherGatewaysAsync(5001, CancellationToken.None);
        Assert.Empty(gateways);
    }

    [Fact]
    public async Task Watcher_GetWatcherGateways_ReturnsEmpty_WhenUserNotWatched()
    {
        var directory = new InMemoryWatcherGatewayDirectory();

        var gateways = await directory.GetWatcherGatewaysAsync(9999, CancellationToken.None);

        // 内存实现永不失败，无人观察返回空集合。
        Assert.Empty(gateways);
    }

    [Fact]
    public async Task Watcher_GetWatcherGateways_ReturnsEmpty_ForNonPositiveUserId()
    {
        var directory = new InMemoryWatcherGatewayDirectory();
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);

        var gateways = await directory.GetWatcherGatewaysAsync(0, CancellationToken.None);

        Assert.Empty(gateways);
    }

    [Fact]
    public async Task Watcher_Register_SkipsNonPositiveWatchedAndWatcherIds()
    {
        var directory = new InMemoryWatcherGatewayDirectory();

        await directory.RegisterWatchersAsync(
            watcherUserId: 0,
            watchedUserIds: [5001],
            instanceId: "gateway-A",
            CancellationToken.None);
        await directory.RegisterWatchersAsync(
            watcherUserId: 1001,
            watchedUserIds: [0, -1, 5001],
            instanceId: "gateway-A",
            CancellationToken.None);

        // 仅 5001 应被登记（watcher=0 的整体跳过，watched<=0 的逐项跳过）。
        var gateways = await directory.GetWatcherGatewaysAsync(5001, CancellationToken.None);
        Assert.Single(gateways);
        Assert.Contains("gateway-A", gateways);

        var emptyForZero = await directory.GetWatcherGatewaysAsync(0, CancellationToken.None);
        Assert.Empty(emptyForZero);
    }

    [Fact]
    public async Task Watcher_GetWatcherGatewaysMany_ReturnsAllUsers()
    {
        var directory = new InMemoryWatcherGatewayDirectory();
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);
        await directory.RegisterWatchersAsync(1002, [5002], "gateway-B", CancellationToken.None);

        var result = await directory.GetWatcherGatewaysManyAsync([5001, 5002, 5003], CancellationToken.None);

        Assert.Equal(3, result.Count);
        Assert.Single(result[5001]);
        Assert.Contains("gateway-A", result[5001]);
        Assert.Single(result[5002]);
        Assert.Contains("gateway-B", result[5002]);
        Assert.Empty(result[5003]);
    }

    [Fact]
    public async Task Watcher_GetWatcherGatewaysMany_EmptyInput_ReturnsEmptyDict()
    {
        var directory = new InMemoryWatcherGatewayDirectory();

        var result = await directory.GetWatcherGatewaysManyAsync(Array.Empty<long>(), CancellationToken.None);

        Assert.Empty(result);
    }

    [Fact]
    public async Task Watcher_Clear_RemovesAllEntries()
    {
        var directory = new InMemoryWatcherGatewayDirectory();
        await directory.RegisterWatchersAsync(1001, [5001], "gateway-A", CancellationToken.None);

        directory.Clear();

        var result = await directory.GetWatcherGatewaysManyAsync([5001], CancellationToken.None);
        Assert.Empty(result[5001]);
    }

    // ---- NullWatcherGatewayDirectory ----

    [Fact]
    public async Task NullWatcher_GetWatcherGateways_ReturnsEmpty()
    {
        // NullWatcherGatewayDirectory 始终返回空集合，触发调用方回退广播。
        var gateways = await NullWatcherGatewayDirectory.Instance
            .GetWatcherGatewaysAsync(1001, CancellationToken.None);

        Assert.Empty(gateways);
    }

    [Fact]
    public async Task NullWatcher_GetWatcherGatewaysMany_ReturnsEmptyDict()
    {
        var result = await NullWatcherGatewayDirectory.Instance
            .GetWatcherGatewaysManyAsync([1001, 1002], CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task NullWatcher_RegisterAndUnregister_AreNoOps()
    {
        // 注册后查询仍为空，证明写操作无副作用。
        await NullWatcherGatewayDirectory.Instance.RegisterWatchersAsync(
            1001, [5001], "gateway-A", CancellationToken.None);
        await NullWatcherGatewayDirectory.Instance.UnregisterWatchersAsync(
            1001, [5001], "gateway-A", CancellationToken.None);

        var gateways = await NullWatcherGatewayDirectory.Instance
            .GetWatcherGatewaysAsync(5001, CancellationToken.None);
        Assert.Empty(gateways);
    }

    [Fact]
    public void NullWatcher_Instance_IsSingleton()
    {
        Assert.Same(NullWatcherGatewayDirectory.Instance, NullWatcherGatewayDirectory.Instance);
    }
}
