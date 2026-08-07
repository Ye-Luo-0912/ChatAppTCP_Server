using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Routing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Tests.Routing;

public sealed class RedisWatcherGatewayDirectoryTests
{
    private const string TestRedisEnvironmentVariable = "CHATAPP_TEST_REDIS";

    [Theory]
    [InlineData(42, "gateway-a", "42:gateway-a")]
    [InlineData(987654321, "region:gateway-b", "987654321:region:gateway-b")]
    public void Member_UsesCanonicalWatcherFirstFormat(
        long watcherUserId,
        string instanceId,
        string expected)
    {
        Assert.Equal(expected, RedisWatcherGatewayDirectory.Member(watcherUserId, instanceId));
    }

    [Fact]
    public void Keys_MatchRealtimeCanonicalSchema()
    {
        Assert.Equal(
            "watchers:123:instances",
            RedisWatcherGatewayDirectory.InstancesKey(123).ToString());
        Assert.Equal(
            "watchers:123:gateways",
            RedisWatcherGatewayDirectory.GatewaysKey(123).ToString());
        Assert.Equal("watchers:__active_shards__", RedisWatcherGatewayDirectory.ActiveShardsKey);
        Assert.Equal("gateway_instances:__active__", RedisWatcherGatewayDirectory.GatewayInstancesKey);
        Assert.Equal(300_000, RedisWatcherGatewayDirectory.WatcherLeaseMs);
    }

    [Fact]
    public void LegacySchema_UsesOldKeyAndInstanceFirstField()
    {
        Assert.Equal("pw:123", RedisWatcherGatewayDirectory.LegacyKey(123).ToString());
        Assert.Equal("gateway-a:456", RedisWatcherGatewayDirectory.LegacyField("gateway-a", 456));
    }

    [Fact]
    public void ExtractLegacyInstances_SupportsColonInInstanceAndRejectsMalformedFields()
    {
        RedisValue[] fields =
        [
            "gateway-a:11",
            "region:gateway-b:12",
            "gateway-a:13",
            "missing-watcher",
            "gateway-c:not-a-number",
            ":14"
        ];

        var instances = RedisWatcherGatewayDirectory.ExtractLegacyInstances(fields);

        Assert.Equal(2, instances.Count);
        Assert.Contains("gateway-a", instances);
        Assert.Contains("region:gateway-b", instances);
    }

    [Fact]
    public async Task RedisRoundTrip_InteroperatesWithCanonicalAndLegacySchemas()
    {
        var connectionString = Environment.GetEnvironmentVariable(TestRedisEnvironmentVariable);
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(connectionString),
            $"Set {TestRedisEnvironmentVariable} to run the Redis interoperability test.");

        var provider = new RedisConnectionProvider(
            Options.Create(new RedisOptions
            {
                ConnectionString = connectionString!,
                StartupTimeout = TimeSpan.FromSeconds(5)
            }),
            NullLogger<RedisConnectionProvider>.Instance);

        await provider.StartAsync(CancellationToken.None);
        try
        {
            var directory = new RedisWatcherGatewayDirectory(
                provider,
                NullLogger<RedisWatcherGatewayDirectory>.Instance);
            var suffix = Guid.NewGuid().ToString("N");
            var watchedUserId = Random.Shared.NextInt64(1_000_000_000, 9_000_000_000);
            var canonicalOnlyUserId = watchedUserId + 1;
            var legacyOnlyUserId = watchedUserId + 2;
            var watcherUserId = watchedUserId + 100;
            var instanceId = $"schema:test:{suffix}";
            var canonicalOnlyInstance = $"canonical:{suffix}";
            var legacyOnlyInstance = $"legacy:{suffix}";
            var heartbeatInstance = $"heartbeat:{suffix}";
            var db = provider.Database;

            try
            {
                await directory.RegisterWatchersAsync(
                    watcherUserId,
                    [watchedUserId],
                    instanceId,
                    CancellationToken.None);
                await directory.RegisterWatchersAsync(
                    watcherUserId,
                    [watchedUserId],
                    instanceId,
                    CancellationToken.None);

                Assert.Equal(RedisType.SortedSet, await db.KeyTypeAsync(
                    RedisWatcherGatewayDirectory.InstancesKey(watchedUserId)));
                Assert.Equal(RedisType.Set, await db.KeyTypeAsync(
                    RedisWatcherGatewayDirectory.GatewaysKey(watchedUserId)));
                Assert.Equal(
                    1,
                    await db.SortedSetLengthAsync(
                        RedisWatcherGatewayDirectory.InstancesKey(watchedUserId)));
                Assert.True(await db.SetContainsAsync(
                    RedisWatcherGatewayDirectory.GatewaysKey(watchedUserId),
                    instanceId));
                Assert.NotNull(await db.SortedSetScoreAsync(
                    RedisWatcherGatewayDirectory.InstancesKey(watchedUserId),
                    $"{watcherUserId}:{instanceId}"));
                Assert.True(await db.HashExistsAsync(
                    RedisWatcherGatewayDirectory.LegacyKey(watchedUserId),
                    $"{instanceId}:{watcherUserId}"));
                Assert.NotNull(await db.SortedSetScoreAsync(
                    RedisWatcherGatewayDirectory.ActiveShardsKey,
                    instanceId));

                // 模拟 RealtimeServices canonical writer，验证 Gateway 可直接读取。
                var futureMs = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds();
                await db.SortedSetAddAsync(
                    RedisWatcherGatewayDirectory.InstancesKey(canonicalOnlyUserId),
                    $"{watcherUserId}:{canonicalOnlyInstance}",
                    futureMs);
                await db.SetAddAsync(
                    RedisWatcherGatewayDirectory.GatewaysKey(canonicalOnlyUserId),
                    canonicalOnlyInstance);

                var canonicalRoutes = await directory.GetWatcherGatewaysAsync(
                    canonicalOnlyUserId,
                    CancellationToken.None);
                Assert.Contains(canonicalOnlyInstance, canonicalRoutes);

                // 迁移期兼容：旧 Gateway 仅写 pw:* HASH 时仍可读取。
                await db.HashSetAsync(
                    RedisWatcherGatewayDirectory.LegacyKey(legacyOnlyUserId),
                    $"{legacyOnlyInstance}:{watcherUserId}",
                    1);
                var legacyRoutes = await directory.GetWatcherGatewaysAsync(
                    legacyOnlyUserId,
                    CancellationToken.None);
                Assert.Contains(legacyOnlyInstance, legacyRoutes);

                var routesByUser = await directory.GetWatcherGatewaysManyAsync(
                    [watchedUserId, canonicalOnlyUserId, legacyOnlyUserId],
                    CancellationToken.None);
                Assert.Contains(instanceId, routesByUser[watchedUserId]);
                Assert.Contains(canonicalOnlyInstance, routesByUser[canonicalOnlyUserId]);
                Assert.Contains(legacyOnlyInstance, routesByUser[legacyOnlyUserId]);

                await directory.RegisterGatewayInstanceAsync(
                    heartbeatInstance,
                    TimeSpan.FromMinutes(1),
                    CancellationToken.None);
                Assert.NotNull(await db.SortedSetScoreAsync(
                    RedisWatcherGatewayDirectory.GatewayInstancesKey,
                    heartbeatInstance));
                var activeShards = await directory.ListActiveShardsAsync(CancellationToken.None);
                Assert.Contains(instanceId, activeShards);
                Assert.Contains(heartbeatInstance, activeShards);
                await directory.UnregisterGatewayInstanceAsync(
                    heartbeatInstance,
                    CancellationToken.None);
                Assert.Null(await db.SortedSetScoreAsync(
                    RedisWatcherGatewayDirectory.GatewayInstancesKey,
                    heartbeatInstance));

                await directory.UnregisterWatchersAsync(
                    watcherUserId,
                    [watchedUserId],
                    instanceId,
                    CancellationToken.None);
                Assert.Null(await db.SortedSetScoreAsync(
                    RedisWatcherGatewayDirectory.InstancesKey(watchedUserId),
                    $"{watcherUserId}:{instanceId}"));
                Assert.False(await db.SetContainsAsync(
                    RedisWatcherGatewayDirectory.GatewaysKey(watchedUserId),
                    instanceId));
                Assert.False(await db.HashExistsAsync(
                    RedisWatcherGatewayDirectory.LegacyKey(watchedUserId),
                    $"{instanceId}:{watcherUserId}"));
            }
            finally
            {
                await DeleteUserKeysAsync(db, watchedUserId);
                await DeleteUserKeysAsync(db, canonicalOnlyUserId);
                await DeleteUserKeysAsync(db, legacyOnlyUserId);
                await db.SortedSetRemoveAsync(RedisWatcherGatewayDirectory.ActiveShardsKey, instanceId);
                await db.SortedSetRemoveAsync(RedisWatcherGatewayDirectory.GatewayInstancesKey, instanceId);
                await db.SortedSetRemoveAsync(
                    RedisWatcherGatewayDirectory.GatewayInstancesKey,
                    heartbeatInstance);
            }
        }
        finally
        {
            await provider.StopAsync(CancellationToken.None);
            await provider.DisposeAsync();
        }
    }

    private static async Task DeleteUserKeysAsync(IDatabase db, long watchedUserId)
    {
        await Task.WhenAll(
            db.KeyDeleteAsync(RedisWatcherGatewayDirectory.InstancesKey(watchedUserId)),
            db.KeyDeleteAsync(RedisWatcherGatewayDirectory.GatewaysKey(watchedUserId)),
            db.KeyDeleteAsync(RedisWatcherGatewayDirectory.LegacyKey(watchedUserId)));
    }
}
