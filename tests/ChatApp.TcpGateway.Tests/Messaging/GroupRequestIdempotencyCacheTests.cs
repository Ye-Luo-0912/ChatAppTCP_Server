using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// GroupRequestIdempotencyCache 单元测试：TTL 过期、容量回收、用户级清理、幂等回调。
/// </summary>
public sealed class GroupRequestIdempotencyCacheTests
{
    [Fact]
    public void TryGet_ReturnsNull_ForNewKey()
    {
        var cache = CreateCache();
        var result = cache.TryGet(userId: 1001, requestId: "req-1");
        Assert.Null(result);
    }

    [Fact]
    public void TryAdd_ThenTryGet_ReturnsCachedResult()
    {
        var cache = CreateCache();
        var original = GroupConversationResult.Success("req-1", "conv-1", "Test Group");

        cache.TryAdd(userId: 1001, requestId: "req-1", original);
        var cached = cache.TryGet(userId: 1001, requestId: "req-1");

        Assert.NotNull(cached);
        Assert.Equal("req-1", cached!.RequestId);
        Assert.True(cached.Succeeded);
        Assert.Equal("conv-1", cached.ConversationId);
        Assert.Equal("Test Group", cached.Title);
    }

    [Fact]
    public void TryGet_AfterTtl_ReturnsNull()
    {
        var time = new ManualTimeProvider();
        var cache = CreateCache(timeProvider: time, ttl: TimeSpan.FromSeconds(30));
        var result = GroupConversationResult.Success("req-1", "conv-1");

        cache.TryAdd(userId: 1001, requestId: "req-1", result);

        // 推进超过 TTL，缓存应过期。
        time.Advance(TimeSpan.FromSeconds(31));

        var cached = cache.TryGet(userId: 1001, requestId: "req-1");
        Assert.Null(cached);
    }

    [Fact]
    public void TryGet_WithinTtl_ReturnsCachedResult()
    {
        var time = new ManualTimeProvider();
        var cache = CreateCache(timeProvider: time, ttl: TimeSpan.FromSeconds(30));
        var result = GroupConversationResult.Success("req-1", "conv-1");

        cache.TryAdd(userId: 1001, requestId: "req-1", result);

        // 推进 29 秒（仍在 TTL 内）。
        time.Advance(TimeSpan.FromSeconds(29));

        var cached = cache.TryGet(userId: 1001, requestId: "req-1");
        Assert.NotNull(cached);
        Assert.Equal("conv-1", cached!.ConversationId);
    }

    [Fact]
    public void TryAdd_DoesNotCacheNullResult()
    {
        var cache = CreateCache();
        cache.TryAdd(userId: 1001, requestId: "req-1", result: null!);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void TryAdd_DoesNotCacheEmptyRequestId()
    {
        var cache = CreateCache();
        var result = GroupConversationResult.Success("", "conv-1");
        cache.TryAdd(userId: 1001, requestId: "", result);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void TryGet_DifferentUsersWithSameRequestId_DoNotCollide()
    {
        var cache = CreateCache();
        var result1 = GroupConversationResult.Success("req-1", "conv-1");
        var result2 = GroupConversationResult.Success("req-1", "conv-2");

        cache.TryAdd(userId: 1001, requestId: "req-1", result1);
        cache.TryAdd(userId: 1002, requestId: "req-1", result2);

        var cached1 = cache.TryGet(userId: 1001, requestId: "req-1");
        var cached2 = cache.TryGet(userId: 1002, requestId: "req-1");

        Assert.NotNull(cached1);
        Assert.Equal("conv-1", cached1!.ConversationId);

        Assert.NotNull(cached2);
        Assert.Equal("conv-2", cached2!.ConversationId);
    }

    [Fact]
    public void TryAdd_OverwritesExistingEntry()
    {
        var cache = CreateCache();
        var result1 = GroupConversationResult.Success("req-1", "conv-old");
        var result2 = GroupConversationResult.Success("req-1", "conv-new");

        cache.TryAdd(userId: 1001, requestId: "req-1", result1);
        cache.TryAdd(userId: 1001, requestId: "req-1", result2);

        var cached = cache.TryGet(userId: 1001, requestId: "req-1");
        Assert.NotNull(cached);
        Assert.Equal("conv-new", cached!.ConversationId);
    }

    [Fact]
    public void CachesBusinessFailureResult()
    {
        var cache = CreateCache();
        // 业务失败（如 not_owner）应被缓存——重试得到相同结果。
        var failed = GroupConversationResult.Failed("req-1", "not_owner", "仅群主可执行此操作。");

        cache.TryAdd(userId: 1001, requestId: "req-1", failed);
        var cached = cache.TryGet(userId: 1001, requestId: "req-1");

        Assert.NotNull(cached);
        Assert.False(cached!.Succeeded);
        Assert.Equal("not_owner", cached.ErrorCode);
    }

    [Fact]
    public void CapacityOverflow_TriggersSweep_RemovesExpiredEntries()
    {
        var time = new ManualTimeProvider();
        // 容量 3，TTL 30 秒。
        var cache = CreateCache(timeProvider: time, maxCapacity: 3, ttl: TimeSpan.FromSeconds(30));

        // 填充 3 条。
        for (var i = 0; i < 3; i++)
        {
            cache.TryAdd(userId: 1000 + i, requestId: $"req-{i}", GroupConversationResult.Success($"req-{i}", $"conv-{i}"));
        }
        Assert.Equal(3, cache.Count);

        // 推进 31 秒，使所有条目过期。
        time.Advance(TimeSpan.FromSeconds(31));

        // 添加第 4 条：应触发 sweep 回收过期条目。
        cache.TryAdd(userId: 1003, requestId: "req-3", GroupConversationResult.Success("req-3", "conv-3"));

        // sweep 后容量恢复，新条目应被缓存。
        Assert.Equal(1, cache.Count);
        var cached = cache.TryGet(userId: 1003, requestId: "req-3");
        Assert.NotNull(cached);
    }

    [Fact]
    public void CapacityOverflow_WhenNoExpiredEntries_SkipsCaching()
    {
        var time = new ManualTimeProvider();
        // 容量 2，TTL 30 秒。
        var cache = CreateCache(timeProvider: time, maxCapacity: 2, ttl: TimeSpan.FromSeconds(30));

        cache.TryAdd(userId: 1001, requestId: "req-1", GroupConversationResult.Success("req-1", "conv-1"));
        cache.TryAdd(userId: 1002, requestId: "req-2", GroupConversationResult.Success("req-2", "conv-2"));
        Assert.Equal(2, cache.Count);

        // 未推进时间，所有条目未过期。第 3 条应被跳过（容量满且无过期）。
        cache.TryAdd(userId: 1003, requestId: "req-3", GroupConversationResult.Success("req-3", "conv-3"));

        // 容量不变，新条目未被缓存。
        Assert.Equal(2, cache.Count);
        Assert.Null(cache.TryGet(userId: 1003, requestId: "req-3"));
    }

    [Fact]
    public void EvictUser_RemovesAllEntriesForUser()
    {
        var cache = CreateCache();

        cache.TryAdd(userId: 1001, requestId: "req-1", GroupConversationResult.Success("req-1", "conv-1"));
        cache.TryAdd(userId: 1001, requestId: "req-2", GroupConversationResult.Success("req-2", "conv-2"));
        cache.TryAdd(userId: 1002, requestId: "req-3", GroupConversationResult.Success("req-3", "conv-3"));

        cache.EvictUser(userId: 1001);

        Assert.Null(cache.TryGet(userId: 1001, requestId: "req-1"));
        Assert.Null(cache.TryGet(userId: 1001, requestId: "req-2"));
        // 其他用户的条目不受影响。
        Assert.NotNull(cache.TryGet(userId: 1002, requestId: "req-3"));
    }

    [Fact]
    public void OnLookup_CallbackFires_HitAndMiss()
    {
        var cache = CreateCache();
        var hits = 0;
        var misses = 0;
        cache.OnLookup = hit =>
        {
            if (hit) hits++; else misses++;
        };

        cache.TryGet(userId: 1001, requestId: "req-1"); // miss
        cache.TryAdd(userId: 1001, requestId: "req-1", GroupConversationResult.Success("req-1", "conv-1"));
        cache.TryGet(userId: 1001, requestId: "req-1"); // hit

        Assert.Equal(1, hits);
        Assert.Equal(1, misses);
    }

    [Fact]
    public void OnLookup_CallbackFires_MissForEmptyRequestId()
    {
        var cache = CreateCache();
        var misses = 0;
        cache.OnLookup = hit => { if (!hit) misses++; };

        cache.TryGet(userId: 1001, requestId: "");

        Assert.Equal(1, misses);
    }

    private static GroupRequestIdempotencyCache CreateCache(
        int maxCapacity = 4096,
        TimeSpan? ttl = null,
        TimeProvider? timeProvider = null) =>
        new(maxCapacity, ttl, timeProvider);

    /// <summary>
    /// 手动时间提供者，用于测试 TTL 过期与 sweep。
    /// 与 RedisCircuitBreakerTests 中的 ManualTimeProvider 一致。
    /// </summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        private long _utcTicks = DateTimeOffset.UtcNow.Ticks;

        public override DateTimeOffset GetUtcNow() =>
            new(Volatile.Read(ref _utcTicks), TimeSpan.Zero);

        public void Advance(TimeSpan duration) =>
            Interlocked.Add(ref _utcTicks, duration.Ticks);
    }
}
