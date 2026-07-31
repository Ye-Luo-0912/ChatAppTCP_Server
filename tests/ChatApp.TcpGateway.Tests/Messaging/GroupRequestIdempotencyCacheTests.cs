using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using ChatApp.TcpGateway.Infrastructure.GroupIdempotency;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// GroupRequestIdempotencyCache 单元测试：TTL 过期、容量回收、用户级清理、幂等回调、冲突检测。
/// </summary>
public sealed class GroupRequestIdempotencyCacheTests
{
    private const int DefaultOperation = 1;
    private const string DefaultPayloadHash = "0";

    [Fact]
    public void TryGet_ReturnsMiss_ForNewKey()
    {
        var cache = CreateCache();
        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash);
        Assert.True(lookup.IsMiss());
        Assert.Null(lookup.Result);
    }

    [Fact]
    public void TryAdd_ThenTryGet_ReturnsCachedResult()
    {
        var cache = CreateCache();
        var original = GroupConversationResult.Success("req-1", "conv-1", "Test Group");

        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash, original);
        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash);

        Assert.True(lookup.IsHit);
        Assert.NotNull(lookup.Result);
        Assert.Equal("req-1", lookup.Result!.RequestId);
        Assert.True(lookup.Result.Succeeded);
        Assert.Equal("conv-1", lookup.Result.ConversationId);
        Assert.Equal("Test Group", lookup.Result.Title);
    }

    [Fact]
    public void TryGet_AfterTtl_ReturnsMiss()
    {
        var time = new ManualTimeProvider();
        var cache = CreateCache(timeProvider: time, ttl: TimeSpan.FromSeconds(30));
        var result = GroupConversationResult.Success("req-1", "conv-1");

        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash, result);

        // 推进超过 TTL，缓存应过期。
        time.Advance(TimeSpan.FromSeconds(31));

        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash);
        Assert.True(lookup.IsMiss());
        Assert.Null(lookup.Result);
    }

    [Fact]
    public void TryGet_WithinTtl_ReturnsCachedResult()
    {
        var time = new ManualTimeProvider();
        var cache = CreateCache(timeProvider: time, ttl: TimeSpan.FromSeconds(30));
        var result = GroupConversationResult.Success("req-1", "conv-1");

        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash, result);

        // 推进 29 秒（仍在 TTL 内）。
        time.Advance(TimeSpan.FromSeconds(29));

        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash);
        Assert.True(lookup.IsHit);
        Assert.NotNull(lookup.Result);
        Assert.Equal("conv-1", lookup.Result!.ConversationId);
    }

    [Fact]
    public void TryAdd_DoesNotCacheNullResult()
    {
        var cache = CreateCache();
        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash, null!);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void TryAdd_DoesNotCacheEmptyRequestId()
    {
        var cache = CreateCache();
        var result = GroupConversationResult.Success("", "conv-1");
        cache.TryAdd(1001, DefaultOperation, "", DefaultPayloadHash, result);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void TryGet_DifferentUsersWithSameRequestId_DoNotCollide()
    {
        var cache = CreateCache();
        var result1 = GroupConversationResult.Success("req-1", "conv-1");
        var result2 = GroupConversationResult.Success("req-1", "conv-2");

        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash, result1);
        cache.TryAdd(1002, DefaultOperation, "req-1", DefaultPayloadHash, result2);

        var lookup1 = cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash);
        var lookup2 = cache.TryGet(1002, DefaultOperation, "req-1", DefaultPayloadHash);

        Assert.True(lookup1.IsHit);
        Assert.Equal("conv-1", lookup1.Result!.ConversationId);

        Assert.True(lookup2.IsHit);
        Assert.Equal("conv-2", lookup2.Result!.ConversationId);
    }

    [Fact]
    public void TryGet_DifferentOperationsWithSameRequestId_DoNotCollide()
    {
        var cache = CreateCache();
        var result1 = GroupConversationResult.Success("req-1", "conv-1");
        var result2 = GroupConversationResult.Success("req-1", "conv-2");

        cache.TryAdd(1001, operation: 1, "req-1", DefaultPayloadHash, result1);
        cache.TryAdd(1001, operation: 2, "req-1", DefaultPayloadHash, result2);

        var lookup1 = cache.TryGet(1001, operation: 1, "req-1", DefaultPayloadHash);
        var lookup2 = cache.TryGet(1001, operation: 2, "req-1", DefaultPayloadHash);

        Assert.True(lookup1.IsHit);
        Assert.Equal("conv-1", lookup1.Result!.ConversationId);

        Assert.True(lookup2.IsHit);
        Assert.Equal("conv-2", lookup2.Result!.ConversationId);
    }

    [Fact]
    public void TryAdd_OverwritesExistingEntry()
    {
        var cache = CreateCache();
        var result1 = GroupConversationResult.Success("req-1", "conv-old");
        var result2 = GroupConversationResult.Success("req-1", "conv-new");

        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash, result1);
        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash, result2);

        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash);
        Assert.True(lookup.IsHit);
        Assert.Equal("conv-new", lookup.Result!.ConversationId);
    }

    [Fact]
    public void CachesBusinessFailureResult()
    {
        var cache = CreateCache();
        // 业务失败（如 not_owner）应被缓存——重试得到相同结果。
        var failed = GroupConversationResult.Failed("req-1", "not_owner", "仅群主可执行此操作。");

        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash, failed);
        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash);

        Assert.True(lookup.IsHit);
        Assert.NotNull(lookup.Result);
        Assert.False(lookup.Result!.Succeeded);
        Assert.Equal("not_owner", lookup.Result.ErrorCode);
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
            cache.TryAdd(1000 + i, DefaultOperation, $"req-{i}", DefaultPayloadHash,
                GroupConversationResult.Success($"req-{i}", $"conv-{i}"));
        }
        Assert.Equal(3, cache.Count);

        // 推进 31 秒，使所有条目过期。
        time.Advance(TimeSpan.FromSeconds(31));

        // 添加第 4 条：应触发 sweep 回收过期条目。
        cache.TryAdd(1003, DefaultOperation, "req-3", DefaultPayloadHash,
            GroupConversationResult.Success("req-3", "conv-3"));

        // sweep 后容量恢复，新条目应被缓存。
        Assert.Equal(1, cache.Count);
        var lookup = cache.TryGet(1003, DefaultOperation, "req-3", DefaultPayloadHash);
        Assert.True(lookup.IsHit);
    }

    [Fact]
    public void CapacityOverflow_WhenNoExpiredEntries_SkipsCaching()
    {
        var time = new ManualTimeProvider();
        // 容量 2，TTL 30 秒。
        var cache = CreateCache(timeProvider: time, maxCapacity: 2, ttl: TimeSpan.FromSeconds(30));

        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash,
            GroupConversationResult.Success("req-1", "conv-1"));
        cache.TryAdd(1002, DefaultOperation, "req-2", DefaultPayloadHash,
            GroupConversationResult.Success("req-2", "conv-2"));
        Assert.Equal(2, cache.Count);

        // 未推进时间，所有条目未过期。第 3 条应被跳过（容量满且无过期）。
        cache.TryAdd(1003, DefaultOperation, "req-3", DefaultPayloadHash,
            GroupConversationResult.Success("req-3", "conv-3"));

        // 容量不变，新条目未被缓存。
        Assert.Equal(2, cache.Count);
        var lookup = cache.TryGet(1003, DefaultOperation, "req-3", DefaultPayloadHash);
        Assert.True(lookup.IsMiss());
    }

    [Fact]
    public void EvictUser_RemovesAllEntriesForUser()
    {
        var cache = CreateCache();

        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash,
            GroupConversationResult.Success("req-1", "conv-1"));
        cache.TryAdd(1001, DefaultOperation, "req-2", DefaultPayloadHash,
            GroupConversationResult.Success("req-2", "conv-2"));
        cache.TryAdd(1002, DefaultOperation, "req-3", DefaultPayloadHash,
            GroupConversationResult.Success("req-3", "conv-3"));

        cache.EvictUser(1001);

        Assert.True(cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash).IsMiss());
        Assert.True(cache.TryGet(1001, DefaultOperation, "req-2", DefaultPayloadHash).IsMiss());
        // 其他用户的条目不受影响。
        Assert.True(cache.TryGet(1002, DefaultOperation, "req-3", DefaultPayloadHash).IsHit);
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

        cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash); // miss
        cache.TryAdd(1001, DefaultOperation, "req-1", DefaultPayloadHash,
            GroupConversationResult.Success("req-1", "conv-1"));
        cache.TryGet(1001, DefaultOperation, "req-1", DefaultPayloadHash); // hit

        Assert.Equal(1, hits);
        Assert.Equal(1, misses);
    }

    [Fact]
    public void OnLookup_CallbackFires_MissForEmptyRequestId()
    {
        var cache = CreateCache();
        var misses = 0;
        cache.OnLookup = hit => { if (!hit) misses++; };

        cache.TryGet(1001, DefaultOperation, "", DefaultPayloadHash);

        Assert.Equal(1, misses);
    }

    [Fact]
    public void TryGet_DifferentPayloadHash_ReturnsConflict()
    {
        var cache = CreateCache();
        var original = GroupConversationResult.Success("req-1", "conv-1");

        cache.TryAdd(1001, DefaultOperation, "req-1", payloadHash: "111", original);

        // 同一 (UserId, Operation, RequestId) 但不同 PayloadHash → 冲突。
        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", payloadHash: "222");

        Assert.True(lookup.IsConflict);
        Assert.False(lookup.IsHit);
        Assert.Null(lookup.Result);
    }

    [Fact]
    public void TryGet_SamePayloadHash_ReturnsHit()
    {
        var cache = CreateCache();
        var original = GroupConversationResult.Success("req-1", "conv-1");

        cache.TryAdd(1001, DefaultOperation, "req-1", payloadHash: "111", original);

        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", payloadHash: "111");

        Assert.True(lookup.IsHit);
        Assert.NotNull(lookup.Result);
    }

    [Fact]
    public void TryGet_Conflict_DoesNotInvokeOnLookupCallback()
    {
        var cache = CreateCache();
        var original = GroupConversationResult.Success("req-1", "conv-1");
        var callbackInvoked = false;
        cache.OnLookup = _ => callbackInvoked = true;

        cache.TryAdd(1001, DefaultOperation, "req-1", payloadHash: "111", original);
        callbackInvoked = false;

        // 冲突不应触发 hit/miss 回调。
        var lookup = cache.TryGet(1001, DefaultOperation, "req-1", payloadHash: "222");

        Assert.True(lookup.IsConflict);
        Assert.False(callbackInvoked);
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

/// <summary>
/// GroupIdempotencyLookup 扩展方法：测试可读性辅助。
/// </summary>
internal static class GroupIdempotencyLookupExtensions
{
    public static bool IsMiss(this in GroupIdempotencyLookup lookup) =>
        !lookup.IsHit && !lookup.IsConflict;
}
