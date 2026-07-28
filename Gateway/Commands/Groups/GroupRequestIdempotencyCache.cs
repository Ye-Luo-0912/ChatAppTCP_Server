using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// 群组命令 RequestId 幂等缓存：在 Gateway 层短时缓存 Realtime 返回的
/// <see cref="GroupConversationResult"/>，避免客户端重试（网络抖动/超时重发）
/// 重复命中 Redis/NATS 往返。
/// <para>
/// 缓存键为 <c>(ActorUserId, RequestId)</c>，TTL 默认 30 秒（覆盖典型客户端重试窗口），
/// 容量上限默认 4096 条（约 ~800 KiB）。容量超限时先回收过期条目，仍超限则跳过缓存。
/// </para>
/// <para>
/// 仅缓存 Realtime 正常返回的结果（含业务失败如 not_owner / member_limit_exceeded）；
/// 异常路径构造的 <c>group_unavailable</c> 不经过此缓存，确保瞬态故障可重试。
/// Realtime 侧的 <c>ActorSessionId</c> 回声跳过是幂等主防线，本缓存为前置快速路径。
/// </para>
/// <para>
/// 线程安全：基于 <see cref="ConcurrentDictionary{TKey, TValue}"/>，读取无锁；
/// 容量回收使用 <see cref="Interlocked"/> CAS 防止并发 sweep。
/// </para>
/// </summary>
public sealed class GroupRequestIdempotencyCache
{
    private readonly ConcurrentDictionary<CacheKey, Entry> _cache;
    private readonly int _maxCapacity;
    private readonly long _ttlTicks;
    private readonly TimeProvider _timeProvider;
    private long _lastSweepTicks;

    private const long SweepIntervalTicks = TimeSpan.TicksPerSecond * 10;

    /// <summary>
    /// 幂等命中/未命中回调，用于 metrics 记录。
    /// 参数：true=命中缓存；false=未命中（将调用 Realtime）。
    /// </summary>
    public Action<bool>? OnLookup { get; set; }

    public GroupRequestIdempotencyCache(
        int maxCapacity = 4096,
        TimeSpan? ttl = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxCapacity, 0);
        _maxCapacity = maxCapacity;
        _ttlTicks = (ttl ?? TimeSpan.FromSeconds(30)).Ticks;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _cache = new ConcurrentDictionary<CacheKey, Entry>(
            concurrencyLevel: Environment.ProcessorCount,
            capacity: maxCapacity);
    }

    /// <summary>
    /// 尝试获取缓存的 Realtime 结果。
    /// </summary>
    /// <returns>命中时返回结果；未命中或已过期返回 null。</returns>
    public GroupConversationResult? TryGet(long userId, string requestId)
    {
        if (string.IsNullOrEmpty(requestId))
        {
            OnLookup?.Invoke(false);
            return null;
        }

        var key = new CacheKey(userId, requestId);
        if (!_cache.TryGetValue(key, out var entry))
        {
            OnLookup?.Invoke(false);
            return null;
        }

        var now = _timeProvider.GetUtcNow().Ticks;
        if (now >= entry.ExpiresAtTicks)
        {
            // 过期：尝试移除（失败说明已被其他线程处理，无所谓）。
            _cache.TryRemove(key, out _);
            OnLookup?.Invoke(false);
            return null;
        }

        OnLookup?.Invoke(true);
        return entry.Result;
    }

    /// <summary>
    /// 缓存 Realtime 返回的结果。容量超限时先回收过期条目。
    /// 不缓存 null 结果。
    /// </summary>
    public void TryAdd(long userId, string requestId, GroupConversationResult result)
    {
        if (string.IsNullOrEmpty(requestId) || result is null)
            return;

        var now = _timeProvider.GetUtcNow().Ticks;
        var key = new CacheKey(userId, requestId);

        // 容量检查：超限时触发 sweep（CAS 防并发 sweep）。
        if (_cache.Count >= _maxCapacity)
        {
            TrySweepExpired(now);
            // sweep 后仍超限：跳过缓存（自然背压，不阻塞调用方）。
            if (_cache.Count >= _maxCapacity)
                return;
        }

        _cache[key] = new Entry(result, now + _ttlTicks);

        // 周期性 sweep：即使未达容量也定期清理过期条目，避免内存滞留。
        TryPeriodicSweep(now);
    }

    /// <summary>
    /// 移除指定用户的全部缓存条目（如用户登出时清理）。
    /// </summary>
    public void EvictUser(long userId)
    {
        // ConcurrentDictionary 不支持按谓词高效批量移除，遍历逐项移除。
        // 用户级清理频率低（登出/被踢），可接受 O(N) 扫描。
        foreach (var key in _cache.Keys)
        {
            if (key.UserId == userId)
                _cache.TryRemove(key, out _);
        }
    }

    /// <summary>当前缓存条目数（诊断/测试用）。</summary>
    public int Count => _cache.Count;

    private void TrySweepExpired(long nowTicks)
    {
        var lastSweep = Interlocked.Read(ref _lastSweepTicks);
        // 防止并发 sweep：10 秒内只 sweep 一次。
        if (nowTicks - lastSweep < SweepIntervalTicks)
            return;
        if (Interlocked.CompareExchange(ref _lastSweepTicks, nowTicks, lastSweep) != lastSweep)
            return;

        foreach (var pair in _cache)
        {
            if (nowTicks >= pair.Value.ExpiresAtTicks)
            {
                _cache.TryRemove(pair.Key, out _);
            }
        }
    }

    private void TryPeriodicSweep(long nowTicks)
    {
        var lastSweep = Interlocked.Read(ref _lastSweepTicks);
        if (nowTicks - lastSweep < SweepIntervalTicks)
            return;
        if (Interlocked.CompareExchange(ref _lastSweepTicks, nowTicks, lastSweep) != lastSweep)
            return;

        foreach (var pair in _cache)
        {
            if (nowTicks >= pair.Value.ExpiresAtTicks)
            {
                _cache.TryRemove(pair.Key, out _);
            }
        }
    }

    private readonly record struct CacheKey(long UserId, string RequestId);

    private readonly record struct Entry(GroupConversationResult Result, long ExpiresAtTicks);
}
