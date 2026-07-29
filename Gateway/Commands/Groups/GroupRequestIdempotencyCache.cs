using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// 群组命令 RequestId 幂等缓存：在 Gateway 层短时缓存 Realtime 返回的
/// <see cref="GroupConversationResult"/>，避免客户端重试（网络抖动/超时重发）
/// 重复命中 Redis/NATS 往返。
/// <para>
/// 缓存键为 <c>(ActorUserId, Operation, RequestId)</c>，并存储 <c>PayloadHash</c>
/// 用于检测同一 RequestId 对应不同操作指纹的冲突。加入 Operation 防止客户端错误复用
/// 同一 RequestId 跨不同操作（如先 AddMembers 再 RemoveMember）时误命中前者结果。
/// TTL 默认 30 秒（覆盖典型客户端重试窗口），容量上限默认 4096 条（约 ~800 KiB）。
/// 容量超限时先回收过期条目，仍超限则跳过缓存。
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
    /// 冲突（同一 RequestId 不同指纹）不计入命中或未命中——由调用方单独处理。
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
    /// <param name="operation">群组操作类型（Create/AddMembers/RemoveMember/...）。
    /// 加入缓存键防止同一 RequestId 跨操作误命中。</param>
    /// <param name="payloadHash">命令负载指纹（不含 RequestId/ActorUserId/ActorSessionId）。
    /// 用于检测同一 (UserId, Operation, RequestId) 但不同负载的冲突。</param>
    /// <returns>
    /// <see cref="GroupIdempotencyLookup.Hit"/> = 命中缓存；
    /// <see cref="GroupIdempotencyLookup.Miss"/> = 未命中或已过期；
    /// <see cref="GroupIdempotencyLookup.Conflict"/> = 同一 RequestId 但负载指纹不匹配。
    /// </returns>
    public GroupIdempotencyLookup TryGet(
        long userId,
        int operation,
        string requestId,
        int payloadHash)
    {
        if (string.IsNullOrEmpty(requestId))
        {
            OnLookup?.Invoke(false);
            return GroupIdempotencyLookup.Miss;
        }

        var key = new CacheKey(userId, operation, requestId);
        if (!_cache.TryGetValue(key, out var entry))
        {
            OnLookup?.Invoke(false);
            return GroupIdempotencyLookup.Miss;
        }

        var now = _timeProvider.GetUtcNow().Ticks;
        if (now >= entry.ExpiresAtTicks)
        {
            // 过期：尝试移除（失败说明已被其他线程处理，无所谓）。
            _cache.TryRemove(key, out _);
            OnLookup?.Invoke(false);
            return GroupIdempotencyLookup.Miss;
        }

        // 负载指纹不匹配：同一 RequestId 但不同操作参数，返回冲突。
        if (entry.PayloadHash != payloadHash)
            return GroupIdempotencyLookup.Conflict;

        OnLookup?.Invoke(true);
        return GroupIdempotencyLookup.Hit(entry.Result);
    }

    /// <summary>
    /// 缓存 Realtime 返回的结果。容量超限时先回收过期条目。
    /// 不缓存 null 结果。
    /// </summary>
    public void TryAdd(
        long userId,
        int operation,
        string requestId,
        int payloadHash,
        GroupConversationResult result)
    {
        if (string.IsNullOrEmpty(requestId) || result is null)
            return;

        var now = _timeProvider.GetUtcNow().Ticks;
        var key = new CacheKey(userId, operation, requestId);

        // 容量检查：超限时触发 sweep（CAS 防并发 sweep）。
        if (_cache.Count >= _maxCapacity)
        {
            TrySweepExpired(now);
            // sweep 后仍超限：跳过缓存（自然背压，不阻塞调用方）。
            if (_cache.Count >= _maxCapacity)
                return;
        }

        _cache[key] = new Entry(result, now + _ttlTicks, payloadHash);

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

    private readonly record struct CacheKey(long UserId, int Operation, string RequestId);

    private readonly record struct Entry(
        GroupConversationResult Result,
        long ExpiresAtTicks,
        int PayloadHash);
}

/// <summary>
/// 幂等缓存查找结果：区分命中、未命中与冲突。
/// </summary>
public readonly record struct GroupIdempotencyLookup
{
    /// <summary>缓存的结果；未命中或冲突时为 null。</summary>
    public GroupConversationResult? Result { get; }

    /// <summary>是否为冲突（同一 RequestId 但负载指纹不匹配）。</summary>
    public bool IsConflict { get; }

    private GroupIdempotencyLookup(GroupConversationResult? result, bool isConflict)
    {
        Result = result;
        IsConflict = isConflict;
    }

    /// <summary>未命中（缓存中不存在或已过期）。</summary>
    public static GroupIdempotencyLookup Miss => default;

    /// <summary>冲突（同一 RequestId 但负载指纹不匹配）。</summary>
    public static GroupIdempotencyLookup Conflict => new(null, isConflict: true);

    /// <summary>命中缓存。</summary>
    public static GroupIdempotencyLookup Hit(GroupConversationResult result) =>
        new(result, isConflict: false);

    /// <summary>是否命中缓存（有缓存结果且无冲突）。</summary>
    public bool IsHit => !IsConflict && Result is not null;
}
