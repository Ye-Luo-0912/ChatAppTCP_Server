using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// 三-3：冻结用户缓存实现。维护已被管理员冻结的用户 Id 集合，
/// 供认证与 Resume 路径快速拒绝。
/// <para>
/// <b>缓存策略</b>：由 <c>UserLifecycleChanged</c> Realtime 事件驱动更新。
/// 冻结条目带 TTL（默认 10 分钟），过期后下次查询触发后台刷新。
/// </para>
/// <para>
/// <b>Cache Miss 策略</b>：fail-open + 后台刷新。
/// <see cref="IsFrozen"/> 未命中时返回 false，同时 fire-and-forget 调用
/// <see cref="IRealtimeMessageBus.QueryUserLifecycleAsync"/> 刷新缓存。
/// 认证路径权威性由 AccessTokenStore 保证；Resume 路径在缓存预热后秒级拦截。
/// </para>
/// <para>
/// 后台刷新对同一 userId 去重：<see cref="ConcurrentDictionary{TKey,TValue}"/>
/// 跟踪 in-flight 刷新，避免缓存 miss 风暴下对 Server 发起重复查询。
/// </para>
/// </summary>
internal sealed partial class FrozenUserCache : IFrozenUserCache, IDisposable
{
    private readonly IRealtimeMessageBus? _messageBus;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<FrozenUserCache> _logger;
    private readonly TimeSpan _entryTtl;
    private readonly int _maxCapacity;

    // userId → frozenAtMs（用 TimeProvider.GetTimestamp 记录写入时刻）。
    private readonly ConcurrentDictionary<long, long> _frozenUsers = new();

    // in-flight 刷新去重：userId → 占位符。刷新完成后移除。
    private readonly ConcurrentDictionary<long, byte> _refreshInFlight = new();

    private readonly ITimer? _cleanupTimer;
    private int _disposed;

    public FrozenUserCache(
        IRealtimeMessageBus? messageBus,
        TimeProvider timeProvider,
        ILogger<FrozenUserCache> logger,
        TimeSpan? entryTtl = null,
        int maxCapacity = 65_536)
    {
        _messageBus = messageBus;
        _timeProvider = timeProvider;
        _logger = logger;
        _entryTtl = entryTtl ?? TimeSpan.FromMinutes(10);
        _maxCapacity = maxCapacity > 0 ? maxCapacity : 65_536;

        // 每 2 分钟清理一次过期条目。使用 TimeProvider 以支持测试替身。
        _cleanupTimer = timeProvider.CreateTimer(
            static state => ((FrozenUserCache)state!).CleanupExpired(),
            this,
            TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(2));
    }

    public bool IsFrozen(long userId)
    {
        if (userId <= 0)
            return false;

        if (_frozenUsers.TryGetValue(userId, out var frozenAtTimestamp))
        {
            var now = _timeProvider.GetTimestamp();
            var expiry = frozenAtTimestamp
                + (long)(_entryTtl.TotalSeconds * _timeProvider.TimestampFrequency);
            if (now < expiry)
                return true;

            // 过期：移除并触发后台刷新。
            _frozenUsers.TryRemove(userId, out _);
        }

        // Cache miss → fail-open + 后台刷新。
        TriggerBackgroundRefresh(userId);
        return false;
    }

    public void MarkFrozen(long userId, long frozenAtMs)
    {
        if (userId <= 0)
            return;

        // 容量上限保护。
        if (_frozenUsers.Count >= _maxCapacity)
            CleanupExpired();

        _frozenUsers[userId] = _timeProvider.GetTimestamp();
    }

    public void MarkUnfrozen(long userId)
    {
        if (userId <= 0)
            return;
        _frozenUsers.TryRemove(userId, out _);
    }

    /// <summary>
    /// fire-and-forget 后台刷新：查询 Server 获取用户当前生命周期状态。
    /// 对同一 userId 去重，避免 miss 风暴下重复查询。
    /// </summary>
    private void TriggerBackgroundRefresh(long userId)
    {
        if (_messageBus is null)
            return; // 无 bus（测试场景）：跳过刷新，纯 fail-open。

        // 去重：已有 in-flight 刷新则跳过。
        if (!_refreshInFlight.TryAdd(userId, 0))
            return;

        _ = RefreshFromServerAsync(userId);
    }

    private async Task RefreshFromServerAsync(long userId)
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            var response = await _messageBus!
                .QueryUserLifecycleAsync(
                    new UserLifecycleQuery { UserId = userId },
                    cts.Token)
                .ConfigureAwait(false);

            if (response.State == UserLifecycleState.Frozen)
            {
                var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                MarkFrozen(userId, nowMs);
                LogUserFrozenRefreshed(userId);
            }
            // 非 Frozen 响应：不写入缓存（用户未冻结）。
        }
        catch (Exception ex)
        {
            // 刷新失败：不阻断 fail-open 行为，仅记录。
            LogRefreshFailed(ex, userId);
        }
        finally
        {
            _refreshInFlight.TryRemove(userId, out _);
        }
    }

    private void CleanupExpired()
    {
        if (_frozenUsers.IsEmpty)
            return;

        var now = _timeProvider.GetTimestamp();
        var expiryThreshold = now
            - (long)(_entryTtl.TotalSeconds * _timeProvider.TimestampFrequency);

        var removed = 0;
        foreach (var kvp in _frozenUsers)
        {
            if (kvp.Value < expiryThreshold)
            {
                if (_frozenUsers.TryRemove(kvp.Key, out _))
                    removed++;
            }
        }

        if (removed > 0)
            LogCacheCleanup(removed, _frozenUsers.Count);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _cleanupTimer?.Dispose();
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Debug,
        Message = "FrozenUserCache 后台刷新确认用户 {UserId} 已冻结")]
    private partial void LogUserFrozenRefreshed(long userId);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "FrozenUserCache 后台刷新失败 UserId={UserId}")]
    private partial void LogRefreshFailed(Exception exception, long userId);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Debug,
        Message = "FrozenUserCache 清理 {Removed} 个过期条目，剩余 {Remaining}")]
    private partial void LogCacheCleanup(int removed, int remaining);
}
