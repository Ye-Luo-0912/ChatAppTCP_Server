using System.Collections.Concurrent;
using System.Linq;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// 带 TTL 缓存的私聊授权器。复用 Presence 授权查询路径（好友关系 OR 同属一会话）。
/// 缓存 (sender, target) → bool，TTL 默认 30 秒（允许）/ 10 秒（拒绝）。
/// Typing 频率高，每次 NATS 往返成本过大，缓存命中后直接返回。
/// </summary>
/// <remarks>
/// <para>
/// 缓存键使用 <see cref="AuthorizationKey"/> record struct（两个完整 long 字段），
/// 而非将两个 long 打包为单个 long——后者会丢弃高位导致 Snowflake ID 碰撞，
/// 使一对用户的授权结果被错误复用于另一对用户。
/// </para>
/// <para>
/// 缓存有最大容量上限（默认 16384），超限时触发清理；后台 Timer 每 60 秒清理过期条目。
/// 拒绝结果使用更短 TTL，确保关系变更后能较快重新授权。
/// </para>
/// </remarks>
internal sealed partial class CachedDirectConversationAuthorizer
    : IDirectConversationAuthorizer, IDisposable
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<CachedDirectConversationAuthorizer> _logger;
    private readonly TimeSpan _allowTtl;
    private readonly TimeSpan _denyTtl;
    private readonly int _maxCapacity;

    // 缓存键：两个完整 long 字段，无高位截断，无碰撞。
    private readonly ConcurrentDictionary<AuthorizationKey, CacheEntry> _cache = new();

    // 后台清理 Timer：周期扫描移除过期条目，防止无界增长。
    private readonly ITimer? _cleanupTimer;
    private int _disposed;

    public CachedDirectConversationAuthorizer(
        IRealtimeMessageBus messageBus,
        TimeProvider timeProvider,
        ILogger<CachedDirectConversationAuthorizer> logger,
        TimeSpan? cacheTtl = null,
        int maxCapacity = 16_384)
    {
        _messageBus = messageBus;
        _timeProvider = timeProvider;
        _logger = logger;
        // 允许结果缓存较久（默认 30s）；拒绝结果缓存较短（默认 10s），
        // 确保关系从禁止→允许变更后能较快生效。
        _allowTtl = cacheTtl ?? TimeSpan.FromSeconds(30);
        _denyTtl = TimeSpan.FromSeconds(Math.Min(10, _allowTtl.TotalSeconds / 2));
        _maxCapacity = maxCapacity > 0 ? maxCapacity : 16_384;

        // 每 60 秒清理一次过期条目。使用 TimeProvider 以支持测试替身。
        _cleanupTimer = timeProvider.CreateTimer(
            static state => ((CachedDirectConversationAuthorizer)state!).CleanupExpired(),
            this,
            TimeSpan.FromSeconds(60),
            TimeSpan.FromSeconds(60));
    }

    public async ValueTask<bool> AuthorizeAsync(
        long senderUserId,
        long targetUserId,
        CancellationToken cancellationToken)
    {
        if (senderUserId <= 0 || targetUserId <= 0 || senderUserId == targetUserId)
            return false;

        var key = new AuthorizationKey(senderUserId, targetUserId);
        var now = _timeProvider.GetTimestamp();

        // 缓存命中且未过期时直接返回。
        if (_cache.TryGetValue(key, out var entry) && now < entry.ExpiryTimestamp)
        {
            return entry.Allowed;
        }

        // 缓存未命中或已过期，查询 Presence 授权服务。
        bool allowed;
        try
        {
            var query = new PresenceAuthorizeQuery
            {
                WatcherUserId = senderUserId,
                TargetUserIds = [targetUserId]
            };
            var response = await _messageBus.AuthorizePresenceAsync(query, cancellationToken)
                .ConfigureAwait(false);
            // AllowedUserIds 包含 target 说明允许。
            allowed = response.AllowedUserIds.Contains(targetUserId);
        }
        catch (Exception ex)
        {
            // 授权服务不可用时降级为拒绝，避免向未授权目标泄漏 Typing 通知。
            // 不缓存异常路径的结果，确保服务恢复后能立即重试。
            LogAuthorizeFailed(ex, senderUserId, targetUserId);
            return false;
        }

        // 容量上限保护：超限时清理过期条目；仍超限则移除最早的条目。
        // 在写入前检查，避免高频写入下无界增长。
        if (_cache.Count >= _maxCapacity)
        {
            CleanupExpired();
            // 若清理后仍超限，移除任意一个条目（ConcurrentDictionary 迭代顺序不保证，
            // 但移除一个即可为新条目腾出空间）。
            if (_cache.Count >= _maxCapacity)
            {
                foreach (var kvp in _cache)
                {
                    _cache.TryRemove(kvp.Key, out _);
                    break;
                }
            }
        }

        var ttl = allowed ? _allowTtl : _denyTtl;
        var expiryTimestamp = now + (long)(ttl.TotalSeconds * _timeProvider.TimestampFrequency);
        _cache[key] = new CacheEntry(allowed, expiryTimestamp);

        return allowed;
    }

    /// <summary>清理所有过期条目。由后台 Timer 周期调用，也在容量超限时触发。</summary>
    private void CleanupExpired()
    {
        if (_cache.IsEmpty)
            return;

        var now = _timeProvider.GetTimestamp();
        var removed = 0;
        foreach (var kvp in _cache)
        {
            if (now >= kvp.Value.ExpiryTimestamp)
            {
                if (_cache.TryRemove(kvp.Key, out _))
                    removed++;
            }
        }

        if (removed > 0)
            LogCacheCleanup(removed, _cache.Count);
    }

    /// <summary>
    /// 失效指定方向的授权缓存条目。关系变更（拉黑、解除好友）后调用，
    /// 确保缓存窗口内不会继续允许已禁止的 Typing/Presence 通知。
    /// </summary>
    public ValueTask InvalidateAsync(
        long senderUserId,
        long targetUserId,
        CancellationToken cancellationToken)
    {
        if (senderUserId <= 0 || targetUserId <= 0)
            return ValueTask.CompletedTask;

        // 移除指定方向的缓存条目。TryRemove 是原子操作，无需额外锁。
        _cache.TryRemove(new AuthorizationKey(senderUserId, targetUserId), out _);
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _cleanupTimer?.Dispose();
    }

    /// <summary>
    /// 缓存键：两个完整 long 用户 ID，无高位截断，消除碰撞。
    /// </summary>
    private readonly record struct AuthorizationKey(long SenderUserId, long TargetUserId);

    private readonly record struct CacheEntry(bool Allowed, long ExpiryTimestamp);

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Direct conversation authorize failed for {Sender}->{Target}")]
    private partial void LogAuthorizeFailed(Exception exception, long sender, long target);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Authorization cache cleanup removed {Removed} expired entries, {Remaining} remaining")]
    private partial void LogCacheCleanup(int removed, int remaining);
}
