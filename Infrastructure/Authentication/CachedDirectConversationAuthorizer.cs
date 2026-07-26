using System.Collections.Concurrent;
using System.Linq;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// 带 TTL 缓存的私聊授权器。复用 Presence 授权查询路径（好友关系 OR 同属一会话）。
/// 缓存 (sender, target) → bool，TTL 默认 30 秒，匹配关系变更后 Presence 缓存失效时间。
/// Typing 频率高，每次 NATS 往返成本过大，缓存命中后直接返回。
/// </summary>
internal sealed partial class CachedDirectConversationAuthorizer : IDirectConversationAuthorizer
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<CachedDirectConversationAuthorizer> _logger;
    private readonly TimeSpan _cacheTtl;

    // 缓存条目：value=true(允许)/false(拒绝)，expiryTicks=过期时间戳。
    private readonly ConcurrentDictionary<long, CacheEntry> _cache = new();

    public CachedDirectConversationAuthorizer(
        IRealtimeMessageBus messageBus,
        TimeProvider timeProvider,
        ILogger<CachedDirectConversationAuthorizer> logger,
        TimeSpan? cacheTtl = null)
    {
        _messageBus = messageBus;
        _timeProvider = timeProvider;
        _logger = logger;
        _cacheTtl = cacheTtl ?? TimeSpan.FromSeconds(30);
    }

    public async ValueTask<bool> AuthorizeAsync(
        long senderUserId,
        long targetUserId,
        CancellationToken cancellationToken)
    {
        if (senderUserId <= 0 || targetUserId <= 0 || senderUserId == targetUserId)
            return false;

        var key = PackKey(senderUserId, targetUserId);
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
            LogAuthorizeFailed(ex, senderUserId, targetUserId);
            return false;
        }

        var expiryTimestamp = now + (long)(_cacheTtl.TotalSeconds * _timeProvider.TimestampFrequency);
        _cache[key] = new CacheEntry(allowed, expiryTimestamp);

        return allowed;
    }

    private static long PackKey(long sender, long target)
    {
        // 确定性打包：sender 在高位，target 在低位。
        return (sender << 32) | (target & 0xFFFFFFFFL);
    }

    private readonly record struct CacheEntry(bool Allowed, long ExpiryTimestamp);

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Direct conversation authorize failed for {Sender}->{Target}")]
    private partial void LogAuthorizeFailed(Exception exception, long sender, long target);
}
