using System.Collections.Concurrent;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// Typing 合并与限频：按 (sender, conversation) 合并；最短间隔限流；自动在 TTL 后过期。
/// 本机扇出经 UserSessionRegistry；跨 Gateway 由调用方经 NATS Core ephemeral 发布（不进 Outbox）。
/// </summary>
internal sealed class TypingFanoutCoordinator
{
    public static readonly TimeSpan DefaultMinInterval = TimeSpan.FromMilliseconds(500);
    public static readonly TimeSpan DefaultTtl = TimeSpan.FromSeconds(4);

    private readonly ConcurrentDictionary<(long Sender, string ConversationId), TypingSlot> _slots = new();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _minInterval;
    private readonly TimeSpan _ttl;

    public TypingFanoutCoordinator(
        TimeProvider? timeProvider = null,
        TimeSpan? minInterval = null,
        TimeSpan? ttl = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _minInterval = minInterval ?? DefaultMinInterval;
        _ttl = ttl ?? DefaultTtl;
    }

    /// <summary>
    /// 返回是否应立即扇出；若为 typing=true，输出自动过期时刻。
    /// </summary>
    public bool TryAccept(
        long senderUserId,
        string conversationId,
        bool isTyping,
        out DateTimeOffset? expireAt)
    {
        expireAt = null;
        var key = (senderUserId, conversationId);
        var now = _timeProvider.GetUtcNow();

        while (true)
        {
            if (_slots.TryGetValue(key, out var existing))
            {
                if (isTyping)
                {
                    if (now - existing.LastAcceptedAt < _minInterval && existing.IsTyping)
                    {
                        var refreshed = existing with { ExpireAt = now + _ttl };
                        if (_slots.TryUpdate(key, refreshed, existing))
                        {
                            expireAt = refreshed.ExpireAt;
                            return false;
                        }

                        continue;
                    }

                    var next = new TypingSlot(true, now, now + _ttl);
                    if (_slots.TryUpdate(key, next, existing))
                    {
                        expireAt = next.ExpireAt;
                        return true;
                    }

                    continue;
                }

                if (_slots.TryRemove(key, out _))
                    return true;
                if (!_slots.ContainsKey(key))
                    return true;
                continue;
            }

            if (!isTyping)
                return false;

            var created = new TypingSlot(true, now, now + _ttl);
            if (!_slots.TryAdd(key, created)) continue;
            expireAt = created.ExpireAt;
            return true;
        }
    }

    public bool TryTakeExpired(long senderUserId, string conversationId, DateTimeOffset expectedExpireAt)
    {
        var key = (senderUserId, conversationId);
        if (!_slots.TryGetValue(key, out var slot))
            return false;
        if (slot.ExpireAt != expectedExpireAt || !slot.IsTyping)
            return false;
        return _slots.TryRemove(key, out var removed)
               && removed.ExpireAt == expectedExpireAt;
    }

    private readonly record struct TypingSlot(bool IsTyping, DateTimeOffset LastAcceptedAt, DateTimeOffset ExpireAt);
}
