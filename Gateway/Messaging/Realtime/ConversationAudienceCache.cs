using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Integration;
using RealtimeGroupConversationCommand =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationCommand;
using RealtimeGroupConversationOperation =
    ChatApp.Realtime.Abstractions.Conversations.GroupConversationOperation;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime;

/// <summary>
/// P1-2：会话受众缓存（ConversationAudienceCache）。
/// <para>
/// 消费 <see cref="RealtimeEvent.AudienceVersion"/>，为会话级广播事件
/// （<see cref="RealtimeEvent.AudienceKind"/> = Conversation、<c>TargetUserIds</c> 为 null）
/// 解析成员用户编号集合。缓存条目携带缓存时的 <see cref="CacheEntry.AudienceVersion"/>：
/// 事件携带的 AudienceVersion 与缓存不一致时视为过期，重新拉取。
/// </para>
/// <para>
/// 高性能设计：
/// <list type="bullet">
///   <item>快速路径无锁：<see cref="ConcurrentDictionary{TKey,TValue}"/> 读 + 不可变 struct 条目，
///       命中且版本匹配时零分配返回成员数组（数组本身只读复用）。</item>
///   <item>慢速路径（未命中 / 过期）经条纹锁（striped SemaphoreSlim）串行化，
///       避免对同一会话的并发拉取风暴（stampede）；条纹数固定为 2 的幂，避免全局锁竞争。</item>
///   <item>条目带 TTL 过期，防止长期陈旧；缓存有界（<see cref="_maxEntries"/>），
///       超出后以约 LRU 的近似策略逐出最旧条目。</item>
/// </list>
/// </para>
/// <para>
/// 拉取失败（NATS 超时 / 熔断）时向上抛出异常，由调用方决定 NAK 重投——
/// 绝不投递给错误的受众集合（fail-closed）。
/// </para>
/// </summary>
internal sealed class ConversationAudienceCache
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _ttl;
    private readonly int _maxEntries;

    // 快速路径：conversationId -> 不可变条目。
    private readonly ConcurrentDictionary<string, CacheEntry> _entries =
        new(StringComparer.Ordinal);

    // 慢速路径：条纹锁，串行化对同一会话的并发拉取。
    private readonly SemaphoreSlim[] _loadGates;
    private readonly int _stripesMask;

    /// <summary>默认缓存条数上限。</summary>
    private const int DefaultMaxEntries = 4096;

    /// <summary>默认 TTL：成员变更事件会主动刷新缓存，TTL 仅作兜底，避免长期陈旧。</summary>
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(5);

    public ConversationAudienceCache(
        IRealtimeMessageBus messageBus,
        TimeProvider? timeProvider = null,
        TimeSpan? ttl = null,
        int? maxEntries = null)
    {
        _messageBus = messageBus;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _ttl = ttl ?? DefaultTtl;
        _maxEntries = maxEntries ?? DefaultMaxEntries;
        _stripesMask = 31; // 32 条条纹
        _loadGates = new SemaphoreSlim[_stripesMask + 1];
        for (var i = 0; i < _loadGates.Length; i++)
            _loadGates[i] = new SemaphoreSlim(1, 1);
    }

    /// <summary>
    /// 取会话成员用户编号集合；按需（未命中 / 过期 / 版本不匹配）从 Realtime 拉取。
    /// </summary>
    /// <param name="conversationId">会话编号。</param>
    /// <param name="expectedAudienceVersion">
    /// 事件携带的 AudienceVersion；null 表示事件未携带版本（仅凭 TTL 兜底过期）。
    /// </param>
    /// <param name="ct">取消令牌。</param>
    /// <returns>活跃成员用户编号（升序）。会话不存在或已解散时返回空数组。</returns>
    /// <exception cref="InvalidOperationException">拉取失败（fail-closed）。</exception>
    public async ValueTask<long[]> GetOrResolveAsync(
        string conversationId,
        long? expectedAudienceVersion,
        CancellationToken ct)
    {
        // 快速路径：无锁读。
        if (TryHit(conversationId, expectedAudienceVersion, out var members))
            return members;

        // 慢速路径：条纹锁串行化拉取，避免同一会话并发风暴。
        var gate = _loadGates[StripeIndex(conversationId)];
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // 获取锁后再次检查（double-checked locking）：可能已被其他调用方填充。
            if (TryHit(conversationId, expectedAudienceVersion, out members))
                return members;

            var loaded = await LoadAsync(conversationId, ct).ConfigureAwait(false);
            _entries[conversationId] = loaded;
            TrimIfNeeded();
            return loaded.MemberUserIds;
        }
        finally
        {
            gate.Release();
        }
    }

    // 命中且（未携带版本 或 版本匹配）时返回成员数组。
    private bool TryHit(
        string conversationId,
        long? expectedAudienceVersion,
        out long[] members)
    {
        if (_entries.TryGetValue(conversationId, out var entry) && !IsExpired(entry))
        {
            if (!expectedAudienceVersion.HasValue
                || entry.AudienceVersion == expectedAudienceVersion.Value)
            {
                members = entry.MemberUserIds;
                return true;
            }
        }
        members = Array.Empty<long>();
        return false;
    }

    private bool IsExpired(in CacheEntry entry) =>
        _timeProvider.GetTimestamp() - entry.LoadedAtTicks > _ttl.Ticks;

    private async Task<CacheEntry> LoadAsync(
        string conversationId,
        CancellationToken ct)
    {
        var result = await _messageBus
            .MutateGroupConversationAsync(
                new RealtimeGroupConversationCommand
                {
                    // 系统内部受众查询：无真实调用者，ActorUserId=0；Realtime 侧对 QueryAudience 跳过注销校验。
                    RequestId = $"aud:{conversationId}",
                    ActorUserId = 0,
                    Operation = RealtimeGroupConversationOperation.QueryAudience,
                    ConversationId = conversationId
                },
                ct)
            .ConfigureAwait(false);

        if (!result.Succeeded)
            throw new InvalidOperationException(
                $"受众查询失败：{result.ErrorCode} {result.ErrorMessage}");

        return new CacheEntry(
            result.AudienceVersion,
            result.AudienceMemberUserIds is { Count: > 0 }
                ? ToArray(result.AudienceMemberUserIds)
                : Array.Empty<long>(),
            _timeProvider.GetTimestamp());
    }

    private static long[] ToArray(IReadOnlyList<long> ids)
    {
        var arr = new long[ids.Count];
        for (var i = 0; i < ids.Count; i++)
            arr[i] = ids[i];
        return arr;
    }

    private int StripeIndex(string conversationId) =>
        unchecked((int)uint.MaxValue & conversationId.GetHashCode()) & _stripesMask;

    // 近似 LRU：缓存超限时随机探测驱逐若干最旧条目（O(1)，避免维护精确 LRU 链的锁与分配）。
    private void TrimIfNeeded()
    {
        if (_entries.Count <= _maxEntries)
            return;
        var now = _timeProvider.GetTimestamp();
        var victims = 0;
        foreach (var pair in _entries)
        {
            if (victims >= 16)
                break;
            if (now - pair.Value.LoadedAtTicks > _ttl.Ticks)
            {
                if (_entries.TryRemove(pair.Key, out _))
                    victims++;
            }
        }
    }

    /// <summary>不可变缓存条目：缓存时的受众版本 + 成员数组 + 加载时间戳。</summary>
    private readonly struct CacheEntry
    {
        public readonly long AudienceVersion;
        public readonly long[] MemberUserIds;
        public readonly long LoadedAtTicks;

        public CacheEntry(long audienceVersion, long[] memberUserIds, long loadedAtTicks)
        {
            AudienceVersion = audienceVersion;
            MemberUserIds = memberUserIds;
            LoadedAtTicks = loadedAtTicks;
        }
    }
}