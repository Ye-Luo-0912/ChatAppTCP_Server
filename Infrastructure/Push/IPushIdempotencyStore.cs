namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 主线一5：Push 投递幂等存储。防止 JetStream NAK 重投导致重复推送。
/// <para>
/// 幂等键 = (TargetUserId, MessageId)。仅当 MessageId 非空时检查。
/// TTL 有限（默认 5 分钟），覆盖 JetStream 重投窗口即可。
/// </para>
/// </summary>
public interface IPushIdempotencyStore
{
    /// <summary>
    /// 尝试标记幂等键为已处理。返回 true 表示首次标记（应执行投递）；
    /// false 表示已处理过（应 ACK 跳过，避免重复推送）。
    /// </summary>
    ValueTask<bool> TryMarkProcessedAsync(long targetUserId, string messageId, CancellationToken cancellationToken = default);
}

/// <summary>
/// 测试用内存幂等存储。生产环境使用 Redis 实现。
/// </summary>
internal sealed class InMemoryPushIdempotencyStore : IPushIdempotencyStore
{
    private readonly TimeSpan _ttl;
    private readonly Dictionary<string, DateTime> _processed = new();
    private readonly object _lock = new();

    public InMemoryPushIdempotencyStore(TimeSpan? ttl = null)
    {
        _ttl = ttl ?? TimeSpan.FromMinutes(5);
    }

    public ValueTask<bool> TryMarkProcessedAsync(
        long targetUserId,
        string messageId,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(messageId))
            return ValueTask.FromResult(true); // 无 MessageId 时不做幂等，允许投递。

        var key = $"{targetUserId}:{messageId}";
        var now = DateTime.UtcNow;
        lock (_lock)
        {
            // 清理过期条目（简易扫描，仅测试用）。
            var expired = _processed.Where(kv => now - kv.Value > _ttl).ToList();
            foreach (var kv in expired)
                _processed.Remove(kv.Key);

            if (_processed.TryGetValue(key, out var processedAt) && now - processedAt <= _ttl)
                return ValueTask.FromResult(false); // 已处理。

            _processed[key] = now;
            return ValueTask.FromResult(true);
        }
    }
}
