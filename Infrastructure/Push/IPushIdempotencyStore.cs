namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// Push 投递幂等存储（Token 级）。
/// <para>
/// 幂等键 = <c>deliveryId + ":" + tokenFingerprint</c>。其中 <c>deliveryId</c> 是本次投递的稳定标识
/// （目标用户 + 消息 Id），tokenFingerprint 是令牌指纹（SHA256 前 8 字节 hex）。
/// </para>
/// <para>
/// 目的：JetStream NAK 重投整条命令时，已成功投递的 token 不应重复推送。
/// 投递前先 <see cref="IsSentAsync"/> 判断，已成功则跳过；成功后 <see cref="MarkSentAsync"/> 记录。
/// TTL 有限（默认 5 分钟），覆盖 JetStream 重投窗口即可。
/// </para>
/// </summary>
public interface IPushIdempotencyStore
{
    /// <summary>
    /// 判断 (deliveryId, tokenFingerprint) 是否已成功投递过。
    /// 返回 true 表示已投递（应跳过，避免重复推送）；false 表示未投递（应发送）。
    /// Redis 故障时 fail-open（返回 false，允许发送），避免幂等检查阻断推送。
    /// </summary>
    ValueTask<bool> IsSentAsync(
        string deliveryId,
        string tokenFingerprint,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 记录 (deliveryId, tokenFingerprint) 已成功投递。
    /// </summary>
    ValueTask MarkSentAsync(
        string deliveryId,
        string tokenFingerprint,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 测试用内存幂等存储。
/// </summary>
internal sealed class InMemoryPushIdempotencyStore : IPushIdempotencyStore
{
    private readonly TimeSpan _ttl;
    private readonly Dictionary<string, DateTime> _sent = new();
    private readonly object _lock = new();

    public InMemoryPushIdempotencyStore(TimeSpan? ttl = null)
    {
        _ttl = ttl ?? TimeSpan.FromMinutes(5);
    }

    public ValueTask<bool> IsSentAsync(
        string deliveryId,
        string tokenFingerprint,
        CancellationToken cancellationToken = default)
    {
        var key = $"{deliveryId}:{tokenFingerprint}";
        var now = DateTime.UtcNow;
        lock (_lock)
        {
            if (_sent.TryGetValue(key, out var sentAt) && now - sentAt <= _ttl)
                return ValueTask.FromResult(true);
            return ValueTask.FromResult(false);
        }
    }

    public ValueTask MarkSentAsync(
        string deliveryId,
        string tokenFingerprint,
        CancellationToken cancellationToken = default)
    {
        var key = $"{deliveryId}:{tokenFingerprint}";
        lock (_lock)
        {
            _sent[key] = DateTime.UtcNow;
            // 简易清理过期条目（仅测试用）。
            var expired = _sent.Where(kv => DateTime.UtcNow - kv.Value > _ttl).ToList();
            foreach (var kv in expired)
                _sent.Remove(kv.Key);
        }
        return ValueTask.CompletedTask;
    }
}