using ChatApp.Realtime.Abstractions.Push;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>无 Redis 时的内存占位实现（仅用于测试与本地调试）。</summary>
internal sealed class InMemoryPushTokenStore : IPushTokenStore
{
    private readonly Dictionary<long, Dictionary<ulong, PushTokenRecord>> _store = new();
    private readonly Lock _gate = new();

    public ValueTask<int> RegisterAsync(
        long userId,
        ulong deviceIdHash,
        PushPlatform platform,
        string token,
        string? appDeviceLabel,
        CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (!_store.TryGetValue(userId, out var bucket))
            {
                bucket = new Dictionary<ulong, PushTokenRecord>();
                _store[userId] = bucket;
            }

            bucket[deviceIdHash] = new PushTokenRecord
            {
                Token = token,
                Platform = platform,
                DeviceIdHash = deviceIdHash,
                AppDeviceLabel = appDeviceLabel,
                UpdatedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            if (bucket.Count > PushTokenLimits.MaxTokensPerUser)
            {
                var oldest = bucket
                    .OrderBy(static kv => kv.Value.UpdatedAtMs)
                    .First();
                bucket.Remove(oldest.Key);
            }

            return ValueTask.FromResult(bucket.Count);
        }
    }

    public ValueTask<int> UnregisterByDeviceAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (!_store.TryGetValue(userId, out var bucket))
                return ValueTask.FromResult(0);

            bucket.Remove(deviceIdHash);
            if (bucket.Count == 0)
                _store.Remove(userId);

            return ValueTask.FromResult(bucket.Count);
        }
    }

    public ValueTask<int> UnregisterByTokenAsync(
        long userId,
        string token,
        CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (!_store.TryGetValue(userId, out var bucket))
                return ValueTask.FromResult(0);

            var keysToRemove = bucket
                .Where(kv => string.Equals(kv.Value.Token, token, StringComparison.Ordinal))
                .Select(kv => kv.Key)
                .ToList();
            foreach (var key in keysToRemove)
                bucket.Remove(key);

            if (bucket.Count == 0)
                _store.Remove(userId);

            return ValueTask.FromResult(bucket.Count);
        }
    }

    public ValueTask<IReadOnlyList<PushTokenRecord>> ListAsync(
        long userId,
        CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (!_store.TryGetValue(userId, out var bucket))
                return ValueTask.FromResult<IReadOnlyList<PushTokenRecord>>([]);

            return ValueTask.FromResult<IReadOnlyList<PushTokenRecord>>(
                bucket.Values.ToList());
        }
    }
}
