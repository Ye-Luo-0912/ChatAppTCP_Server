using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// Typing 合并/限频/过期协调器。
/// <para>
/// P0-2 重写要点：
/// <list type="bullet">
/// <item>单 <see cref="PeriodicTimer"/> 驱动的分桶时间轮，不再为每个 typing 状态创建独立 <see cref="Task.Delay"/>；</item>
/// <item>限频刷新只更新版本号与到期桶登记，不再丢失过期任务（修正旧实现刷新后无任务负责 typing=false 的确定性 bug）；</item>
/// <item>本机扇出与跨网关 ephemeral 发布经 <see cref="ReadEmissionsAsync"/> 拉取，发射路径有界、可合并（同一 key 仅保留最新状态）、可丢弃（旧状态被覆盖）；</item>
/// <item>全局/每发送方活跃槽位上限，防止恶意会话轮换耗尽内存。</item>
/// </list>
/// Typing 属于“最新状态有效”的易失数据，不应使用无界 fire-and-forget Task。
/// </para>
/// </summary>
internal sealed class TypingFanoutCoordinator
{
    public static readonly TimeSpan DefaultMinInterval = TimeSpan.FromMilliseconds(500);
    public static readonly TimeSpan DefaultTtl = TimeSpan.FromSeconds(4);
    public static readonly TimeSpan DefaultTickInterval = TimeSpan.FromMilliseconds(500);
    public const int DefaultMaxSlots = 10_000;
    public const int DefaultMaxSlotsPerSender = 64;

    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _minInterval;
    private readonly TimeSpan _ttl;
    private readonly TimeSpan _tickInterval;
    private readonly int _maxSlots;
    private readonly int _maxSlotsPerSender;
    private readonly ILogger<TypingFanoutCoordinator> _logger;

    private readonly Lock _gate = new();
    private readonly Dictionary<Key, Slot> _slots = new();
    private readonly List<BucketEntry>[] _buckets;
    private readonly int _bucketCount;
    private long _lastPumpedTick;

    // 发射合并：_pending 保存每个 (sender,conversation) 的最新待发状态。
    // _signal 为单槽 DropWrite 信号：多个生产者合并为一次通知，消费方一次性排空 _pending。
    private readonly Dictionary<Key, TypingEmission> _pending = new();
    private readonly Channel<int> _signal = Channel.CreateBounded<int>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public TypingFanoutCoordinator(
        TimeProvider? timeProvider = null,
        TimeSpan? minInterval = null,
        TimeSpan? ttl = null,
        TimeSpan? tickInterval = null,
        int? maxSlots = null,
        int? maxSlotsPerSender = null,
        ILogger<TypingFanoutCoordinator>? logger = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _minInterval = minInterval ?? DefaultMinInterval;
        _ttl = ttl ?? DefaultTtl;
        _tickInterval = tickInterval ?? DefaultTickInterval;
        _maxSlots = maxSlots ?? DefaultMaxSlots;
        _maxSlotsPerSender = maxSlotsPerSender ?? DefaultMaxSlotsPerSender;
        _logger = logger ?? NullLogger<TypingFanoutCoordinator>.Instance;

        if (_ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl));
        if (_tickInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(tickInterval));

        // 桶数覆盖 TTL + 余量，常规刷新下桶不会被同周期复用。
        _bucketCount = Math.Max(8, (int)(_ttl / _tickInterval) + 2);
        _buckets = new List<BucketEntry>[_bucketCount];
        for (var i = 0; i < _bucketCount; i++)
            _buckets[i] = new List<BucketEntry>();

        _lastPumpedTick = CurrentTick;
    }

    private long CurrentTick => _timeProvider.GetUtcNow().UtcTicks / _tickInterval.Ticks;

    /// <summary>
    /// 接受一次 typing 通知。返回是否产生状态发射（未因限频/超限被丢弃）。
    /// 过期由协调器内部时间轮负责；调用方无需自行调度过期任务。
    /// 状态变更经 <see cref="ReadEmissionsAsync"/> 发射，由调用方执行本机扇出与 ephemeral 发布。
    /// </summary>
    public bool TryAccept(
        long senderUserId,
        long targetUserId,
        string conversationId,
        bool isTyping)
    {
        if (senderUserId <= 0 || targetUserId <= 0 || string.IsNullOrEmpty(conversationId))
            return false;

        var key = new Key(senderUserId, conversationId);
        var now = _timeProvider.GetUtcNow();
        TypingEmission? emission = null;

        lock (_gate)
        {
            if (_slots.TryGetValue(key, out var existing))
            {
                if (isTyping)
                {
                    var withinInterval = now - existing.LastAcceptedAt < _minInterval;
                    if (existing.IsTyping && withinInterval)
                    {
                        // 限频：仅刷新过期时间与版本号，并重新登记到期桶。不发射。
                        // 修正旧实现：刷新 ExpireAt 但不创建新过期任务，导致 typing 永不置 false。
                        var version = existing.Version + 1;
                        var expireAt = now + _ttl;
                        _slots[key] = existing with { Version = version, ExpireAt = expireAt };
                        EnqueueExpiryLocked(key, version, targetUserId, expireAt);
                        return false;
                    }

                    var v = existing.Version + 1;
                    var exp = now + _ttl;
                    _slots[key] = new Slot(true, now, exp, v);
                    EnqueueExpiryLocked(key, v, targetUserId, exp);
                    emission = new TypingEmission(senderUserId, targetUserId, conversationId, true);
                }
                else
                {
                    // 显式停止：移除槽位并发射 false。已登记的到期条目因版本号不匹配自动失效。
                    _slots.Remove(key);
                    emission = new TypingEmission(senderUserId, targetUserId, conversationId, false);
                }
            }
            else
            {
                if (!isTyping)
                    return false; // 无活跃 typing，无需发射 false。

                if (_slots.Count >= _maxSlots)
                {
                    LogSlotsExhausted(_logger, _slots.Count);
                    return false;
                }

                var perSender = CountPerSenderLocked(senderUserId);
                if (perSender >= _maxSlotsPerSender)
                {
                    LogPerSenderSlotsExhausted(_logger, senderUserId, perSender);
                    return false;
                }

                var v = 1u;
                var exp = now + _ttl;
                _slots[key] = new Slot(true, now, exp, v);
                EnqueueExpiryLocked(key, v, targetUserId, exp);
                emission = new TypingEmission(senderUserId, targetUserId, conversationId, true);
            }

            if (emission.HasValue)
                SignalEmissionLocked(emission.GetValueOrDefault());
        }

        return emission.HasValue;
    }

    /// <summary>
    /// 推进过期扫描：到期且版本号仍匹配的槽位被移除并发射 false。
    /// 生产环境由宿主的 PeriodicTimer 循环调用；测试可直接调用以推进逻辑时钟。
    /// </summary>
    public void PumpExpired()
    {
        var now = _timeProvider.GetUtcNow();
        var currentTick = now.UtcTicks / _tickInterval.Ticks;
        if (currentTick <= _lastPumpedTick)
            return;

        List<TypingEmission> emissions = new();

        lock (_gate)
        {
            var gap = currentTick - _lastPumpedTick;
            if (gap >= _bucketCount)
            {
                // 大停顿（超过一圈）：全量扫描所有桶，避免漏过期。
                FullSweepLocked(now, emissions);
            }
            else
            {
                for (var tick = _lastPumpedTick + 1; tick <= currentTick; tick++)
                    SweepBucketLocked(tick, now, emissions);
            }

            _lastPumpedTick = currentTick;

            if (emissions.Count > 0)
                SignalEmissionsLocked(emissions);
        }
    }

    /// <summary>
    /// 读取发射流：同一 (sender,conversation) 仅保留最新状态；有界、可丢弃、可合并。
    /// 取消时（如宿主停机）正常结束枚举。
    /// </summary>
    public async IAsyncEnumerable<TypingEmission> ReadEmissionsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var reader = _signal.Reader;

        while (!cancellationToken.IsCancellationRequested)
        {
            bool shouldExit;
            try
            {
                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                shouldExit = false;
            }
            catch (OperationCanceledException)
            {
                shouldExit = true;
            }
            catch (ChannelClosedException)
            {
                shouldExit = true;
            }

            if (shouldExit)
                yield break;

            List<TypingEmission> batch;
            lock (_gate)
            {
                if (_pending.Count == 0)
                    continue;

                batch = new List<TypingEmission>(_pending.Values);
                _pending.Clear();
            }

            foreach (var emission in batch)
                yield return emission;
        }
    }

    /// <summary>
    /// 同步排空当前待发状态（测试钩子；生产请使用 <see cref="ReadEmissionsAsync"/>）。
    /// </summary>
    internal List<TypingEmission> DrainPending()
    {
        lock (_gate)
        {
            if (_pending.Count == 0)
                return new List<TypingEmission>();

            var batch = new List<TypingEmission>(_pending.Values);
            _pending.Clear();
            return batch;
        }
    }

    private void EnqueueExpiryLocked(Key key, uint version, long targetUserId, DateTimeOffset expireAt)
    {
        var tick = expireAt.UtcTicks / _tickInterval.Ticks;
        var bucketIndex = (int)(tick % _bucketCount);
        _buckets[bucketIndex].Add(new BucketEntry(key, version, targetUserId, expireAt));
    }

    private void SweepBucketLocked(long tick, DateTimeOffset now, List<TypingEmission> emissions)
    {
        var bucket = _buckets[tick % _bucketCount];
        if (bucket.Count == 0)
            return;

        List<BucketEntry>? leftover = null;

        foreach (var entry in bucket)
        {
            if (entry.ExpireAt > now)
            {
                // 未来到期（仅停顿导致桶被复用时出现）：重新挂到正确桶，避免漏过期。
                (leftover ??= new List<BucketEntry>()).Add(entry);
                continue;
            }

            if (!_slots.TryGetValue(entry.Key, out var slot))
                continue; // 已被显式停止移除。
            if (slot.Version != entry.Version)
                continue; // 已被刷新/重建，新到期条目已另登记。
            if (!slot.IsTyping)
                continue;

            _slots.Remove(entry.Key);
            emissions.Add(new TypingEmission(
                entry.Key.SenderUserId,
                entry.TargetUserId,
                entry.Key.ConversationId,
                isTyping: false));
        }

        bucket.Clear();

        if (leftover is not null)
        {
            foreach (var entry in leftover)
            {
                var idx = (int)(entry.ExpireAt.UtcTicks / _tickInterval.Ticks % _bucketCount);
                _buckets[idx].Add(entry);
            }
        }
    }

    private void FullSweepLocked(DateTimeOffset now, List<TypingEmission> emissions)
    {
        for (var i = 0; i < _bucketCount; i++)
            SweepBucketLocked(_lastPumpedTick + 1 + i, now, emissions);
    }

    private void SignalEmissionLocked(in TypingEmission emission)
    {
        _pending[new Key(emission.SenderUserId, emission.ConversationId)] = emission;
        _signal.Writer.TryWrite(1);
    }

    private void SignalEmissionsLocked(List<TypingEmission> emissions)
    {
        foreach (var e in emissions)
            _pending[new Key(e.SenderUserId, e.ConversationId)] = e;
        _signal.Writer.TryWrite(1);
    }

    private int CountPerSenderLocked(long senderUserId)
    {
        var count = 0;
        foreach (var key in _slots.Keys)
            if (key.SenderUserId == senderUserId)
                count++;
        return count;
    }

    private readonly record struct Key(long SenderUserId, string ConversationId);
    private readonly record struct Slot(bool IsTyping, DateTimeOffset LastAcceptedAt, DateTimeOffset ExpireAt, uint Version);
    private readonly record struct BucketEntry(Key Key, uint Version, long TargetUserId, DateTimeOffset ExpireAt);

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Warning,
        Message = "Typing 全局槽位已满 ({Count})，丢弃新 typing")]
    private static partial void LogSlotsExhausted(ILogger logger, int count);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Warning,
        Message = "Typing 单用户槽位已满 Sender={SenderUserId} ({Count})，丢弃")]
    private static partial void LogPerSenderSlotsExhausted(ILogger logger, long senderUserId, int count);
}

/// <summary>Typing 发射：本机扇出与 ephemeral 发布的输入。</summary>
internal readonly record struct TypingEmission(
    long SenderUserId,
    long TargetUserId,
    string ConversationId,
    bool IsTyping);
