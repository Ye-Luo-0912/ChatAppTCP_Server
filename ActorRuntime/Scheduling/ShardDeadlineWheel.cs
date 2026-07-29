using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ChatApp.ActorRuntime.Abstractions;

namespace ChatApp.ActorRuntime.Scheduling;

/// <summary>
/// Shard 单线程时间轮。桶通过双 List 交换原地排空，触发路径不创建快照集合。
/// <para>
/// 条目携带 <see cref="ActivationId"/> 与 Deadline Epoch：触发时由 Shard 校验
/// Actor 当前激活纪元与 Deadline 代际，不匹配即为惰性取消的过期条目（直接丢弃）。
/// 时间轮不提供显式 Remove——取消通过 Epoch bump 实现，条目在触发时刻自行离开轮。
/// </para>
/// </summary>
internal sealed class ShardDeadlineWheel<TKey, TState, TMessage>
    where TKey : notnull
    where TState : struct
    where TMessage : struct
{
    private const int BucketCount = 256;
    private const int MaxCatchUpTicksPerPump = 4096;

    private readonly TimeProvider _timeProvider;
    private readonly long _tickIntervalTimestamp;
    private readonly int _bucketMask;
    private readonly Bucket[] _buckets;
    private readonly IDeadlineCallback<TKey, TMessage> _callback;
    private long _lastTickTimestamp;
    private long _currentTickIndex;
    private int _pendingCount;

    public ShardDeadlineWheel(
        TimeProvider timeProvider,
        TimeSpan tickInterval,
        IDeadlineCallback<TKey, TMessage> callback)
    {
        _timeProvider = timeProvider;
        _tickIntervalTimestamp =
            (long)(tickInterval.TotalSeconds * timeProvider.TimestampFrequency);
        if (_tickIntervalTimestamp <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(tickInterval),
                "tickInterval is too small for the TimeProvider frequency.");
        }

        _bucketMask = BucketCount - 1;
        _buckets = new Bucket[BucketCount];
        for (var i = 0; i < BucketCount; i++)
            _buckets[i] = new Bucket();

        _callback = callback ?? throw new ArgumentNullException(nameof(callback));
        _lastTickTimestamp = timeProvider.GetTimestamp();
    }

    public int PendingCount => Volatile.Read(ref _pendingCount);

    /// <summary>
    /// 调度一条 Deadline，返回条目所在桶索引（用于 O(1) 取消）。
    /// </summary>
    public int Schedule(
        TimeSpan delay,
        ActivationId activation,
        uint deadlineEpoch,
        in TKey key,
        in TMessage message)
    {
        if (delay <= TimeSpan.Zero)
            return -1;

        var delayTimestamps =
            (long)(delay.TotalSeconds * _timeProvider.TimestampFrequency);
        var ticksToFire =
            Math.Max(1L, (delayTimestamps + _tickIntervalTimestamp - 1) /
                             _tickIntervalTimestamp);

        // exact N×BucketCount 在第 N 圈到达当前桶时触发，不能多等一整圈。
        var rounds = (int)((ticksToFire - 1) / BucketCount);
        var bucketOffset = (int)(ticksToFire % BucketCount);
        var targetIndex =
            (int)(_currentTickIndex + bucketOffset) & _bucketMask;

        _buckets[targetIndex].Add(new TimerEntry
        {
            Rounds = rounds,
            Activation = activation,
            DeadlineEpoch = deadlineEpoch,
            Key = key,
            Message = message
        });
        _pendingCount++;
        return targetIndex;
    }

    /// <summary>
    /// 标记指定桶中匹配 (activation, deadlineEpoch) 的条目为已取消。
    /// 条目在桶排空时被跳过，不回调 OnExpired。
    /// 仅扫描单个桶，O(bucket_size)——桶通常很小（初始容量 8）。
    /// </summary>
    public void TryCancelScheduled(int bucketIndex, ActivationId activation, uint deadlineEpoch)
    {
        if ((uint)bucketIndex >= (uint)BucketCount)
            return;
        _buckets[bucketIndex].TryCancel(activation, deadlineEpoch);
    }

    public void PumpExpired()
    {
        var now = _timeProvider.GetTimestamp();
        var elapsed = now - _lastTickTimestamp;
        var ticksToAdvance = elapsed / _tickIntervalTimestamp;
        if (ticksToAdvance <= 0)
            return;

        var advancing = (int)Math.Min(
            ticksToAdvance,
            MaxCatchUpTicksPerPump);
        for (var i = 0; i < advancing; i++)
        {
            _currentTickIndex =
                (_currentTickIndex + 1) & _bucketMask;
            ProcessBucket(_buckets[_currentTickIndex]);
        }

        // 仅扣除真正推进的时间，保留长 GC pause 后尚未追赶的 tick。
        _lastTickTimestamp += advancing * _tickIntervalTimestamp;
    }

    public void Stop()
    {
        for (var i = 0; i < _buckets.Length; i++)
        {
            var entries = _buckets[i].BeginDrain();
            for (var j = 0; j < entries.Count; j++)
            {
                var message = entries[j].Message;
                _callback.DropScheduled(in message);
            }

            _pendingCount -= entries.Count;
            Bucket.EndDrain(entries);
        }

        _pendingCount = 0;
    }

    private void ProcessBucket(Bucket bucket)
    {
        if (bucket.Count == 0)
            return;

        var entries = bucket.BeginDrain();
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];

            // 已取消的条目直接丢弃，不回调 OnExpired。
            if (entry.Cancelled)
            {
                _pendingCount--;
                continue;
            }

            if (entry.Rounds > 0)
            {
                entry.Rounds--;
                bucket.Add(entry);
                continue;
            }

            _pendingCount--;
            _callback.OnExpired(
                in entry.Key,
                entry.Activation,
                entry.DeadlineEpoch,
                in entry.Message);
        }

        Bucket.EndDrain(entries);
    }

    private struct TimerEntry
    {
        public int Rounds;
        public ActivationId Activation;
        public uint DeadlineEpoch;
        public TKey Key;
        public TMessage Message;
#pragma warning disable CS0649 // 通过 CollectionsMarshal.AsSpan ref 赋值
        public bool Cancelled;
#pragma warning restore CS0649
    }

    private sealed class Bucket
    {
        private List<TimerEntry> _pending = new(8);
        private List<TimerEntry> _draining = new(8);

        public int Count => _pending.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(TimerEntry entry) => _pending.Add(entry);

        /// <summary>
        /// 标记 _pending 列表中匹配 (activation, deadlineEpoch) 的条目为已取消。
        /// 不从 List 移除（避免 RemoveAt 的 O(n) 移位），仅在排空时跳过。
        /// </summary>
        public void TryCancel(ActivationId activation, uint deadlineEpoch)
        {
            var list = _pending;
            for (var i = 0; i < list.Count; i++)
            {
                ref var entry = ref CollectionsMarshal.AsSpan(list)[i];
                if (!entry.Cancelled &&
                    entry.Activation == activation &&
                    entry.DeadlineEpoch == deadlineEpoch)
                {
                    entry.Cancelled = true;
                    return;
                }
            }
        }

        public List<TimerEntry> BeginDrain()
        {
            (_pending, _draining) = (_draining, _pending);
            _pending.Clear();
            return _draining;
        }

        public static void EndDrain(List<TimerEntry> entries)
        {
            entries.Clear();
        }
    }
}

internal interface IDeadlineCallback<TKey, TMessage>
    where TKey : notnull
    where TMessage : struct
{
    /// <summary>
    /// Deadline 到期（在 Shard Consumer 线程上调用）。实现方校验 Activation/Epoch
    /// 并直接投递到 Actor 控制通道；过期条目丢弃。
    /// </summary>
    void OnExpired(
        in TKey key,
        ActivationId activation,
        uint deadlineEpoch,
        in TMessage message);

    void DropScheduled(in TMessage message);
}
