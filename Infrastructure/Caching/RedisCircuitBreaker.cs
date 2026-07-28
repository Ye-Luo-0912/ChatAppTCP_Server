using System.Threading;

namespace ChatApp.TcpGateway.Infrastructure.Caching;

/// <summary>
/// 默认 <see cref="IRedisCircuitBreaker"/> 实现：连续失败计数 + 开路时间戳的状态机。
/// <para>
/// 状态存储在两个字段中（<c>_consecutiveFailures</c> 与 <c>_openUntilTicks</c>），
/// 通过 <see cref="Interlocked"/> 操作保证线程安全，避免锁竞争。
/// </para>
/// <para>
/// 配置：
/// <list type="bullet">
/// <item><c>failureThreshold</c>：Closed → Open 的连续失败阈值（默认 5）。</item>
/// <item><c>openDuration</c>：Open 持续时间，超时后转 HalfOpen（默认 5 秒）。</item>
/// </list>
/// </para>
/// <para>
/// 时间戳统一使用 <see cref="TimeProvider.GetUtcNow"/>.<see cref="DateTimeOffset.Ticks"/>
/// （100ns 单位），与 <see cref="TimeSpan.Ticks"/> 同单位，便于换算与测试。
/// </para>
/// </summary>
public sealed class RedisCircuitBreaker : IRedisCircuitBreaker
{
    private readonly int _failureThreshold;
    private readonly long _openDurationTicks;
    private readonly TimeProvider _timeProvider;

    // 0 = Closed/HalfOpen（无累计失败）；>0 = 自上次成功以来的连续失败数。
    // 当达到 _failureThreshold 时写入 _openUntilTicks。
    private long _consecutiveFailures;
    // 0 = 未开路；>0 = 开路截止时间戳（DateTimeOffset.Ticks 单位）。
    private long _openUntilTicks;

    public RedisCircuitBreaker(
        int failureThreshold = 5,
        TimeSpan? openDuration = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(failureThreshold, 0);
        _failureThreshold = failureThreshold;
        _openDurationTicks = (openDuration ?? TimeSpan.FromSeconds(5)).Ticks;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public bool IsAvailable
    {
        get
        {
            // Open 状态：检查是否到达 HalfOpen 转换时间。
            var openUntil = Volatile.Read(ref _openUntilTicks);
            if (openUntil == 0)
                return true;

            var now = _timeProvider.GetUtcNow().Ticks;
            if (now < openUntil)
                return false; // 仍在 Open 窗口内。

            // 进入 HalfOpen：清零 openUntil，允许试探请求通过。
            // _consecutiveFailures 保持原值——试探失败时立即重新 Open。
            Interlocked.CompareExchange(ref _openUntilTicks, 0, openUntil);
            return true;
        }
    }

    public void RecordSuccess()
    {
        // 重置失败计数；若处于 Open/HalfOpen 也清零开路标记
        // （HalfOpen→Closed 已由 IsAvailable 处理 openUntil 清零，此处幂等）。
        Interlocked.Exchange(ref _consecutiveFailures, 0);
        Volatile.Write(ref _openUntilTicks, 0);
    }

    public void RecordFailure()
    {
        var failures = Interlocked.Increment(ref _consecutiveFailures);
        if (failures >= _failureThreshold)
        {
            // 进入 Open：计算开路截止时间戳（DateTimeOffset.Ticks 单位）。
            var now = _timeProvider.GetUtcNow().Ticks;
            Volatile.Write(ref _openUntilTicks, now + _openDurationTicks);
        }
    }

    // 仅供测试与可观测性：当前连续失败数。
    public int ConsecutiveFailures => (int)Volatile.Read(ref _consecutiveFailures);
    // 仅供测试与可观测性：开路截止时间戳；0 表示未开路。
    public long OpenUntilTicks => Volatile.Read(ref _openUntilTicks);
}
