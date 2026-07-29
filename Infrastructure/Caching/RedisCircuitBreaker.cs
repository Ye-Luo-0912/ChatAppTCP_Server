using System.Threading;

namespace ChatApp.TcpGateway.Infrastructure.Caching;

/// <summary>
/// 默认 <see cref="IRedisCircuitBreaker"/> 实现：Closed → Open → HalfOpen → Closed/Open 的显式状态机。
/// <para>
/// 状态存储在两个字段中：<c>_state</c>（int，0=Closed/1=Open/2=HalfOpenProbeInFlight）与
/// <c>_stateExpiryTicks</c>（long，Open 窗口或 HalfOpen Probe Lease 的截止时间戳），
/// 通过 <see cref="Interlocked"/> 操作保证线程安全，避免锁竞争。
/// </para>
/// <para>
/// HalfOpen 状态下仅允许一个调用者获得 Probe Lease：
/// Open 窗口超时后通过 CAS（Open → HalfOpenProbeInFlight）原子地获取 Lease，
/// 其余并发竞争者返回 false。Probe Lease 有超时保护（<c>probeTimeout</c>），
/// 防止探针调用者异常退出导致永久阻塞。
/// </para>
/// <para>
/// 配置：
/// <list type="bullet">
/// <item><c>failureThreshold</c>：Closed → Open 的连续失败阈值（默认 5）。</item>
/// <item><c>openDuration</c>：Open 持续时间，超时后转 HalfOpen（默认 5 秒）。</item>
/// <item><c>probeTimeout</c>：HalfOpen Probe Lease 超时，超时后允许新探针（默认 10 秒）。</item>
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
    private readonly long _probeTimeoutTicks;
    private readonly TimeProvider _timeProvider;

    // 状态：0 = Closed, 1 = Open, 2 = HalfOpenProbeInFlight
    private int _state;
    // Open 窗口或 HalfOpen Probe Lease 的截止时间戳（DateTimeOffset.Ticks 单位）。
    // Closed 状态下为 0。Open 状态下为 (now + openDuration)。HalfOpen 状态下为 (now + probeTimeout)。
    private long _stateExpiryTicks;
    // 自上次成功以来的连续失败数。
    private long _consecutiveFailures;

    private const int StateClosed = 0;
    private const int StateOpen = 1;
    private const int StateHalfOpenProbeInFlight = 2;

    public RedisCircuitBreaker(
        int failureThreshold = 5,
        TimeSpan? openDuration = null,
        TimeSpan? probeTimeout = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(failureThreshold, 0);
        _failureThreshold = failureThreshold;
        _openDurationTicks = (openDuration ?? TimeSpan.FromSeconds(5)).Ticks;
        // Probe Lease 超时默认为 openDuration 的 2 倍，覆盖 Redis 操作超时（默认 5s）+ 往返。
        _probeTimeoutTicks = (probeTimeout ?? TimeSpan.FromSeconds(10)).Ticks;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public bool IsAvailable
    {
        get
        {
            var state = Volatile.Read(ref _state);
            if (state == StateClosed)
                return true;

            var now = _timeProvider.GetUtcNow().Ticks;
            var expiry = Volatile.Read(ref _stateExpiryTicks);

            // 仍在 Open 窗口或 HalfOpen Probe Lease 内 → 快速失败。
            if (now < expiry)
                return false;

            // Open 窗口超时或 HalfOpen Probe Lease 超时：尝试通过 CAS 获取 Probe Lease。
            // CAS 从当前状态（Open 或 HalfOpenProbeInFlight）→ HalfOpenProbeInFlight。
            if (Interlocked.CompareExchange(ref _state, StateHalfOpenProbeInFlight, state) == state)
            {
                // 成功获取 Probe Lease：设置 Lease 超时。
                Volatile.Write(ref _stateExpiryTicks, now + _probeTimeoutTicks);
                return true;
            }

            // CAS 失败：另一个调用者已获取 Lease（或状态已变）。
            // 重新读取状态判断是否可以放行。
            var currentState = Volatile.Read(ref _state);
            return currentState == StateClosed;
        }
    }

    public CircuitBreakerState State
    {
        get
        {
            var state = Volatile.Read(ref _state);
            return state switch
            {
                StateOpen => CircuitBreakerState.Open,
                StateHalfOpenProbeInFlight => CircuitBreakerState.HalfOpenProbeInFlight,
                _ => CircuitBreakerState.Closed
            };
        }
    }

    public void RecordSuccess()
    {
        Interlocked.Exchange(ref _consecutiveFailures, 0);
        Volatile.Write(ref _stateExpiryTicks, 0);
        // 从任意状态转 Closed（HalfOpen → Closed 是正常路径）。
        Volatile.Write(ref _state, StateClosed);
    }

    public void RecordFailure()
    {
        var state = Volatile.Read(ref _state);

        if (state == StateHalfOpenProbeInFlight)
        {
            // 探针失败：立即重新 Open。
            var now = _timeProvider.GetUtcNow().Ticks;
            Volatile.Write(ref _stateExpiryTicks, now + _openDurationTicks);
            Volatile.Write(ref _state, StateOpen);
            return;
        }

        // Closed 状态：累加失败计数，可能转 Open。
        var failures = Interlocked.Increment(ref _consecutiveFailures);
        if (failures >= _failureThreshold && state == StateClosed)
        {
            var now = _timeProvider.GetUtcNow().Ticks;
            Volatile.Write(ref _stateExpiryTicks, now + _openDurationTicks);
            // CAS：仅 Closed → Open，避免覆盖其他线程已转换的状态。
            Interlocked.CompareExchange(ref _state, StateOpen, StateClosed);
        }
    }

    // 仅供测试与可观测性：当前连续失败数。
    public int ConsecutiveFailures => (int)Volatile.Read(ref _consecutiveFailures);

    // 仅供测试与可观测性：状态截止时间戳；0 表示 Closed（无截止）。
    public long OpenUntilTicks => Volatile.Read(ref _stateExpiryTicks);
}
