namespace ChatApp.TcpGateway.Infrastructure.Caching;

/// <summary>
/// Redis 应用层熔断器：基于连续操作失败次数的状态机，避免 Redis 故障期间
/// 持续串行触发超时造成跨 Gateway 重连风暴。
/// <para>
/// 三状态：
/// <list type="bullet">
/// <item><b>Closed</b>：正常放行；连续失败数达到 <c>failureThreshold</c> 后熔断为 Open。</item>
/// <item><b>Open</b>：所有请求快速失败（<see cref="IsAvailable"/> = false）；
/// 经过 <c>openDuration</c> 后转 HalfOpen。</item>
/// <item><b>HalfOpen</b>：仅允许一次试探请求（Probe Lease）；成功 → Closed，失败 → Open。
/// 其他并发调用者在探针完成前继续快速失败，避免恢复窗口制造并发探测风暴。</item>
/// </list>
/// </para>
/// <para>
/// 仅记录应用层操作结果（操作抛异常或超时即视为失败），不感知底层连接状态——
/// StackExchange.Redis 自身的连接失败事件由 <see cref="RedisConnectionProvider"/> 处理。
/// 调用方在执行 Redis 操作前检查 <see cref="IsAvailable"/>，操作完成后调用
/// <see cref="RecordSuccess"/> 或 <see cref="RecordFailure"/> 更新状态。
/// </para>
/// <para>
/// 线程安全：所有状态更新通过 <see cref="Interlocked"/> CAS 完成，无锁。
/// <see cref="IsAvailable"/> 会通过 CAS 原子地获取 Probe Lease——只有一个调用者
/// 能从 Open → HalfOpen 转换中获得 Lease，其余竞争者返回 false。
/// </para>
/// <para>
/// <see cref="State"/> 属性提供非抢占式状态查询，不获取 Probe Lease。
/// 用于辅助依赖（如设备租约查询）在主操作已获取 Lease 后检查是否可安全发起 Redis 调用。
/// </para>
/// </summary>
public interface IRedisCircuitBreaker
{
    /// <summary>
    /// 当前是否允许调用 Redis。
    /// <para>
    /// Closed 状态返回 true。Open 状态在开路窗口内返回 false。
    /// 开路窗口超时后，通过 CAS 原子地将状态从 Open → HalfOpenProbeInFlight；
    /// 竞争成功者返回 true（获得 Probe Lease），竞争失败者返回 false。
    /// </para>
    /// <para>
    /// HalfOpenProbeInFlight 状态返回 false（探针已在进行中），
    /// 除非探针 Lease 已超时（由 <c>probeTimeout</c> 控制），此时允许新调用者获取新 Lease。
    /// </para>
    /// <para>
    /// 调用方应在 Redis 操作前检查；返回 false 时应快速失败而非发起 Redis 调用。
    /// 获得 Lease 的调用方必须在操作完成后调用 <see cref="RecordSuccess"/> 或 <see cref="RecordFailure"/>。
    /// </para>
    /// </summary>
    bool IsAvailable { get; }

    /// <summary>
    /// 当前熔断器状态（非抢占式查询，不获取 Probe Lease）。
    /// 用于辅助依赖在不触发探针获取的情况下检查是否可安全发起 Redis 调用。
    /// </summary>
    CircuitBreakerState State { get; }

    /// <summary>记录一次成功操作。重置失败计数；HalfOpen 状态下转 Closed。</summary>
    void RecordSuccess();

    /// <summary>记录一次失败操作。累加失败计数；达到阈值或 HalfOpen 时转 Open。</summary>
    void RecordFailure();
}

/// <summary>
/// 熔断器状态。
/// </summary>
public enum CircuitBreakerState
{
    /// <summary>正常放行。连续失败数达到阈值后转 Open。</summary>
    Closed = 0,

    /// <summary>开路。所有请求快速失败，直到 openDuration 超时后尝试转 HalfOpen。</summary>
    Open = 1,

    /// <summary>半开，探针已发出。仅探针调用者可发起 Redis 调用，其余快速失败。</summary>
    HalfOpenProbeInFlight = 2
}
