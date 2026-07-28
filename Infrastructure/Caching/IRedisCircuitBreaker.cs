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
/// <item><b>HalfOpen</b>：允许一次试探请求；成功 → Closed，失败 → Open。</item>
/// </list>
/// </para>
/// <para>
/// 仅记录应用层操作结果（操作抛异常或超时即视为失败），不感知底层连接状态——
/// StackExchange.Redis 自身的连接失败事件由 <see cref="RedisConnectionProvider"/> 处理。
/// 调用方在执行 Redis 操作前检查 <see cref="IsAvailable"/>，操作完成后调用
/// <see cref="RecordSuccess"/> 或 <see cref="RecordFailure"/> 更新状态。
/// </para>
/// <para>
/// 线程安全：所有状态更新通过 <see cref="Interlocked"/> 完成。
/// HalfOpen 状态下允许多个并发试探请求，其中任意一个失败即重新 Open，
/// 成功的请求会重置失败计数；这是有意的简化——避免引入单槽信号量影响吞吐。
/// </para>
/// </summary>
public interface IRedisCircuitBreaker
{
    /// <summary>
    /// 当前是否允许调用 Redis。Open 状态返回 false，其他状态返回 true。
    /// 调用方应在 Redis 操作前检查；返回 false 时应快速失败而非发起 Redis 调用。
    /// </summary>
    bool IsAvailable { get; }

    /// <summary>记录一次成功操作。重置失败计数；HalfOpen 状态下转 Closed。</summary>
    void RecordSuccess();

    /// <summary>记录一次失败操作。累加失败计数；达到阈值或 HalfOpen 时转 Open。</summary>
    void RecordFailure();
}
