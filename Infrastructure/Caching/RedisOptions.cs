namespace ChatApp.TcpGateway.Infrastructure.Caching;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    public string ConnectionString { get; set; } = string.Empty;

    public TimeSpan StartupTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// 应用层熔断器连续失败阈值：连续失败达到此值后熔断为 Open 状态。
    /// 设为 0 关闭熔断器（仅依赖底层 StackExchange.Redis 重试）。
    /// 默认 5：避免单次抖动触发熔断，又能快速响应持续性故障。
    /// </summary>
    public int CircuitBreakerFailureThreshold { get; set; } = 5;

    /// <summary>
    /// 熔断器 Open 状态持续时间。超时后转 HalfOpen，允许一次试探请求。
    /// 默认 5 秒：平衡 Redis 故障恢复时间与 Resume 路径快速失败需求。
    /// </summary>
    public TimeSpan CircuitBreakerOpenDuration { get; set; } = TimeSpan.FromSeconds(5);
}
