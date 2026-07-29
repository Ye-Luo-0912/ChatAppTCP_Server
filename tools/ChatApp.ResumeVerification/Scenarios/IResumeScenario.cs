using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 单个 Resume 故障场景。场景按顺序执行，各自负责断言并采集指标。
/// </summary>
internal interface IResumeScenario
{
    /// <summary>场景名称（唯一标识，用于报告）。</summary>
    string Name { get; }

    /// <summary>执行场景并返回结果。</summary>
    Task<ScenarioResult> ExecuteAsync(
        ResumeScenarioContext context,
        CancellationToken cancellationToken);
}

/// <summary>
/// 场景执行上下文，提供网关端点、Redis 连接串、Token 引导工厂与指标采样器。
/// </summary>
internal sealed class ResumeScenarioContext
{
    /// <summary>网关端点列表（至少 1 个）。</summary>
    public required IReadOnlyList<GatewayEndpoint> GatewayEndpoints { get; init; }

    /// <summary>Redis 连接串（用于引导 AccessToken 与指标采样）。</summary>
    public required string RedisConnectionString { get; init; }

    /// <summary>
    /// 为指定用户 Id 创建 <see cref="ResumeTokenBootstrap"/>，写入 AccessToken 到 Redis。
    /// </summary>
    public required Func<long, CancellationToken, Task<ResumeTokenBootstrap>> BootstrapFactory { get; init; }

    /// <summary>Prometheus 指标采样器（可能为 null，当网关未暴露 metrics 时）。</summary>
    public MetricsSampler? MetricsSampler { get; init; }

    /// <summary>时间提供者，便于测试注入。</summary>
    public required TimeProvider TimeProvider { get; init; }

    /// <summary>验证运行选项。</summary>
    public required ResumeVerificationOptions Options { get; init; }
}
