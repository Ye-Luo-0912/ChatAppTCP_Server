namespace ChatApp.ResumeVerification.Diagnostics;

/// <summary>
/// Resume 故障压力验证汇总报告。
/// </summary>
internal sealed class ResumeVerificationReport
{
    /// <summary>验证开始时间（UTC）。</summary>
    public required DateTimeOffset StartedAtUtc { get; init; }

    /// <summary>验证完成时间（UTC）。</summary>
    public required DateTimeOffset CompletedAtUtc { get; init; }

    /// <summary>验证运行配置快照。</summary>
    public required ResumeVerificationConfiguration Configuration { get; init; }

    /// <summary>各场景结果。</summary>
    public required List<ScenarioResult> Scenarios { get; init; }

    /// <summary>是否全部场景通过。</summary>
    public bool AllPassed { get; init; }
}

/// <summary>
/// 验证运行配置快照，写入报告便于复现。
/// </summary>
internal sealed class ResumeVerificationConfiguration
{
    /// <summary>网关端点列表（HOST:PORT）。</summary>
    public required List<string> GatewayEndpoints { get; init; }

    /// <summary>普通场景使用的用户数。</summary>
    public int UserCount { get; init; }

    /// <summary>reconnect-storm 场景的会话规模。</summary>
    public int StormSize { get; init; }

    /// <summary>Redis 故障注入前的等待秒数（0 表示工具不注入故障）。</summary>
    public int RedisDownDelaySeconds { get; init; }

    /// <summary>Redis 恢复前的等待秒数。</summary>
    public int RedisRecoveryDelaySeconds { get; init; }

    /// <summary>用户 Id 起始值。</summary>
    public long BootstrapUserIdStart { get; init; }

    /// <summary>预热秒数。</summary>
    public int WarmupSeconds { get; init; }
}

/// <summary>
/// 单个场景的执行结果。
/// </summary>
internal sealed class ScenarioResult
{
    /// <summary>场景名称（与 <see cref="IResumeScenario.Name"/> 一致）。</summary>
    public required string Name { get; init; }

    /// <summary>是否通过。</summary>
    public bool Passed { get; init; }

    /// <summary>人类可读的场景摘要。</summary>
    public required string Summary { get; init; }

    /// <summary>采集到的指标样本。</summary>
    public required List<MetricSample> Metrics { get; init; }

    /// <summary>错误明细。</summary>
    public required List<string> Errors { get; init; }

    /// <summary>场景运行时长（秒）。</summary>
    public double DurationSeconds { get; init; }
}

/// <summary>
/// 单个指标样本。
/// </summary>
internal sealed class MetricSample
{
    /// <summary>指标名称。</summary>
    public required string Name { get; init; }

    /// <summary>指标值。</summary>
    public double Value { get; init; }

    /// <summary>采样时间（UTC）。</summary>
    public required DateTimeOffset SampledAtUtc { get; init; }
}
