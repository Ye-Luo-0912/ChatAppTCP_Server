using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景辅助方法，统一构造 <see cref="ScenarioResult"/> 与计时。
/// </summary>
internal static class ScenarioHelpers
{
    /// <summary>构造通过的结果。</summary>
    public static ScenarioResult Pass(
        string name,
        Stopwatch startedAt,
        string summary,
        List<MetricSample>? metrics = null,
        List<string>? errors = null) =>
        Build(name, passed: true, startedAt, summary, metrics, errors);

    /// <summary>构造失败的结果。</summary>
    public static ScenarioResult Fail(
        string name,
        Stopwatch startedAt,
        string summary,
        List<MetricSample>? metrics = null,
        List<string>? errors = null) =>
        Build(name, passed: false, startedAt, summary, metrics, errors);

    /// <summary>构造跳过的结果（视为通过，但摘要注明跳过原因）。</summary>
    public static ScenarioResult Skip(
        string name,
        Stopwatch startedAt,
        string reason,
        List<MetricSample>? metrics = null) =>
        Build(name, passed: true, startedAt, $"SKIPPED: {reason}", metrics, errors: null);

    private static ScenarioResult Build(
        string name,
        bool passed,
        Stopwatch startedAt,
        string summary,
        List<MetricSample>? metrics,
        List<string>? errors) =>
        new()
        {
            Name = name,
            Passed = passed,
            Summary = summary,
            Metrics = metrics ?? new List<MetricSample>(),
            Errors = errors ?? new List<string>(),
            DurationSeconds = startedAt.Elapsed.TotalSeconds
        };

    /// <summary>采集指标样本（若采样器可用）。</summary>
    public static async Task<List<MetricSample>> SampleMetricsAsync(
        ResumeScenarioContext context,
        CancellationToken ct)
    {
        if (context.MetricsSampler is null)
        {
            return new List<MetricSample>();
        }

        try
        {
            return await context.MetricsSampler
                .SampleAsMetricSamplesAsync(ct)
                .ConfigureAwait(false);
        }
        catch
        {
            return new List<MetricSample>();
        }
    }
}
