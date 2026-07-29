using System.Globalization;

namespace ChatApp.ResumeVerification.Runtime;

/// <summary>
/// 采集网关 Prometheus 指标，聚焦 <c>gateway_resume_*</c> 系列。
/// </summary>
internal sealed class MetricsSampler
{
    private static readonly HttpClient HttpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(3)
    };

    private readonly Uri _metricsUri;

    /// <summary>构造采样器。</summary>
    /// <param name="metricsUrl">网关 metrics 端点（如 <c>http://host:port/metrics</c>）。</param>
    /// <param name="timeProvider">时间提供者，默认 <see cref="TimeProvider.System"/>。</param>
    public MetricsSampler(string metricsUrl, TimeProvider? timeProvider = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(metricsUrl);
        _metricsUri = new Uri(metricsUrl);
        TimeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>时间提供者。</summary>
    public TimeProvider TimeProvider { get; }

    /// <summary>
    /// 采样一次指标，返回指标名到值的映射。
    /// 仅保留 <c>gateway_resume_*</c> 与 <c>gateway_authenticated_sessions</c>。
    /// </summary>
    public async Task<Dictionary<string, double>> SampleAsync(CancellationToken ct)
    {
        var content = await HttpClient.GetStringAsync(_metricsUri, ct)
            .ConfigureAwait(false);
        var metrics = new Dictionary<string, double>(StringComparer.Ordinal);

        foreach (var rawLine in content.Split('\n'))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line[0] == '#')
            {
                continue;
            }

            var separator = line.LastIndexOf(' ');
            if (separator <= 0 || separator == line.Length - 1)
            {
                continue;
            }

            var name = line[..separator];
            if (!IsResumeRelevant(name))
            {
                continue;
            }

            if (double.TryParse(
                    line[(separator + 1)..],
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture,
                    out var value) &&
                double.IsFinite(value))
            {
                metrics[name] = value;
            }
        }

        return metrics;
    }

    /// <summary>采样并返回当前时间戳的 <see cref="MetricSample"/> 列表。</summary>
    public async Task<List<Diagnostics.MetricSample>> SampleAsMetricSamplesAsync(CancellationToken ct)
    {
        var now = TimeProvider.GetUtcNow();
        var raw = await SampleAsync(ct).ConfigureAwait(false);
        var samples = new List<Diagnostics.MetricSample>(raw.Count);
        foreach (var (name, value) in raw)
        {
            samples.Add(new Diagnostics.MetricSample
            {
                Name = name,
                Value = value,
                SampledAtUtc = now
            });
        }
        return samples;
    }

    private static bool IsResumeRelevant(string name) =>
        name.StartsWith("gateway_resume_", StringComparison.Ordinal) ||
        name.StartsWith("gateway_authenticated_sessions", StringComparison.Ordinal);
}
