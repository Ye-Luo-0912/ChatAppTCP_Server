namespace ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;

internal sealed class LatencySnapshot
{
    public long Count { get; init; }
    public double AverageMs { get; init; }
    public double P50Ms { get; init; }
    public double P95Ms { get; init; }
    public double P99Ms { get; init; }
    public double MaximumMs { get; init; }
}
