using ChatApp.Realtime.PipelineLoadGenerator.Configuration;
using ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class PipelineLoadGeneratorTests
{
    [Fact]
    public void LatencyHistogramComputesBoundedPercentiles()
    {
        var histogram = new LatencyHistogram();
        for (var milliseconds = 1; milliseconds <= 100; milliseconds++)
            histogram.Record(TimeSpan.FromMilliseconds(milliseconds));

        var snapshot = histogram.Snapshot();

        Assert.Equal(100, snapshot.Count);
        Assert.Equal(50.5, snapshot.AverageMs, precision: 3);
        Assert.Equal(50, snapshot.P50Ms);
        Assert.Equal(95, snapshot.P95Ms);
        Assert.Equal(99, snapshot.P99Ms);
        Assert.Equal(100, snapshot.MaximumMs);
    }

    [Fact]
    public void LatencyHistogramCapsPercentileStorageButKeepsActualMaximum()
    {
        var histogram = new LatencyHistogram();
        histogram.Record(TimeSpan.FromSeconds(90));

        var snapshot = histogram.Snapshot();

        Assert.Equal(60_000, snapshot.P99Ms);
        Assert.Equal(90_000, snapshot.MaximumMs);
    }

    [Fact]
    public void OptionsParseRepeatableBaselineSettings()
    {
        var options = PipelineLoadOptions.Parse(
        [
            "--duration-seconds", "60",
            "--warmup-seconds", "10",
            "--concurrency", "16",
            "--operations-per-second", "80",
            "--payload-bytes", "512",
            "--report-directory", ".artifacts/performance"
        ]);

        Assert.Equal(TimeSpan.FromSeconds(60), options.Duration);
        Assert.Equal(TimeSpan.FromSeconds(10), options.Warmup);
        Assert.Equal(16, options.Concurrency);
        Assert.Equal(80, options.OperationsPerSecond);
        Assert.Equal(512, options.PayloadBytes);
        Assert.Equal(".artifacts/performance", options.ReportDirectory);
    }

    [Fact]
    public void OptionsRejectUnboundedConcurrency()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            PipelineLoadOptions.Parse(
            [
                "--concurrency", "1025"
            ]));
    }
}