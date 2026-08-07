using System.Diagnostics;
using ChatApp.Performance.Orchestrator.Diagnostics;
using ChatApp.Performance.Orchestrator.Runtime;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class ResourceSamplingCoverageTests
{
    [Fact]
    public void CoverageUsesOnlyMeasurementWindowAndIsCappedAtOneHundredPercent()
    {
        var started = Stopwatch.GetTimestamp();
        var completedAfterDrain = AddSeconds(started, 14);
        var processTimelines = new[]
        {
            new ProcessTimeline(
                "gateway-1",
                1,
                new[]
                {
                    Sample(AddSeconds(started, -1)),
                    Sample(AddSeconds(started, 1)),
                    Sample(AddSeconds(started, 3)),
                    Sample(AddSeconds(started, 5)),
                    Sample(AddSeconds(started, 7)),
                    Sample(AddSeconds(started, 9)),
                    Sample(AddSeconds(started, 11))
                })
        };
        var dockerTimelines = new[]
        {
            new DockerTimeline(
                "nats",
                new[]
                {
                    AddSeconds(started, 0),
                    AddSeconds(started, 1),
                    AddSeconds(started, 2),
                    AddSeconds(started, 3),
                    AddSeconds(started, 4),
                    AddSeconds(started, 5),
                    AddSeconds(started, 6)
                })
        };

        var coverage = ResourceSamplingCoverage.Calculate(
            processTimelines,
            dockerTimelines,
            ["nats"],
            started,
            completedAfterDrain,
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(2));

        var process = Assert.Single(coverage, series => series.Series == "gateway-1");
        Assert.Equal(5, process.SamplesInMeasurement);
        Assert.Equal(5, process.ExpectedSamplesInMeasurement);
        Assert.Equal(100, process.CoveragePercent);
        var docker = Assert.Single(coverage, series => series.Series == "nats");
        Assert.Equal(100, docker.CoveragePercent);
    }

    [Fact]
    public void MissingSeriesAndEarlyMeasurementRemainBelowThreshold()
    {
        var started = Stopwatch.GetTimestamp();
        var coverage = ResourceSamplingCoverage.Calculate(
            [
                new ProcessTimeline(
                    "gateway-1",
                    1,
                    [Sample(AddSeconds(started, 1)), Sample(AddSeconds(started, 3))])
            ],
            [],
            ["nats", "postgres"],
            started,
            AddSeconds(started, 4),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(2));

        var process = Assert.Single(coverage, series => series.Series == "gateway-1");
        Assert.Equal(40, process.CoveragePercent);
        Assert.All(
            coverage.Where(series => series.Kind == "docker"),
            series => Assert.Equal(0, series.CoveragePercent));
    }

    private static (long TimestampTicks, long WorkingSetBytes) Sample(long timestamp) =>
        (timestamp, 1);

    private static long AddSeconds(long timestamp, double seconds) =>
        timestamp + (long)(seconds * Stopwatch.Frequency);
}
