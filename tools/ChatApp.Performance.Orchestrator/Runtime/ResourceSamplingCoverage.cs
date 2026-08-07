using System.Diagnostics;
using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal static class ResourceSamplingCoverage
{
    public static IReadOnlyList<ResourceSamplingSeriesCoverage> Calculate(
        IReadOnlyList<ProcessTimeline> processTimelines,
        IReadOnlyList<DockerTimeline> dockerTimelines,
        IReadOnlyList<string> expectedDockerContainers,
        long measurementStartedTimestamp,
        long measurementCompletedTimestamp,
        TimeSpan expectedMeasurementDuration,
        TimeSpan sampleInterval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(sampleInterval, TimeSpan.Zero);

        var expectedSamples = Math.Max(
            1,
            (int)Math.Floor(
                expectedMeasurementDuration.TotalMilliseconds /
                sampleInterval.TotalMilliseconds));
        var hasMeasurementWindow = measurementStartedTimestamp > 0 &&
                                   measurementCompletedTimestamp >= measurementStartedTimestamp;
        var measurementEndTimestamp = hasMeasurementWindow
            ? Math.Min(
                measurementCompletedTimestamp,
                AddStopwatchDuration(
                    measurementStartedTimestamp,
                    expectedMeasurementDuration))
            : measurementStartedTimestamp;
        var result = new List<ResourceSamplingSeriesCoverage>(
            processTimelines.Count + Math.Max(
                dockerTimelines.Count,
                expectedDockerContainers.Count));

        foreach (var timeline in processTimelines.OrderBy(
                     static timeline => timeline.Label,
                     StringComparer.Ordinal))
        {
            var samples = hasMeasurementWindow
                ? timeline.WorkingSetSamples.Count(sample =>
                    sample.TimestampTicks >= measurementStartedTimestamp &&
                    sample.TimestampTicks <= measurementEndTimestamp)
                : 0;
            result.Add(CreateSeries("process", timeline.Label, samples, expectedSamples));
        }

        var dockerByName = dockerTimelines.ToDictionary(
            static timeline => timeline.Container,
            StringComparer.Ordinal);
        var dockerNames = expectedDockerContainers
            .Concat(dockerByName.Keys)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal);
        foreach (var name in dockerNames)
        {
            var samples = 0;
            if (hasMeasurementWindow && dockerByName.TryGetValue(name, out var timeline))
            {
                samples = timeline.SampleTimestamps.Count(timestamp =>
                    timestamp >= measurementStartedTimestamp &&
                    timestamp <= measurementEndTimestamp);
            }
            result.Add(CreateSeries("docker", name, samples, expectedSamples));
        }

        return result;
    }

    private static ResourceSamplingSeriesCoverage CreateSeries(
        string kind,
        string series,
        int samples,
        int expectedSamples) => new()
    {
        Kind = kind,
        Series = series,
        SamplesInMeasurement = samples,
        ExpectedSamplesInMeasurement = expectedSamples,
        CoveragePercent = Math.Min(100d, samples * 100d / expectedSamples)
    };

    private static long AddStopwatchDuration(long timestamp, TimeSpan duration)
    {
        var durationTicks = Math.Max(
            0d,
            duration.TotalSeconds * Stopwatch.Frequency);
        if (durationTicks >= long.MaxValue - timestamp)
            return long.MaxValue;
        return timestamp + (long)Math.Ceiling(durationTicks);
    }
}
