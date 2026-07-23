using System.Globalization;
using System.Text;
using System.Text.Json;

namespace ChatApp.Performance.Orchestrator.Diagnostics;

internal static class BenchmarkReportWriter
{
    public static BenchmarkReportPaths Write(
        BenchmarkReport report,
        string sessionDirectory)
    {
        Directory.CreateDirectory(sessionDirectory);
        var jsonPath = Path.GetFullPath(Path.Combine(sessionDirectory, "benchmark-report.json"));
        var markdownPath = Path.GetFullPath(Path.Combine(sessionDirectory, "benchmark-report.md"));
        File.WriteAllText(
            jsonPath,
            JsonSerializer.Serialize(
                report,
                BenchmarkReportJsonContext.Default.BenchmarkReport));
        File.WriteAllText(markdownPath, CreateMarkdown(report));
        return new BenchmarkReportPaths(jsonPath, markdownPath);
    }

    private static string CreateMarkdown(BenchmarkReport report)
    {
        var text = new StringBuilder();
        text.AppendLine("# ChatApp multi-process benchmark report");
        text.AppendLine();
        text.AppendLine(FormattableString.Invariant(
            $"Result: **{(report.Succeeded ? "PASSED" : "FAILED")}**"));
        text.AppendLine();
        text.AppendLine(FormattableString.Invariant(
            $"Window: {report.StartedAtUtc:O} - {report.CompletedAtUtc:O}"));
        text.AppendLine();
        text.AppendLine("## Configuration");
        text.AppendLine();
        text.AppendLine("| Item | Value |");
        text.AppendLine("|---|---:|");
        text.AppendLine(FormattableString.Invariant(
            $"| Gateways | {report.Configuration.GatewayCount} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Warmup | {report.Configuration.WarmupSeconds:F0} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Duration | {report.Configuration.DurationSeconds:F0} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP mode | {report.Configuration.TcpMode} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP connections | {report.Configuration.TcpConnections} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Pipeline enabled | {report.Configuration.PipelineEnabled} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Pipeline concurrency | {report.Configuration.PipelineConcurrency} |"));
        text.AppendLine();
        text.AppendLine("## Process results");
        text.AppendLine();
        text.AppendLine("| Process | Kind | PID | Exit | Managed stop |");
        text.AppendLine("|---|---|---:|---:|---|");
        foreach (var process in report.Processes)
        {
            text.AppendLine(FormattableString.Invariant(
                $"| {process.Label} | {process.Kind} | {process.ProcessId} | {process.ExitCode?.ToString(CultureInfo.InvariantCulture) ?? "running"} | {process.StoppedByOrchestrator} |"));
        }

        text.AppendLine();
        text.AppendLine("## Load results");
        text.AppendLine();
        text.AppendLine("| Load | Kind | Succeeded | Failed | Error rate | Throughput/s | p50 ms | p95 ms | p99 ms |");
        text.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var load in report.LoadResults)
        {
            text.AppendLine(string.Format(
                CultureInfo.InvariantCulture,
                "| {0} | {1} | {2} | {3} | {4:F3}% | {5:F2} | {6:F2} | {7:F2} | {8:F2} |",
                load.Name,
                load.Kind,
                load.Succeeded,
                load.Failed,
                load.ErrorRatePercent,
                load.ThroughputPerSecond,
                load.P50Milliseconds,
                load.P95Milliseconds,
                load.P99Milliseconds));
        }

        text.AppendLine();
        text.AppendLine("## Process resources");
        text.AppendLine();
        text.AppendLine("| Process | Avg CPU | Max CPU | Start WS | End WS | WS change | Max WS | Start private | End private | Private change | Max private | Threads | Handles |");
        text.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var resource in report.ProcessResources)
        {
            text.AppendLine(string.Format(
                CultureInfo.InvariantCulture,
                "| {0} | {1:F2}% | {2:F2}% | {3:F2} MiB | {4:F2} MiB | {5:+0.00;-0.00;0.00} MiB | {6:F2} MiB | {7:F2} MiB | {8:F2} MiB | {9:+0.00;-0.00;0.00} MiB | {10:F2} MiB | {11} | {12} |",
                resource.Label,
                resource.AverageCpuPercent,
                resource.MaximumCpuPercent,
                resource.FirstWorkingSetBytes / 1_048_576d,
                resource.LastWorkingSetBytes / 1_048_576d,
                (resource.LastWorkingSetBytes - resource.FirstWorkingSetBytes) / 1_048_576d,
                resource.MaximumWorkingSetBytes / 1_048_576d,
                resource.FirstPrivateMemoryBytes / 1_048_576d,
                resource.LastPrivateMemoryBytes / 1_048_576d,
                (resource.LastPrivateMemoryBytes - resource.FirstPrivateMemoryBytes) / 1_048_576d,
                resource.MaximumPrivateMemoryBytes / 1_048_576d,
                resource.MaximumThreadCount,
                resource.MaximumHandleCount));
        }

        if (report.DockerResources.Count != 0)
        {
            text.AppendLine();
            text.AppendLine("## Docker resources");
            text.AppendLine();
            text.AppendLine("| Container | Avg CPU | Max CPU | Start memory | End memory | Memory change | Avg memory | Max memory | Last net I/O |");
            text.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|---|");
            foreach (var resource in report.DockerResources)
            {
                text.AppendLine(string.Format(
                    CultureInfo.InvariantCulture,
                    "| {0} | {1:F2}% | {2:F2}% | {3:F2} MiB | {4:F2} MiB | {5:+0.00;-0.00;0.00} MiB | {6:F2} MiB | {7:F2} MiB | {8} |",
                    resource.Container,
                    resource.AverageCpuPercent,
                    resource.MaximumCpuPercent,
                    resource.FirstMemoryBytes / 1_048_576d,
                    resource.LastMemoryBytes / 1_048_576d,
                    (resource.LastMemoryBytes - resource.FirstMemoryBytes) / 1_048_576d,
                    resource.AverageMemoryBytes / 1_048_576d,
                    resource.MaximumMemoryBytes / 1_048_576d,
                    resource.LastNetworkIo ?? string.Empty));
            }
        }

        var changedMetrics = report.MetricDeltas
            .Where(static pair => Math.Abs(pair.Value) > double.Epsilon)
            .OrderBy(static pair =>
                pair.Key.StartsWith("chatapp_", StringComparison.Ordinal) ||
                pair.Key.StartsWith("realtime_", StringComparison.Ordinal)
                    ? 0
                    : 1)
            .ThenByDescending(static pair => Math.Abs(pair.Value))
            .Take(30)
            .ToArray();
        if (changedMetrics.Length != 0)
        {
            text.AppendLine();
            text.AppendLine("## Metric changes (top 30)");
            text.AppendLine();
            text.AppendLine("| Prometheus series | Delta |");
            text.AppendLine("|---|---:|");
            foreach (var metric in changedMetrics)
            {
                text.AppendLine(string.Format(
                    CultureInfo.InvariantCulture,
                    "| `{0}` | {1:G8} |",
                    metric.Key,
                    metric.Value));
            }
        }

        if (report.MetricTrends.Count != 0)
        {
            text.AppendLine();
            text.AppendLine("## Soak metric trends");
            text.AppendLine();
            text.AppendLine("| Prometheus series | Samples | Start | End | Delta | Min | Max |");
            text.AppendLine("|---|---:|---:|---:|---:|---:|---:|");
            foreach (var metric in report.MetricTrends)
            {
                text.AppendLine(string.Format(
                    CultureInfo.InvariantCulture,
                    "| `{0}` | {1} | {2:G8} | {3:G8} | {4:G8} | {5:G8} | {6:G8} |",
                    metric.Series,
                    metric.Samples,
                    metric.FirstValue,
                    metric.LastValue,
                    metric.Delta,
                    metric.MinimumValue,
                    metric.MaximumValue));
            }
        }

        if (report.Artifacts.Count != 0)
        {
            text.AppendLine();
            text.AppendLine("## Child reports");
            text.AppendLine();
            foreach (var artifact in report.Artifacts)
                text.Append("- `").Append(artifact).AppendLine("`");
        }

        if (report.Errors.Count != 0)
        {
            text.AppendLine();
            text.AppendLine("## Errors");
            text.AppendLine();
            foreach (var error in report.Errors)
                text.Append("- ").AppendLine(error);
        }

        return text.ToString();
    }
}

internal sealed record BenchmarkReportPaths(string JsonPath, string MarkdownPath);
