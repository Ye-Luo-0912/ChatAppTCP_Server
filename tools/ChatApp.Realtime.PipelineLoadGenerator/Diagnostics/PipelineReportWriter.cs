using System.Globalization;
using System.Text;
using System.Text.Json;

namespace ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;

internal static class PipelineReportWriter
{
    public static void WriteConsole(PipelineLoadReport report)
    {
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"NATS ping: {report.NatsPingMs:F2} ms"));
        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Pipelines: {report.Succeeded} succeeded, {report.Failed} failed; " +
                $"{report.CompletedPipelinesPerSecond:F2}/s; error={report.ErrorRatePercent:F3}%"));

        foreach (var (name, latency) in report.Latencies)
        {
            Console.WriteLine(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"{name}: count={latency.Count}; avg={latency.AverageMs:F2} ms; " +
                    $"p50={latency.P50Ms:F2}; p95={latency.P95Ms:F2}; " +
                    $"p99={latency.P99Ms:F2}; max={latency.MaximumMs:F2}"));
        }

        foreach (var error in report.ErrorSamples)
            Console.Error.WriteLine(error);
    }

    public static PipelineReportPaths? WriteFiles(
        PipelineLoadReport report,
        string? reportDirectory)
    {
        if (string.IsNullOrWhiteSpace(reportDirectory))
            return null;

        Directory.CreateDirectory(reportDirectory);
        var timestamp = report.GeneratedAtUtc.ToString(
            "yyyyMMdd-HHmmss'Z'",
            CultureInfo.InvariantCulture);
        var jsonPath = Path.GetFullPath(
            Path.Combine(reportDirectory, $"pipeline-load-{timestamp}.json"));
        var markdownPath = Path.GetFullPath(
            Path.Combine(reportDirectory, $"pipeline-load-{timestamp}.md"));

        File.WriteAllText(
            jsonPath,
            JsonSerializer.Serialize(
                report,
                PipelineReportJsonContext.Default.PipelineLoadReport));
        File.WriteAllText(markdownPath, CreateMarkdown(report));
        return new PipelineReportPaths(jsonPath, markdownPath);
    }

    private static string CreateMarkdown(PipelineLoadReport report)
    {
        var text = new StringBuilder();
        text.AppendLine("# Realtime pipeline load report");
        text.AppendLine();
        text.AppendLine("Generated: " + report.GeneratedAtUtc.ToString("O", CultureInfo.InvariantCulture));
        text.AppendLine();
        text.AppendLine("## Result");
        text.AppendLine();
        text.AppendLine("| Metric | Value |");
        text.AppendLine("|---|---:|");
        text.AppendLine(FormattableString.Invariant(
            $"| NATS ping | {report.NatsPingMs:F2} ms |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Elapsed | {report.ElapsedSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Succeeded | {report.Succeeded} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Failed | {report.Failed} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Error rate | {report.ErrorRatePercent:F3}% |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Completed pipelines/s | {report.CompletedPipelinesPerSecond:F2} |"));
        text.AppendLine();
        text.AppendLine("## Latency");
        text.AppendLine();
        text.AppendLine("| Stage | Count | Avg ms | p50 ms | p95 ms | p99 ms | Max ms |");
        text.AppendLine("|---|---:|---:|---:|---:|---:|---:|");
        foreach (var (name, latency) in report.Latencies)
        {
            text.AppendLine(string.Format(
                CultureInfo.InvariantCulture,
                "| {0} | {1} | {2:F2} | {3:F2} | {4:F2} | {5:F2} | {6:F2} |",
                name,
                latency.Count,
                latency.AverageMs,
                latency.P50Ms,
                latency.P95Ms,
                latency.P99Ms,
                latency.MaximumMs));
        }

        text.AppendLine();
        text.AppendLine("## Configuration");
        text.AppendLine();
        text.AppendLine("```text");
        text.AppendLine(FormattableString.Invariant(
            $"NATS={report.Configuration.NatsUrl}"));
        text.AppendLine(FormattableString.Invariant(
            $"Warmup={report.Configuration.WarmupSeconds:F0}s"));
        text.AppendLine(FormattableString.Invariant(
            $"Duration={report.Configuration.DurationSeconds:F0}s"));
        text.AppendLine(FormattableString.Invariant(
            $"Concurrency={report.Configuration.Concurrency}"));
        text.AppendLine(FormattableString.Invariant(
            $"TargetOpsPerSecond={report.Configuration.TargetOperationsPerSecond}"));
        text.AppendLine(FormattableString.Invariant(
            $"PayloadBytes={report.Configuration.PayloadBytes}"));
        text.AppendLine("```");

        if (report.ErrorSamples.Count != 0)
        {
            text.AppendLine();
            text.AppendLine("## Error samples");
            text.AppendLine();
            foreach (var error in report.ErrorSamples)
                text.Append("- ").AppendLine(error);
        }

        return text.ToString();
    }
}

internal sealed record PipelineReportPaths(
    string JsonPath,
    string MarkdownPath);
