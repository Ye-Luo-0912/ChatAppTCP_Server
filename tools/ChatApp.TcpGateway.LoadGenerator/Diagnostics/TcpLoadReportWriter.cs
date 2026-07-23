using System.Globalization;
using System.Text;
using System.Text.Json;

namespace ChatApp.TcpGateway.LoadGenerator.Diagnostics;

internal static class TcpLoadReportWriter
{
    public static TcpLoadReportPaths? WriteFiles(
        TcpLoadReport report,
        string? reportDirectory)
    {
        if (string.IsNullOrWhiteSpace(reportDirectory))
            return null;

        Directory.CreateDirectory(reportDirectory);
        var timestamp = report.GeneratedAtUtc.ToString(
            "yyyyMMdd-HHmmss'Z'",
            CultureInfo.InvariantCulture);
        var jsonPath = Path.GetFullPath(
            Path.Combine(reportDirectory, $"tcp-load-{timestamp}.json"));
        var markdownPath = Path.GetFullPath(
            Path.Combine(reportDirectory, $"tcp-load-{timestamp}.md"));

        File.WriteAllText(
            jsonPath,
            JsonSerializer.Serialize(
                report,
                TcpLoadReportJsonContext.Default.TcpLoadReport));
        File.WriteAllText(markdownPath, CreateMarkdown(report));
        return new TcpLoadReportPaths(jsonPath, markdownPath);
    }

    private static string CreateMarkdown(TcpLoadReport report)
    {
        var text = new StringBuilder();
        text.AppendLine("# TCP gateway load report");
        text.AppendLine();
        text.AppendLine("Generated: " + report.GeneratedAtUtc.ToString("O", CultureInfo.InvariantCulture));
        text.AppendLine();
        text.AppendLine("| Metric | Value |");
        text.AppendLine("|---|---:|");
        text.AppendLine(FormattableString.Invariant($"| Elapsed | {report.ElapsedSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant($"| Connections succeeded | {report.SuccessfulConnections} |"));
        text.AppendLine(FormattableString.Invariant($"| Connections failed | {report.FailedConnections} |"));
        text.AppendLine(FormattableString.Invariant($"| Sent | {report.Sent} |"));
        text.AppendLine(FormattableString.Invariant($"| Received | {report.Received} |"));
        text.AppendLine(FormattableString.Invariant($"| MQ accepted | {report.Acknowledged} |"));
        text.AppendLine(FormattableString.Invariant($"| Rejected | {report.Rejected} |"));
        text.AppendLine(FormattableString.Invariant($"| Sent/s | {report.SentPerSecond:F2} |"));
        text.AppendLine(FormattableString.Invariant($"| Received/s | {report.ReceivedPerSecond:F2} |"));
        text.AppendLine(FormattableString.Invariant($"| Latency p50 | {report.Latency.P50Ms:F3} ms |"));
        text.AppendLine(FormattableString.Invariant($"| Latency p95 | {report.Latency.P95Ms:F3} ms |"));
        text.AppendLine(FormattableString.Invariant($"| Latency p99 | {report.Latency.P99Ms:F3} ms |"));
        text.AppendLine();
        text.AppendLine("```text");
        text.AppendLine(FormattableString.Invariant(
            $"Mode={report.Configuration.Mode}; Endpoint={report.Configuration.Host}:{report.Configuration.Port}"));
        text.AppendLine(FormattableString.Invariant(
            $"Connections={report.Configuration.Connections}; Duration={report.Configuration.DurationSeconds:F0}s"));
        text.AppendLine(FormattableString.Invariant(
            $"MessagesPerSecond={report.Configuration.MessagesPerSecond}; PayloadBytes={report.Configuration.PayloadBytes}; SlowReaders={report.Configuration.SlowReaders}"));
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

internal sealed record TcpLoadReportPaths(string JsonPath, string MarkdownPath);
