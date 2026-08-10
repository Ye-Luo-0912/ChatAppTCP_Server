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
        text.AppendLine(FormattableString.Invariant($"| Ramp | {report.RampSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant($"| Stabilization | {report.StabilizationSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant($"| Measurement | {report.MeasurementSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant($"| Delivery drain configured | {report.Configuration.DeliveryDrainSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant($"| Inactive chat heartbeat | {report.Configuration.InactiveHeartbeatSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant($"| Delivery drain elapsed | {report.DeliveryDrainElapsedSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant($"| Delivery drain completed | {report.DeliveryDrainCompleted} |"));
        text.AppendLine(FormattableString.Invariant($"| Total elapsed | {report.TotalElapsedSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant($"| Target strategy | {report.TargetStrategy} |"));
        text.AppendLine(FormattableString.Invariant($"| Unique authenticated users | {report.UniqueAuthenticatedUsers} |"));
        text.AppendLine(FormattableString.Invariant($"| Active senders | {report.Configuration.ActiveSenders} |"));
        text.AppendLine(FormattableString.Invariant($"| Connections succeeded | {report.SuccessfulConnections} |"));
        text.AppendLine(FormattableString.Invariant($"| Connections failed | {report.FailedConnections} |"));
        text.AppendLine(FormattableString.Invariant($"| TCP connect succeeded | {report.TcpConnectSucceeded} |"));
        text.AppendLine(FormattableString.Invariant($"| TCP connect failed | {report.TcpConnectFailed} |"));
        text.AppendLine(FormattableString.Invariant($"| Auth succeeded | {report.AuthSucceeded} |"));
        text.AppendLine(FormattableString.Invariant($"| Auth invalid token | {report.AuthInvalidToken} |"));
        text.AppendLine(FormattableString.Invariant($"| Auth dependency unavailable | {report.AuthDependencyUnavailable} |"));
        text.AppendLine(FormattableString.Invariant($"| Auth other failure | {report.AuthOtherFailure} |"));
        text.AppendLine(FormattableString.Invariant($"| Auth succeeded w/o resume token | {report.AuthSucceededWithoutResumeToken} |"));
        text.AppendLine(FormattableString.Invariant($"| Chat send failed | {report.ChatSendFailed} |"));
        text.AppendLine(FormattableString.Invariant($"| Chat receive failed | {report.ChatReceiveFailed} |"));
        text.AppendLine(FormattableString.Invariant($"| Server closed | {report.ServerClosed} |"));
        text.AppendLine(FormattableString.Invariant($"| Protocol rejected | {report.ProtocolRejected} |"));
        text.AppendLine(FormattableString.Invariant($"| Completed normally | {report.CompletedNormally} |"));
        text.AppendLine(FormattableString.Invariant($"| Peak active connections | {report.PeakActiveConnections} |"));
        text.AppendLine(FormattableString.Invariant($"| Healthy conns (p95) | {report.Healthy.Connections} conns, {report.Healthy.P95Ms:F3} ms |"));
        text.AppendLine(FormattableString.Invariant($"| Slow conns (p95) | {report.Slow.Connections} conns, {report.Slow.P95Ms:F3} ms |"));
        text.AppendLine(FormattableString.Invariant($"| Sent | {report.Sent} |"));
        text.AppendLine(FormattableString.Invariant($"| Expected recipient deliveries | {report.ExpectedDeliveries} |"));
        text.AppendLine(FormattableString.Invariant($"| Received | {report.Received} |"));
        text.AppendLine(FormattableString.Invariant($"| MQ accepted | {report.Acknowledged} |"));
        text.AppendLine(FormattableString.Invariant($"| Rejected | {report.Rejected} |"));
        text.AppendLine(FormattableString.Invariant($"| Duplicate/untracked MQ ACK | {report.DuplicateAcknowledgements} |"));
        text.AppendLine(FormattableString.Invariant($"| Duplicate/untracked peer delivery | {report.DuplicateDeliveries} |"));
        text.AppendLine(FormattableString.Invariant($"| ACK message-id fingerprint | {report.AcknowledgementIdFingerprint.Count} samples, sum={report.AcknowledgementIdFingerprint.SumHex}, xor={report.AcknowledgementIdFingerprint.XorHex} |"));
        text.AppendLine(FormattableString.Invariant($"| Delivery message-id fingerprint | {report.DeliveryIdFingerprint.Count} samples, sum={report.DeliveryIdFingerprint.SumHex}, xor={report.DeliveryIdFingerprint.XorHex} |"));
        text.AppendLine(FormattableString.Invariant($"| Outstanding | {report.Outstanding} |"));
        text.AppendLine(FormattableString.Invariant($"| Tracking TTL-expired | {report.TrackingExpired} |"));
        text.AppendLine(FormattableString.Invariant($"| Tracking dropped | {report.TrackingDropped} |"));
        text.AppendLine(FormattableString.Invariant($"| Runtime failure | {report.RuntimeFailure ?? "none"} |"));
        text.AppendLine(FormattableString.Invariant($"| Sent/s | {report.SentPerSecond:F2} |"));
        text.AppendLine(FormattableString.Invariant($"| Received/s | {report.ReceivedPerSecond:F2} |"));
        text.AppendLine(FormattableString.Invariant($"| Primary latency kind | {report.PrimaryLatencyKind} |"));
        text.AppendLine(FormattableString.Invariant($"| Delivery latency source | {report.DeliveryLatencySource} |"));
        text.AppendLine(FormattableString.Invariant($"| Message-id correlation evidence | {report.MessageIdCorrelationEvidence} |"));
        text.AppendLine(FormattableString.Invariant($"| Primary latency p50 | {report.Latency.P50Ms:F3} ms |"));
        text.AppendLine(FormattableString.Invariant($"| Primary latency p95 | {report.Latency.P95Ms:F3} ms |"));
        text.AppendLine(FormattableString.Invariant($"| Primary latency p99 | {report.Latency.P99Ms:F3} ms |"));
        text.AppendLine(FormattableString.Invariant($"| MQ ACK latency | {FormatLatency(report.AcknowledgementLatency)} |"));
        text.AppendLine(FormattableString.Invariant($"| Peer delivery latency | {FormatLatency(report.DeliveryLatency)} |"));
        text.AppendLine(FormattableString.Invariant($"| Semantic gate | {(report.Gate.Passed ? "PASSED" : "FAILED")} |"));
        text.AppendLine();
        text.AppendLine("```text");
        text.AppendLine(FormattableString.Invariant(
            $"Mode={report.Configuration.Mode}; Endpoint={report.Configuration.Host}:{report.Configuration.Port}"));
        text.AppendLine(FormattableString.Invariant(
            $"Connections={report.Configuration.Connections}; Duration={report.Configuration.DurationSeconds:F0}s"));
        text.AppendLine(FormattableString.Invariant(
            $"ActiveSenders={report.Configuration.ActiveSenders}; MessagesPerSecond={report.Configuration.MessagesPerSecond}; PayloadBytes={report.Configuration.PayloadBytes}; SlowReaders={report.Configuration.SlowReaders}; DeliveryDrain={report.Configuration.DeliveryDrainSeconds:F0}s; InactiveHeartbeat={report.Configuration.InactiveHeartbeatSeconds:F0}s"));
        text.AppendLine("```");

        if (report.Gate.Failures.Count != 0)
        {
            text.AppendLine();
            text.AppendLine("## Semantic gate failures");
            text.AppendLine();
            foreach (var failure in report.Gate.Failures)
                text.Append("- ").AppendLine(failure);
        }

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

    private static string FormatLatency(TcpLatencySnapshot latency) =>
        latency.Count == 0
            ? "unavailable (0 correlated samples)"
            : string.Create(
                CultureInfo.InvariantCulture,
                $"{latency.Count} samples; p50/p95/p99={latency.P50Ms:F3} / " +
                $"{latency.P95Ms:F3} / {latency.P99Ms:F3} ms");
}

internal sealed record TcpLoadReportPaths(string JsonPath, string MarkdownPath);
