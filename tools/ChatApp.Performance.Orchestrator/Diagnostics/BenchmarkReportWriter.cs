using System.Globalization;
using System.Text;
using System.Text.Json;

namespace ChatApp.Performance.Orchestrator.Diagnostics;

internal static class BenchmarkReportWriter
{
    public static BenchmarkReportPaths Write(
        BenchmarkReport report,
        IReadOnlyList<ProcessTimeline> timelines,
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
        WriteTimelineCsv(sessionDirectory, timelines);
        return new BenchmarkReportPaths(jsonPath, markdownPath);
    }

    private static void WriteTimelineCsv(
        string sessionDirectory,
        IReadOnlyList<ProcessTimeline> timelines)
    {
        var csvPath = Path.GetFullPath(Path.Combine(sessionDirectory, "process-resource-timeline.csv"));
        var builder = new StringBuilder();
        builder.AppendLine("label,pid,timestamp_ticks,working_set_bytes");
        foreach (var timeline in timelines)
        {
            foreach (var (timestampTicks, workingSetBytes) in timeline.WorkingSetSamples)
            {
                builder.Append(timeline.Label).Append(',')
                    .Append(timeline.ProcessId).Append(',')
                    .Append(timestampTicks).Append(',')
                    .Append(workingSetBytes).AppendLine();
            }
        }

        File.WriteAllText(csvPath, builder.ToString(), new UTF8Encoding(false));
    }

    private static string CreateMarkdown(BenchmarkReport report)
    {
        var text = new StringBuilder();
        text.AppendLine("# ChatApp multi-process benchmark report");
        text.AppendLine();
        text.AppendLine(FormattableString.Invariant(
            $"Result: **{(report.Succeeded ? "PASSED" : "FAILED")}**"));
        text.AppendLine(FormattableString.Invariant(
            $"Run validity: **{(report.Validity.IsValid ? "VALID" : "INVALID")}**"));
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
            $"| Realtime processing concurrency | {report.Configuration.RealtimeProcessingConcurrency} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Realtime queue / prefetch / max ACK pending | {report.Configuration.RealtimeProcessingQueueCapacity} / {report.Configuration.RealtimePrefetchMaxMessages} / {report.Configuration.RealtimeMaxAckPending} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Realtime routing | {(report.Configuration.RealtimeShardedRouting ? "Sharded" : "Broadcast")} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Warmup | {report.Configuration.WarmupSeconds:F0} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Duration | {report.Configuration.DurationSeconds:F0} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP mode | {report.Configuration.TcpMode} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP connections | {report.Configuration.TcpConnections} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP active senders | {report.Configuration.TcpActiveSenders} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP peer routing | {(report.Configuration.TcpCrossGateway ? "Cross-Gateway" : "Same-Gateway")} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP messages / active sender | {report.Configuration.TcpMessagesPerSecond:G17}/s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP delivery drain | {report.Configuration.TcpDeliveryDrainSeconds:F0} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP inactive heartbeat | {report.Configuration.TcpInactiveHeartbeatSeconds:F0} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP ACK / delivery gates | {report.Configuration.TcpMinimumAcknowledgementRatio:P3} / {report.Configuration.TcpMinimumDeliveryRatio:P3} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP global connection ramp | {report.Configuration.TcpConnectionsPerSecond}/s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Estimated ramp | {report.Configuration.EstimatedTcpRampSeconds:F0} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| TCP token users | {report.Configuration.TcpTokenCount} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Bootstrap token TTL | {report.Configuration.TcpBootstrapTokenLifetimeSeconds:F0} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Inbound transport | {report.Configuration.InboundTransportMode} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Outbound send | {report.Configuration.OutboundSendMode} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Outbound queue | {report.Configuration.OutboundQueueMode} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Pipeline enabled | {report.Configuration.PipelineEnabled} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Pipeline concurrency | {report.Configuration.PipelineConcurrency} |"));
        text.AppendLine();
        text.AppendLine("## Reproducibility");
        text.AppendLine();
        text.AppendLine("| Repository | Commit | Branch | Dirty | Capture error |");
        text.AppendLine("|---|---|---|---|---|");
        AppendRepository(text, "Gateway / orchestrator", report.Provenance.GatewayRepository);
        AppendRepository(text, "Realtime", report.Provenance.RealtimeRepository);
        text.AppendLine();
        text.AppendLine(FormattableString.Invariant(
            $"Orchestrator version: `{report.Provenance.OrchestratorVersion}`"));
        text.AppendLine();
        text.AppendLine("### Frozen snapshot binding");
        text.AppendLine();
        text.AppendLine("| Item | Value |");
        text.AppendLine("|---|---|");
        text.AppendLine(FormattableString.Invariant(
            $"| Strict binding required | {report.Provenance.SnapshotBinding.Required} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Binding complete | {report.Provenance.SnapshotBinding.Complete} |"));
        AppendSnapshotBinding(text, "Run ID", report.Provenance.SnapshotBinding.RunId);
        AppendSnapshotBinding(text, "Run root", report.Provenance.SnapshotBinding.RunRoot);
        AppendSnapshotBinding(
            text,
            "Source archive path",
            report.Provenance.SnapshotBinding.SourceArchivePath);
        AppendSnapshotBinding(
            text,
            "Source archive SHA-256",
            report.Provenance.SnapshotBinding.SourceArchiveSha256);
        AppendSnapshotBinding(
            text,
            "Canonical feed archive path",
            report.Provenance.SnapshotBinding.CanonicalFeedArchivePath);
        AppendSnapshotBinding(
            text,
            "Canonical feed archive SHA-256",
            report.Provenance.SnapshotBinding.CanonicalFeedArchiveSha256);
        AppendSnapshotBinding(
            text,
            "dotnet executable path",
            report.Provenance.SnapshotBinding.DotnetExecutablePath);
        AppendSnapshotBinding(
            text,
            "dotnet executable SHA-256",
            report.Provenance.SnapshotBinding.DotnetExecutableSha256);
        text.AppendLine();
        text.AppendLine("## Run validity and sampling coverage");
        text.AppendLine();
        text.AppendLine("| Item | Value |");
        text.AppendLine("|---|---:|");
        text.AppendLine(FormattableString.Invariant(
            $"| Expected measurement | {report.Validity.ExpectedMeasurementSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Observed coordinated window | {report.Validity.ObservedMeasurementSeconds:F2} s |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Measurement boundary source | {report.Validity.MeasurementBoundarySource} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Completed load processes | {report.Validity.CompletedLoadProcesses}/{report.Validity.ExpectedLoadProcesses} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Services alive throughout | {report.Validity.ServicesAliveThroughMeasurement} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Process samples (minimum/expected) | {report.Validity.MinimumProcessSamples}/{report.Validity.ExpectedProcessSamplesPerProcess} ({report.Validity.ProcessSamplingCoveragePercent:F1}%) |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Prometheus samples | {report.Validity.PrometheusSamples}/{report.Validity.ExpectedPrometheusSamples} ({report.Validity.PrometheusSamplingCoveragePercent:F1}%) |"));
        if (report.Validity.InvalidReasons.Count != 0)
        {
            text.AppendLine();
            foreach (var reason in report.Validity.InvalidReasons)
                text.Append("- ").AppendLine(reason);
        }
        text.AppendLine();
        text.AppendLine("## Process results");
        text.AppendLine();
        // item 八：把 exit 137 归因到具体 OOM 来源，而不是一律判成 Gateway 内存泄漏。
        text.AppendLine("| Process | Kind | PID | Exit | Managed stop | OOM attribution |");
        text.AppendLine("|---|---|---:|---:|---:|---|");
        foreach (var process in report.Processes)
        {
            text.AppendLine(FormattableString.Invariant(
                $"| {process.Label} | {process.Kind} | {process.ProcessId} | {process.ExitCode?.ToString(CultureInfo.InvariantCulture) ?? "running"} | {process.StoppedByOrchestrator} | {OomClassificationName(process.OomClassification)} |"));
        }

        var oomProcesses = report.Processes
            .Where(static process => process.OomClassification != OomClassification.None)
            .ToArray();
        if (oomProcesses.Length != 0)
        {
            text.AppendLine();
            text.AppendLine("### OOM attribution evidence");
            text.AppendLine();
            foreach (var process in oomProcesses)
            {
                text.Append("- **").Append(process.Label).Append("**: ")
                    .Append(OomClassificationName(process.OomClassification));
                if (!string.IsNullOrWhiteSpace(process.OomEvidence))
                    text.Append(" — `").Append(process.OomEvidence).Append('`');
                text.AppendLine();
            }
        }

        text.AppendLine();
        text.AppendLine("## Load results");
        text.AppendLine();
        text.AppendLine("| Load | Kind | Ramp s | Stabilize s | Measure s | Target | Users | Active senders | Succeeded | Failed | Error rate | Throughput/s | p50 ms | p95 ms | p99 ms |");
        text.AppendLine("|---|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var load in report.LoadResults)
        {
            text.AppendLine(string.Format(
                CultureInfo.InvariantCulture,
                "| {0} | {1} | {2:F2} | {3:F2} | {4:F2} | {5} | {6} | {7} | {8} | {9} | {10:F3}% | {11:F2} | {12:F2} | {13:F2} | {14:F2} |",
                load.Name,
                load.Kind,
                load.RampSeconds,
                load.StabilizationSeconds,
                load.MeasurementSeconds,
                load.TargetStrategy ?? string.Empty,
                load.UniqueAuthenticatedUsers,
                load.ActiveSenders,
                load.Succeeded,
                load.Failed,
                load.ErrorRatePercent,
                load.ThroughputPerSecond,
                load.P50Milliseconds,
                load.P95Milliseconds,
                load.P99Milliseconds));
        }

        var tcpMessageLoads = report.LoadResults
            .Where(static load => load.Kind.StartsWith("tcp-", StringComparison.Ordinal))
            .ToArray();
        if (tcpMessageLoads.Length != 0)
        {
            text.AppendLine();
            text.AppendLine("### TCP message semantics");
            text.AppendLine();
            text.AppendLine("| Load | Sent messages | Expected recipient deliveries | MQ ACK | Received deliveries | Rejected |");
            text.AppendLine("|---|---:|---:|---:|---:|---:|");
            foreach (var load in tcpMessageLoads)
            {
                text.AppendLine(FormattableString.Invariant(
                    $"| {load.Name} | {load.MessagesSent} | {load.MessagesExpectedDeliveries} | {load.MessagesAcknowledged} | {load.MessagesReceived} | {load.MessagesRejected} |"));
            }
        }

        if (tcpMessageLoads.Length != 0)
        {
            text.AppendLine();
            text.AppendLine("### TCP stage attribution");
            text.AppendLine();
            text.AppendLine("| Load | TCP conn fail | Auth invalid token | Auth dependency | Auth other | Auth w/o resume token | Server closed | Protocol rejected |");
            text.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|");
            foreach (var load in tcpMessageLoads)
            {
                text.AppendLine(FormattableString.Invariant(
                    $"| {load.Name} | {load.TcpConnectFailed} | {load.AuthInvalidToken} | {load.AuthDependencyUnavailable} | {load.AuthOtherFailure} | {load.AuthSucceededWithoutResumeToken} | {load.ServerClosed} | {load.ProtocolRejected} |"));
            }
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

    private static void AppendRepository(
        StringBuilder text,
        string label,
        GitRepositorySnapshot repository)
    {
        text.Append("| ").Append(label)
            .Append(" | `").Append(repository.CommitSha).Append('`')
            .Append(" | `").Append(repository.Branch).Append('`')
            .Append(" | ").Append(repository.WorkingTreeDirty)
            .Append(" | ").Append(repository.CaptureError ?? string.Empty)
            .AppendLine(" |");
    }

    private static void AppendSnapshotBinding(
        StringBuilder text,
        string label,
        string? value)
    {
        var displayValue = (value ?? "not supplied")
            .Replace("|", "\\|", StringComparison.Ordinal)
            .Replace("`", "&#96;", StringComparison.Ordinal)
            .Replace("\r", string.Empty, StringComparison.Ordinal)
            .Replace("\n", " ", StringComparison.Ordinal);
        text.Append("| ").Append(label).Append(" | `").Append(displayValue).AppendLine("` |");
    }

    // item 八：把 OomClassification 转成人工可读文案，避免把 exit 137 一律判成内存泄漏。
    private static string OomClassificationName(OomClassification classification) =>
        classification switch
        {
            OomClassification.None => "none",
            OomClassification.ManagedOOM => "ManagedOOM",
            OomClassification.KilledByCgroupOOM => "KilledByCgroupOOM",
            OomClassification.KilledByKernelOOM => "KilledByKernelOOM",
            OomClassification.SIGKILLUnknown => "SIGKILLUnknown",
            _ => classification.ToString()
        };
}

internal sealed record BenchmarkReportPaths(string JsonPath, string MarkdownPath);
