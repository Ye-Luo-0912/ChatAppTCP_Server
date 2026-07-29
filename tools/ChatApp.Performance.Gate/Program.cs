using System.Globalization;
using System.Text.Json;

var options = GateOptions.Parse(args);
using var document = JsonDocument.Parse(File.ReadAllText(options.ReportPath));
var report = document.RootElement;
var checks = new List<GateCheck>();

var benchmarkSucceeded = report.GetProperty("Succeeded").GetBoolean();
checks.Add(new("Benchmark report succeeded", benchmarkSucceeded, benchmarkSucceeded ? "passed" : "failed"));

var pipeline = report.GetProperty("LoadResults")
    .EnumerateArray()
    .FirstOrDefault(static item => item.GetProperty("Name").GetString() == "pipeline");
if (pipeline.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
    throw new InvalidOperationException("Pipeline load result was not found in the benchmark report.");

var errorRate = pipeline.GetProperty("ErrorRatePercent").GetDouble();
var p95 = pipeline.GetProperty("P95Milliseconds").GetDouble();
checks.Add(new(
    $"Pipeline error rate <= {options.MaximumErrorRatePercent.ToString(CultureInfo.InvariantCulture)}%",
    errorRate <= options.MaximumErrorRatePercent,
    errorRate.ToString("F4", CultureInfo.InvariantCulture) + "%"));
checks.Add(new(
    $"Pipeline p95 <= {options.MaximumP95Milliseconds.ToString(CultureInfo.InvariantCulture)} ms",
    p95 <= options.MaximumP95Milliseconds,
    p95.ToString("F2", CultureInfo.InvariantCulture) + " ms"));

var expectedTcpConnections = report.GetProperty("Configuration")
    .GetProperty("TcpConnections")
    .GetInt32();
if (expectedTcpConnections > 0)
{
    var tcpResults = report.GetProperty("LoadResults")
        .EnumerateArray()
        .Where(static item => item.GetProperty("Kind").GetString()?.StartsWith(
            "tcp-",
            StringComparison.Ordinal) == true)
        .ToArray();
    if (tcpResults.Length == 0)
        throw new InvalidOperationException(
            "TCP load results were not found in the benchmark report.");

    var successfulTcpConnections = tcpResults.Sum(
        static item => item.GetProperty("Succeeded").GetInt64());
    var failedTcpConnections = tcpResults.Sum(
        static item => item.GetProperty("Failed").GetInt64());
    checks.Add(new(
        $"TCP connections succeeded >= {expectedTcpConnections.ToString(CultureInfo.InvariantCulture)}",
        successfulTcpConnections >= expectedTcpConnections,
        successfulTcpConnections.ToString(CultureInfo.InvariantCulture)));
    checks.Add(new(
        "TCP connection failures <= 0",
        failedTcpConnections == 0,
        failedTcpConnections.ToString(CultureInfo.InvariantCulture)));
}

var metrics = report.GetProperty("MetricsAfter");
var jetStreamPending = SumRequiredMetrics(metrics, "chatapp_jetstream_pending{");
var outboxPending = SumRequiredMetrics(metrics, "realtime_outbox_pending{");
var outboxOldestAge = MaxRequiredMetric(metrics, "realtime_outbox_oldest_age_seconds{");
checks.Add(new(
    $"Final JetStream pending <= {options.MaximumJetStreamPending}",
    jetStreamPending <= options.MaximumJetStreamPending,
    jetStreamPending.ToString("F0", CultureInfo.InvariantCulture)));
checks.Add(new(
    $"Final Outbox pending <= {options.MaximumOutboxPending}",
    outboxPending <= options.MaximumOutboxPending,
    outboxPending.ToString("F0", CultureInfo.InvariantCulture)));
checks.Add(new(
    $"Final Outbox oldest age <= {options.MaximumOutboxOldestAgeSeconds.ToString(CultureInfo.InvariantCulture)} s",
    outboxOldestAge <= options.MaximumOutboxOldestAgeSeconds,
    outboxOldestAge.ToString("F3", CultureInfo.InvariantCulture) + " s"));

// GC / 内存 / 每连接字节 硬门禁：可选阈值，null 时跳过。
// 指标来源：MetricsAfter（Prometheus 快照）+ ProcessResources（进程级采样）。
// GC 指标为 cumulative counter，用 MetricDeltas（After-Before）计算增量。
if (report.TryGetProperty("MetricDeltas", out var deltas))
{
    AddGcGateChecks(checks, deltas, options);
}

if (report.TryGetProperty("ProcessResources", out var processResources))
{
    AddProcessResourceChecks(checks, processResources, options);
}

if (expectedTcpConnections > 0 && options.MaximumBytesPerConnection is not null)
{
    AddBytesPerConnectionCheck(checks, metrics, options, expectedTcpConnections);
}

AddStageLatencyChecks(checks, pipeline, options);

var result = new GateResult(
    Path.GetFullPath(options.ReportPath),
    checks.All(static check => check.Passed),
    DateTimeOffset.UtcNow,
    checks);
var output = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true });
if (options.OutputPath is not null)
    File.WriteAllText(options.OutputPath, output);
Console.WriteLine(output);
return result.Passed ? 0 : 1;

static void AddGcGateChecks(List<GateCheck> checks, JsonElement deltas, GateOptions options)
{
    // Gen2 GC 次数增量（cumulative counter delta）。
    if (options.MaximumGen2Collections is not null)
    {
        var gen2Delta = SumOptionalMetrics(deltas, "dotnet_gc_collections_total{", "gc_heap_generation=\"gen2\"");
        if (gen2Delta.HasValue)
        {
            checks.Add(new(
                $"Gen2 GC collections delta <= {options.MaximumGen2Collections}",
                gen2Delta.Value <= options.MaximumGen2Collections.Value,
                gen2Delta.Value.ToString("F0", CultureInfo.InvariantCulture)));
        }
    }

    // GC 暂停时间增量（秒，cumulative counter delta）。
    if (options.MaximumGcPauseSeconds is not null)
    {
        var pauseDelta = SumOptionalMetrics(deltas, "dotnet_gc_pause_time_seconds_total{");
        if (pauseDelta.HasValue)
        {
            checks.Add(new(
                $"GC pause time delta <= {options.MaximumGcPauseSeconds.Value.ToString(CultureInfo.InvariantCulture)} s",
                pauseDelta.Value <= options.MaximumGcPauseSeconds.Value,
                pauseDelta.Value.ToString("F3", CultureInfo.InvariantCulture) + " s"));
        }
    }

    // 总分配字节增量（cumulative counter delta），转 MB 便于阈值配置。
    if (options.MaximumAllocatedMegabytes is not null)
    {
        var allocDelta = SumOptionalMetrics(deltas, "dotnet_gc_heap_total_allocated_bytes_total{");
        if (allocDelta.HasValue)
        {
            var allocMb = allocDelta.Value / (1024.0 * 1024.0);
            checks.Add(new(
                $"Allocated bytes delta <= {options.MaximumAllocatedMegabytes.Value.ToString(CultureInfo.InvariantCulture)} MB",
                allocMb <= options.MaximumAllocatedMegabytes.Value,
                allocMb.ToString("F2", CultureInfo.InvariantCulture) + " MB"));
        }
    }

    // LOH 最后堆大小（gauge，非 counter，直接读 MetricsAfter 但此处用 deltas 也合理）。
    if (options.MaximumLohHeapBytes is not null)
    {
        var lohSize = MaxOptionalMetric(deltas, "dotnet_gc_last_collection_heap_size_bytes{", "gc_heap_generation=\"loh\"");
        if (lohSize.HasValue)
        {
            checks.Add(new(
                $"LOH heap size <= {options.MaximumLohHeapBytes.Value.ToString(CultureInfo.InvariantCulture)} bytes",
                lohSize.Value <= options.MaximumLohHeapBytes.Value,
                lohSize.Value.ToString("F0", CultureInfo.InvariantCulture) + " bytes"));
        }
    }

    // POH 最后堆大小。
    if (options.MaximumPohHeapBytes is not null)
    {
        var pohSize = MaxOptionalMetric(deltas, "dotnet_gc_last_collection_heap_size_bytes{", "gc_heap_generation=\"poh\"");
        if (pohSize.HasValue)
        {
            checks.Add(new(
                $"POH heap size <= {options.MaximumPohHeapBytes.Value.ToString(CultureInfo.InvariantCulture)} bytes",
                pohSize.Value <= options.MaximumPohHeapBytes.Value,
                pohSize.Value.ToString("F0", CultureInfo.InvariantCulture) + " bytes"));
        }
    }
}

static void AddProcessResourceChecks(
    List<GateCheck> checks,
    JsonElement processResources,
    GateOptions options)
{
    if (options.MaximumWorkingSetMegabytes is null)
        return;

    // 取所有进程的最大 WorkingSet 峰值（含 gateway + realtime）。
    double maxWorkingSetBytes = 0;
    foreach (var proc in processResources.EnumerateArray())
    {
        if (proc.TryGetProperty("MaximumWorkingSetBytes", out var wsEl) &&
            wsEl.ValueKind == JsonValueKind.Number)
        {
            var ws = wsEl.GetDouble();
            if (ws > maxWorkingSetBytes)
                maxWorkingSetBytes = ws;
        }
    }

    if (maxWorkingSetBytes > 0)
    {
        var wsMb = maxWorkingSetBytes / (1024.0 * 1024.0);
        checks.Add(new(
            $"Max working set <= {options.MaximumWorkingSetMegabytes.Value.ToString(CultureInfo.InvariantCulture)} MB",
            wsMb <= options.MaximumWorkingSetMegabytes.Value,
            wsMb.ToString("F2", CultureInfo.InvariantCulture) + " MB"));
    }
}

static void AddBytesPerConnectionCheck(
    List<GateCheck> checks,
    JsonElement metrics,
    GateOptions options,
    int expectedTcpConnections)
{
    // gateway.inbound.avg_per_session.bytes 是 gauge，直接读 MetricsAfter。
    // 取所有 gateway 副本的最大值。
    var avgPerSession = MaxOptionalMetric(metrics, "gateway.inbound.avg_per_session.bytes{");
    var threshold = options.MaximumBytesPerConnection;
    if (avgPerSession.HasValue && threshold.HasValue)
    {
        checks.Add(new(
            $"Inbound avg bytes/session <= {threshold.Value.ToString(CultureInfo.InvariantCulture)}",
            avgPerSession.Value <= threshold.Value,
            avgPerSession.Value.ToString("F0", CultureInfo.InvariantCulture) + " bytes"));
    }
}

static void AddStageLatencyChecks(
    List<GateCheck> checks,
    JsonElement pipeline,
    GateOptions options)
{
    if (options.MaximumHistoryP95Milliseconds is null &&
        options.MaximumConversationListP95Milliseconds is null &&
        options.MaximumSyncBootstrapP95Milliseconds is null &&
        !options.RequireConversationStages)
    {
        return;
    }

    if (!pipeline.TryGetProperty("SourceReport", out var sourceReportPathElement))
        throw new InvalidOperationException("Pipeline SourceReport path was not found.");

    var sourceReportPath = sourceReportPathElement.GetString();
    if (string.IsNullOrWhiteSpace(sourceReportPath) || !File.Exists(sourceReportPath))
        throw new InvalidOperationException(
            "Pipeline SourceReport file was missing: " + sourceReportPath);

    using var sourceDocument = JsonDocument.Parse(File.ReadAllText(sourceReportPath));
    if (!sourceDocument.RootElement.TryGetProperty("Latencies", out var latencies))
        throw new InvalidOperationException("Pipeline SourceReport Latencies were missing.");

    RequireOrCheck(
        checks,
        latencies,
        "history_query",
        options.MaximumHistoryP95Milliseconds,
        options.RequireConversationStages);
    RequireOrCheck(
        checks,
        latencies,
        "conversation_list_query",
        options.MaximumConversationListP95Milliseconds,
        options.RequireConversationStages);
    RequireOrCheck(
        checks,
        latencies,
        "sync_bootstrap",
        options.MaximumSyncBootstrapP95Milliseconds,
        options.RequireConversationStages);
}

static void RequireOrCheck(
    List<GateCheck> checks,
    JsonElement latencies,
    string stage,
    double? maximumP95Milliseconds,
    bool requireStage)
{
    if (!latencies.TryGetProperty(stage, out var stageElement))
    {
        if (requireStage || maximumP95Milliseconds is not null)
        {
            checks.Add(new(
                $"Pipeline stage '{stage}' present",
                false,
                "missing"));
        }

        return;
    }

    if (maximumP95Milliseconds is null)
    {
        if (requireStage)
        {
            checks.Add(new(
                $"Pipeline stage '{stage}' present",
                true,
                "present"));
        }

        return;
    }

    var actual = stageElement.GetProperty("P95Ms").GetDouble();
    checks.Add(new(
        $"Pipeline {stage} p95 <= {maximumP95Milliseconds.Value.ToString(CultureInfo.InvariantCulture)} ms",
        actual <= maximumP95Milliseconds.Value,
        actual.ToString("F2", CultureInfo.InvariantCulture) + " ms"));
}

static double SumRequiredMetrics(JsonElement metrics, string prefix)
{
    var sum = 0d;
    var matches = 0;
    foreach (var metric in metrics.EnumerateObject())
    {
        if (metric.Name.StartsWith(prefix, StringComparison.Ordinal))
        {
            sum += metric.Value.GetDouble();
            matches++;
        }
    }

    if (matches == 0)
        throw new InvalidOperationException($"Required metric '{prefix}' was not found in MetricsAfter.");

    return sum;
}

/// <summary>
/// 可选指标求和：按 prefix + 可选 labelFilter 匹配，未找到返回 null（不抛异常）。
/// labelFilter 用于区分同前缀不同 label 的 series（如 gen0/gen1/gen2）。
/// </summary>
static double? SumOptionalMetrics(JsonElement metrics, string prefix, string? labelFilter = null)
{
    var sum = 0d;
    var matches = 0;
    foreach (var metric in metrics.EnumerateObject())
    {
        if (!metric.Name.StartsWith(prefix, StringComparison.Ordinal))
            continue;
        if (labelFilter is not null && !metric.Name.Contains(labelFilter, StringComparison.Ordinal))
            continue;
        sum += metric.Value.GetDouble();
        matches++;
    }

    return matches > 0 ? sum : null;
}

static double MaxRequiredMetric(JsonElement metrics, string prefix)
{
    double? maximum = null;
    foreach (var metric in metrics.EnumerateObject())
    {
        if (!metric.Name.StartsWith(prefix, StringComparison.Ordinal))
            continue;

        maximum = Math.Max(maximum ?? double.NegativeInfinity, metric.Value.GetDouble());
    }

    return maximum ?? throw new InvalidOperationException(
        $"Required metric '{prefix}' was not found in MetricsAfter.");
}

/// <summary>
/// 可选指标取最大值：按 prefix + 可选 labelFilter 匹配，未找到返回 null（不抛异常）。
/// </summary>
static double? MaxOptionalMetric(JsonElement metrics, string prefix, string? labelFilter = null)
{
    double? maximum = null;
    foreach (var metric in metrics.EnumerateObject())
    {
        if (!metric.Name.StartsWith(prefix, StringComparison.Ordinal))
            continue;
        if (labelFilter is not null && !metric.Name.Contains(labelFilter, StringComparison.Ordinal))
            continue;
        maximum = Math.Max(maximum ?? double.NegativeInfinity, metric.Value.GetDouble());
    }

    return maximum;
}

internal sealed record GateCheck(string Name, bool Passed, string Actual);
internal sealed record GateResult(
    string ReportPath,
    bool Passed,
    DateTimeOffset EvaluatedAtUtc,
    IReadOnlyList<GateCheck> Checks);

internal sealed record GateOptions(
    string ReportPath,
    double MaximumErrorRatePercent,
    double MaximumP95Milliseconds,
    long MaximumJetStreamPending,
    long MaximumOutboxPending,
    double MaximumOutboxOldestAgeSeconds,
    double? MaximumHistoryP95Milliseconds,
    double? MaximumConversationListP95Milliseconds,
    double? MaximumSyncBootstrapP95Milliseconds,
    bool RequireConversationStages,
    double? MaximumGen2Collections,
    double? MaximumGcPauseSeconds,
    double? MaximumAllocatedMegabytes,
    double? MaximumLohHeapBytes,
    double? MaximumPohHeapBytes,
    double? MaximumWorkingSetMegabytes,
    double? MaximumBytesPerConnection,
    string? OutputPath)
{
    public static GateOptions Parse(string[] args)
    {
        string? reportPath = null;
        string? outputPath = null;
        var maxErrorRate = 0d;
        var maxP95 = 300d;
        long maxJetStreamPending = 0;
        long maxOutboxPending = 16;
        var maxOutboxAge = 5d;
        double? maxHistoryP95 = null;
        double? maxConversationListP95 = null;
        double? maxSyncBootstrapP95 = null;
        var requireConversationStages = false;
        // GC / 内存 / 每连接字节 可选阈值：null 时跳过对应检查。
        double? maxGen2Collections = null;
        double? maxGcPauseSeconds = null;
        double? maxAllocatedMb = null;
        double? maxLohHeapBytes = null;
        double? maxPohHeapBytes = null;
        double? maxWorkingSetMb = null;
        double? maxBytesPerConnection = null;

        for (var index = 0; index < args.Length; index++)
        {
            var option = args[index];
            if (option is "--help" or "-h")
                throw new ArgumentException(Usage);
            if (option is "--require-conversation-stages")
            {
                requireConversationStages = true;
                continue;
            }

            if (++index >= args.Length)
                throw new ArgumentException("Missing value for " + option + Environment.NewLine + Usage);
            var value = args[index];
            switch (option)
            {
                case "--report": reportPath = Path.GetFullPath(value); break;
                case "--output": outputPath = Path.GetFullPath(value); break;
                case "--max-error-rate-percent": maxErrorRate = ParseDouble(value, option); break;
                case "--max-p95-ms": maxP95 = ParseDouble(value, option); break;
                case "--max-jetstream-pending": maxJetStreamPending = ParseLong(value, option); break;
                case "--max-outbox-pending": maxOutboxPending = ParseLong(value, option); break;
                case "--max-outbox-oldest-age-seconds": maxOutboxAge = ParseDouble(value, option); break;
                case "--max-history-p95-ms": maxHistoryP95 = ParseDouble(value, option); break;
                case "--max-conversation-list-p95-ms": maxConversationListP95 = ParseDouble(value, option); break;
                case "--max-sync-bootstrap-p95-ms": maxSyncBootstrapP95 = ParseDouble(value, option); break;
                case "--max-gen2-collections": maxGen2Collections = ParseDouble(value, option); break;
                case "--max-gc-pause-seconds": maxGcPauseSeconds = ParseDouble(value, option); break;
                case "--max-allocated-mb": maxAllocatedMb = ParseDouble(value, option); break;
                case "--max-loh-heap-bytes": maxLohHeapBytes = ParseDouble(value, option); break;
                case "--max-poh-heap-bytes": maxPohHeapBytes = ParseDouble(value, option); break;
                case "--max-working-set-mb": maxWorkingSetMb = ParseDouble(value, option); break;
                case "--max-bytes-per-connection": maxBytesPerConnection = ParseDouble(value, option); break;
                default: throw new ArgumentException("Unknown option: " + option + Environment.NewLine + Usage);
            }
        }

        if (reportPath is null || !File.Exists(reportPath))
            throw new FileNotFoundException("A valid --report path is required.", reportPath);
        if (maxErrorRate < 0 || maxP95 <= 0 || maxJetStreamPending < 0 || maxOutboxPending < 0 || maxOutboxAge < 0)
            throw new ArgumentOutOfRangeException(nameof(args), "Gate thresholds must be non-negative.");
        if (maxHistoryP95 is < 0 || maxConversationListP95 is < 0 || maxSyncBootstrapP95 is < 0)
            throw new ArgumentOutOfRangeException(nameof(args), "Stage p95 thresholds must be non-negative.");

        return new GateOptions(
            reportPath,
            maxErrorRate,
            maxP95,
            maxJetStreamPending,
            maxOutboxPending,
            maxOutboxAge,
            maxHistoryP95,
            maxConversationListP95,
            maxSyncBootstrapP95,
            requireConversationStages,
            maxGen2Collections,
            maxGcPauseSeconds,
            maxAllocatedMb,
            maxLohHeapBytes,
            maxPohHeapBytes,
            maxWorkingSetMb,
            maxBytesPerConnection,
            outputPath);
    }

    private static double ParseDouble(string value, string option) =>
        double.TryParse(value, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new ArgumentException("Invalid number for " + option + ": " + value);

    private static long ParseLong(string value, string option) =>
        long.TryParse(value, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new ArgumentException("Invalid integer for " + option + ": " + value);

    private const string Usage =
        "Usage: --report PATH [--output PATH] [--max-error-rate-percent 0] [--max-p95-ms 300] " +
        "[--max-jetstream-pending 0] [--max-outbox-pending 16] [--max-outbox-oldest-age-seconds 5] " +
        "[--max-history-p95-ms N] [--max-conversation-list-p95-ms N] [--max-sync-bootstrap-p95-ms N] " +
        "[--require-conversation-stages] " +
        "[--max-gen2-collections N] [--max-gc-pause-seconds N] [--max-allocated-mb N] " +
        "[--max-loh-heap-bytes N] [--max-poh-heap-bytes N] [--max-working-set-mb N] " +
        "[--max-bytes-per-connection N]";
}
