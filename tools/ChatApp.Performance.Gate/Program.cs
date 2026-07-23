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
        "[--require-conversation-stages]";
}
