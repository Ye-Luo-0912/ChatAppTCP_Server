using System.Runtime;
using System.Runtime.InteropServices;
using ChatApp.Realtime.PipelineLoadGenerator.Configuration;
using ChatApp.Realtime.PipelineLoadGenerator.Runtime;

namespace ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;

internal sealed class PipelineLoadReport
{
    public required DateTimeOffset GeneratedAtUtc { get; init; }
    public required PipelineLoadConfiguration Configuration { get; init; }
    public required PipelineLoadEnvironment Environment { get; init; }
    public double NatsPingMs { get; init; }
    public double ElapsedSeconds { get; init; }
    public long Started { get; init; }
    public long Succeeded { get; init; }
    public long Failed { get; init; }
    public double ErrorRatePercent { get; init; }
    public double CompletedPipelinesPerSecond { get; init; }
    public long GeneratorAllocatedBytes { get; init; }
    public long GeneratorWorkingSetBytes { get; init; }
    public required Dictionary<string, LatencySnapshot> Latencies { get; init; }
    public required IReadOnlyList<string> ErrorSamples { get; init; }

    public static PipelineLoadReport Create(
        PipelineLoadOptions options,
        PipelineLoadMeasurement measurement,
        TimeSpan ping,
        TimeSpan elapsed,
        long allocatedBytes)
    {
        var completed = measurement.Succeeded + measurement.Failed;
        return new PipelineLoadReport
        {
            GeneratedAtUtc = DateTimeOffset.UtcNow,
            Configuration = new PipelineLoadConfiguration
            {
                NatsUrl = options.NatsUrl,
                WarmupSeconds = options.Warmup.TotalSeconds,
                DurationSeconds = options.Duration.TotalSeconds,
                Concurrency = options.Concurrency,
                TargetOperationsPerSecond = options.OperationsPerSecond,
                PayloadBytes = options.PayloadBytes,
                OperationTimeoutSeconds = options.OperationTimeout.TotalSeconds,
                BaseUserId = options.BaseUserId
            },
            Environment = new PipelineLoadEnvironment
            {
                Framework = RuntimeInformation.FrameworkDescription,
                OperatingSystem = RuntimeInformation.OSDescription,
                Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
                ProcessorCount = System.Environment.ProcessorCount,
                ServerGc = GCSettings.IsServerGC,
                TotalAvailableMemoryBytes = GC
                    .GetGCMemoryInfo()
                    .TotalAvailableMemoryBytes
            },
            NatsPingMs = ping.TotalMilliseconds,
            ElapsedSeconds = elapsed.TotalSeconds,
            Started = measurement.Started,
            Succeeded = measurement.Succeeded,
            Failed = measurement.Failed,
            ErrorRatePercent = completed == 0
                ? 0
                : measurement.Failed * 100d / completed,
            CompletedPipelinesPerSecond = elapsed <= TimeSpan.Zero
                ? 0
                : measurement.Succeeded / elapsed.TotalSeconds,
            GeneratorAllocatedBytes = allocatedBytes,
            GeneratorWorkingSetBytes = System.Environment.WorkingSet,
            Latencies = new Dictionary<string, LatencySnapshot>(
                StringComparer.Ordinal)
            {
                ["message_publish_ack"] =
                    measurement.MessagePublishAck.Snapshot(),
                ["message_persisted_outbox"] =
                    measurement.MessagePersisted.Snapshot(),
                ["receipt_publish_ack"] =
                    measurement.ReceiptPublishAck.Snapshot(),
                ["receipt_persisted_outbox"] =
                    measurement.ReceiptPersisted.Snapshot(),
                ["history_query"] =
                    measurement.HistoryQuery.Snapshot(),
                ["conversation_list_query"] =
                    measurement.ConversationListQuery.Snapshot(),
                ["conversation_mark_read"] =
                    measurement.ConversationMarkRead.Snapshot(),
                ["sync_bootstrap"] =
                    measurement.SyncBootstrap.Snapshot(),
                ["complete_pipeline"] =
                    measurement.CompletePipeline.Snapshot()
            },
            ErrorSamples = measurement.Errors
        };
    }
}

internal sealed class PipelineLoadConfiguration
{
    public required string NatsUrl { get; init; }
    public double WarmupSeconds { get; init; }
    public double DurationSeconds { get; init; }
    public int Concurrency { get; init; }
    public int TargetOperationsPerSecond { get; init; }
    public int PayloadBytes { get; init; }
    public double OperationTimeoutSeconds { get; init; }
    public long BaseUserId { get; init; }
}

internal sealed class PipelineLoadEnvironment
{
    public required string Framework { get; init; }
    public required string OperatingSystem { get; init; }
    public required string Architecture { get; init; }
    public int ProcessorCount { get; init; }
    public bool ServerGc { get; init; }
    public long TotalAvailableMemoryBytes { get; init; }
}
