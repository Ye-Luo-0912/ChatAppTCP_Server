using System.Runtime;
using System.Runtime.InteropServices;
using ChatApp.Performance.Orchestrator.Configuration;

namespace ChatApp.Performance.Orchestrator.Diagnostics;

internal sealed class BenchmarkReport
{
    public required DateTimeOffset StartedAtUtc { get; init; }
    public required DateTimeOffset CompletedAtUtc { get; init; }
    public bool Succeeded { get; init; }
    public required BenchmarkConfiguration Configuration { get; init; }
    public required BenchmarkEnvironment Environment { get; init; }
    public required IReadOnlyList<BenchmarkProcessResult> Processes { get; init; }
    public required IReadOnlyList<LoadResultSummary> LoadResults { get; init; }
    public required IReadOnlyList<ProcessResourceSummary> ProcessResources { get; init; }
    public required IReadOnlyList<DockerResourceSummary> DockerResources { get; init; }
    public required Dictionary<string, double> MetricsBefore { get; init; }
    public required Dictionary<string, double> MetricsAfter { get; init; }
    public required Dictionary<string, double> MetricDeltas { get; init; }
    public required IReadOnlyList<PrometheusMetricTrend> MetricTrends { get; init; }
    public required IReadOnlyList<string> Artifacts { get; init; }
    public required IReadOnlyList<string> Errors { get; init; }
}

internal sealed class BenchmarkConfiguration
{
    public required string RepositoryRoot { get; init; }
    public required string RealtimeRepositoryRoot { get; init; }
    public required string BuildConfiguration { get; init; }
    public bool BuildBeforeRun { get; init; }
    public int GatewayCount { get; init; }
    public int GatewayBasePort { get; init; }
    public int RealtimePort { get; init; }
    public required string NatsUrl { get; init; }
    public int JetStreamReplicas { get; init; }
    public bool SmokeNoopStorage { get; init; }
    public string? RealtimeDatabaseEnvironmentVariable { get; init; }
    public string? GarnetEnvironmentVariable { get; init; }
    public double WarmupSeconds { get; init; }
    public double DurationSeconds { get; init; }
    public double SampleIntervalMilliseconds { get; init; }
    public required string TcpMode { get; init; }
    public int TcpConnections { get; init; }
    public int TcpMessagesPerSecond { get; init; }
    public int TcpPayloadBytes { get; init; }
    public int TcpSlowReaders { get; init; }
    public int TcpTokenCount { get; init; }
    public bool PipelineEnabled { get; init; }
    public int PipelineConcurrency { get; init; }
    public int PipelineOperationsPerSecond { get; init; }
    public int PipelinePayloadBytes { get; init; }
    public long PipelineBaseUserId { get; init; }
    public required string OutboundSendMode { get; init; }
    public int OnDemandSendWorkerCount { get; init; }
    public int OnDemandSendBurstLimit { get; init; }
    public required IReadOnlyList<string> DockerContainers { get; init; }

    public static BenchmarkConfiguration Create(BenchmarkOptions options) => new()
    {
        RepositoryRoot = options.RepositoryRoot,
        RealtimeRepositoryRoot = options.RealtimeRepositoryRoot,
        BuildConfiguration = options.BuildConfiguration,
        BuildBeforeRun = options.BuildBeforeRun,
        GatewayCount = options.GatewayCount,
        GatewayBasePort = options.GatewayBasePort,
        RealtimePort = options.RealtimePort,
        NatsUrl = options.NatsUrl,
        JetStreamReplicas = options.JetStreamReplicas,
        SmokeNoopStorage = options.SmokeNoopStorage,
        RealtimeDatabaseEnvironmentVariable = options.RealtimeDatabaseEnvironmentVariable,
        GarnetEnvironmentVariable = options.GarnetEnvironmentVariable,
        WarmupSeconds = options.Warmup.TotalSeconds,
        DurationSeconds = options.Duration.TotalSeconds,
        SampleIntervalMilliseconds = options.SampleInterval.TotalMilliseconds,
        TcpMode = options.TcpMode,
        TcpConnections = options.TcpConnections,
        TcpMessagesPerSecond = options.TcpMessagesPerSecond,
        TcpPayloadBytes = options.TcpPayloadBytes,
        TcpSlowReaders = options.TcpSlowReaders,
        TcpTokenCount = options.TcpTokens.Count,
        PipelineEnabled = options.PipelineEnabled,
        PipelineConcurrency = options.PipelineConcurrency,
        PipelineOperationsPerSecond = options.PipelineOperationsPerSecond,
        PipelinePayloadBytes = options.PipelinePayloadBytes,
        PipelineBaseUserId = options.PipelineBaseUserId,
        OutboundSendMode = options.OutboundSendMode,
        OnDemandSendWorkerCount = options.OnDemandSendWorkerCount,
        OnDemandSendBurstLimit = options.OnDemandSendBurstLimit,
        DockerContainers = options.DockerContainers
    };
}

internal sealed class BenchmarkEnvironment
{
    public required string Framework { get; init; }
    public required string OperatingSystem { get; init; }
    public required string Architecture { get; init; }
    public int ProcessorCount { get; init; }
    public bool ServerGc { get; init; }
    public long TotalAvailableMemoryBytes { get; init; }

    public static BenchmarkEnvironment Create() => new()
    {
        Framework = RuntimeInformation.FrameworkDescription,
        OperatingSystem = RuntimeInformation.OSDescription,
        Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
        ProcessorCount = Environment.ProcessorCount,
        ServerGc = GCSettings.IsServerGC,
        TotalAvailableMemoryBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes
    };
}

internal sealed class BenchmarkProcessResult
{
    public required string Label { get; init; }
    public required string Kind { get; init; }
    public int ProcessId { get; init; }
    public int? ExitCode { get; init; }
    public bool StoppedByOrchestrator { get; init; }
    public required string StandardOutputPath { get; init; }
    public required string StandardErrorPath { get; init; }
    public required IReadOnlyList<string> StandardOutputTail { get; init; }
    public required IReadOnlyList<string> StandardErrorTail { get; init; }
}

internal sealed class LoadResultSummary
{
    public required string Name { get; init; }
    public required string Kind { get; init; }
    public long Succeeded { get; init; }
    public long Failed { get; init; }
    public double ErrorRatePercent { get; init; }
    public double ThroughputPerSecond { get; init; }
    public double P50Milliseconds { get; init; }
    public double P95Milliseconds { get; init; }
    public double P99Milliseconds { get; init; }
    public required string SourceReport { get; init; }
}

internal sealed class ProcessResourceSummary
{
    public required string Label { get; init; }
    public int ProcessId { get; init; }
    public int Samples { get; init; }
    public double AverageCpuPercent { get; init; }
    public double MaximumCpuPercent { get; init; }
    public double TotalCpuSeconds { get; init; }
    public long FirstWorkingSetBytes { get; init; }
    public long LastWorkingSetBytes { get; init; }
    public long MinimumWorkingSetBytes { get; init; }
    public long AverageWorkingSetBytes { get; init; }
    public long MaximumWorkingSetBytes { get; init; }
    public long FirstPrivateMemoryBytes { get; init; }
    public long LastPrivateMemoryBytes { get; init; }
    public long MinimumPrivateMemoryBytes { get; init; }
    public long AveragePrivateMemoryBytes { get; init; }
    public long MaximumPrivateMemoryBytes { get; init; }
    public int MaximumThreadCount { get; init; }
    public int MaximumHandleCount { get; init; }
}

internal sealed class DockerResourceSummary
{
    public required string Container { get; init; }
    public int Samples { get; init; }
    public double AverageCpuPercent { get; init; }
    public double MaximumCpuPercent { get; init; }
    public long FirstMemoryBytes { get; init; }
    public long LastMemoryBytes { get; init; }
    public long MinimumMemoryBytes { get; init; }
    public long AverageMemoryBytes { get; init; }
    public long MaximumMemoryBytes { get; init; }
    public string? LastNetworkIo { get; init; }
    public string? LastBlockIo { get; init; }
}

internal sealed class PrometheusMetricTrend
{
    public required string Series { get; init; }
    public int Samples { get; init; }
    public double FirstValue { get; init; }
    public double LastValue { get; init; }
    public double MinimumValue { get; init; }
    public double MaximumValue { get; init; }
    public double Delta { get; init; }
}