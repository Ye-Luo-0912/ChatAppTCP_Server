using System.Runtime;
using System.Runtime.InteropServices;
using ChatApp.Performance.Orchestrator.Configuration;

namespace ChatApp.Performance.Orchestrator.Diagnostics;

internal sealed class BenchmarkReport
{
    public required DateTimeOffset StartedAtUtc { get; init; }
    public required DateTimeOffset CompletedAtUtc { get; init; }
    public bool Succeeded { get; init; }
    public required BenchmarkRunValidity Validity { get; init; }
    public required BenchmarkConfiguration Configuration { get; init; }
    public required BenchmarkEnvironment Environment { get; init; }
    public required BenchmarkProvenance Provenance { get; init; }
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
    public int RealtimeProcessingConcurrency { get; init; }
    public int RealtimeProcessingQueueCapacity { get; init; }
    public int RealtimePrefetchMaxMessages { get; init; }
    public int RealtimeMaxAckPending { get; init; }
    public bool RealtimeShardedRouting { get; init; }
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
    public int TcpActiveSenders { get; init; }
    public bool TcpCrossGateway { get; init; }
    public double TcpMessagesPerSecond { get; init; }
    public double TcpDeliveryDrainSeconds { get; init; }
    public double TcpInactiveHeartbeatSeconds { get; init; }
    public double TcpMinimumAcknowledgementRatio { get; init; }
    public double TcpMinimumDeliveryRatio { get; init; }
    public int TcpPayloadBytes { get; init; }
    public int TcpSlowReaders { get; init; }
    public int TcpConnectionsPerSecond { get; init; }
    public int TcpTokenCount { get; init; }
    public bool TcpBootstrapAuthentication { get; init; }
    public double TcpBootstrapTokenLifetimeSeconds { get; init; }
    public double EstimatedTcpRampSeconds { get; init; }
    public bool PipelineEnabled { get; init; }
    public int PipelineConcurrency { get; init; }
    public int PipelineOperationsPerSecond { get; init; }
    public int PipelinePayloadBytes { get; init; }
    public long PipelineBaseUserId { get; init; }
    public required string InboundTransportMode { get; init; }
    public required string OutboundSendMode { get; init; }
    public required string OutboundQueueMode { get; init; }
    public int OnDemandSendWorkerCount { get; init; }
    public int OnDemandSendBurstLimit { get; init; }
    public required IReadOnlyList<string> DockerContainers { get; init; }

    public static BenchmarkConfiguration Create(
        BenchmarkOptions options,
        int effectiveTcpTokenCount,
        TimeSpan? bootstrapTokenLifetime) => new()
    {
        RepositoryRoot = options.RepositoryRoot,
        RealtimeRepositoryRoot = options.RealtimeRepositoryRoot,
        BuildConfiguration = options.BuildConfiguration,
        BuildBeforeRun = options.BuildBeforeRun,
        GatewayCount = options.GatewayCount,
        GatewayBasePort = options.GatewayBasePort,
        RealtimePort = options.RealtimePort,
        RealtimeProcessingConcurrency = options.RealtimeProcessingConcurrency,
        RealtimeProcessingQueueCapacity = options.GetRealtimeProcessingQueueCapacity(),
        RealtimePrefetchMaxMessages = options.GetRealtimePrefetchMaxMessages(),
        RealtimeMaxAckPending = options.GetRealtimeMaxAckPending(),
        RealtimeShardedRouting = options.ShouldUseShardedRealtimeRouting(),
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
        TcpActiveSenders = options.GetEffectiveTcpActiveSenders(),
        TcpCrossGateway = options.TcpCrossGateway,
        TcpMessagesPerSecond = options.TcpMessagesPerSecond,
        TcpDeliveryDrainSeconds = options.TcpDeliveryDrain.TotalSeconds,
        TcpInactiveHeartbeatSeconds = options.TcpInactiveHeartbeatInterval.TotalSeconds,
        TcpMinimumAcknowledgementRatio = options.TcpMinimumAcknowledgementRatio,
        TcpMinimumDeliveryRatio = options.TcpMinimumDeliveryRatio,
        TcpPayloadBytes = options.TcpPayloadBytes,
        TcpSlowReaders = options.TcpSlowReaders,
        TcpConnectionsPerSecond = options.TcpConnectionsPerSecond,
        TcpTokenCount = effectiveTcpTokenCount,
        TcpBootstrapAuthentication = options.TcpBootstrapAuthentication,
        TcpBootstrapTokenLifetimeSeconds = bootstrapTokenLifetime?.TotalSeconds ?? 0,
        EstimatedTcpRampSeconds = options.GetEstimatedTcpRamp().TotalSeconds,
        PipelineEnabled = options.PipelineEnabled,
        PipelineConcurrency = options.PipelineConcurrency,
        PipelineOperationsPerSecond = options.PipelineOperationsPerSecond,
        PipelinePayloadBytes = options.PipelinePayloadBytes,
        PipelineBaseUserId = options.PipelineBaseUserId,
        InboundTransportMode = options.InboundTransportMode,
        OutboundSendMode = options.OutboundSendMode,
        OutboundQueueMode = options.OutboundQueueMode,
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
    // item 八：进程退出/OOM 归因分类，避免将 exit 137 一律判成 Gateway 内存泄漏。
    public OomClassification OomClassification { get; init; }
    public string? OomEvidence { get; init; }
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
    public long TcpConnectFailed { get; init; }
    public long AuthInvalidToken { get; init; }
    public long AuthDependencyUnavailable { get; init; }
    public long AuthOtherFailure { get; init; }
    public long AuthSucceededWithoutResumeToken { get; init; }
    public long ServerClosed { get; init; }
    public long ProtocolRejected { get; init; }
    public double ErrorRatePercent { get; init; }
    public double ThroughputPerSecond { get; init; }
    public double P50Milliseconds { get; init; }
    public double P95Milliseconds { get; init; }
    public double P99Milliseconds { get; init; }
    // 连接分桶：健康/慢连接延迟独立统计（slow-consumer 场景）。
    public int PeakActiveConnections { get; init; }
    public double HealthyP95Milliseconds { get; init; }
    public double SlowP95Milliseconds { get; init; }
    // 消息成功数独立于连接数，避免连接数与消息成功数混用。
    public long MessagesSent { get; init; }
    public long MessagesExpectedDeliveries { get; init; }
    public long MessagesAcknowledged { get; init; }
    public long MessagesRejected { get; init; }
    public long MessagesReceived { get; init; }
    public double RampSeconds { get; init; }
    public double StabilizationSeconds { get; init; }
    public double MeasurementSeconds { get; init; }
    public string? TargetStrategy { get; init; }
    public int UniqueAuthenticatedUsers { get; init; }
    public int ActiveSenders { get; init; }
    public required string SourceReport { get; init; }
}

internal sealed class BenchmarkRunValidity
{
    public bool IsValid { get; init; }
    public DateTimeOffset? LoadStartedAtUtc { get; init; }
    public DateTimeOffset? MeasurementStartedAtUtc { get; init; }
    public DateTimeOffset? MeasurementCompletedAtUtc { get; init; }
    public double ExpectedMeasurementSeconds { get; init; }
    public double ObservedMeasurementSeconds { get; init; }
    public required string MeasurementBoundarySource { get; init; }
    public int ExpectedLoadProcesses { get; init; }
    public int CompletedLoadProcesses { get; init; }
    public bool ServicesAliveThroughMeasurement { get; init; }
    public int ExpectedProcessSamplesPerProcess { get; init; }
    public int MinimumProcessSamples { get; init; }
    public double ProcessSamplingCoveragePercent { get; init; }
    public int ExpectedPrometheusSamples { get; init; }
    public int PrometheusSamples { get; init; }
    public double PrometheusSamplingCoveragePercent { get; init; }
    public required IReadOnlyList<ResourceSamplingSeriesCoverage> ResourceSamplingSeriesCoverage { get; init; }
    public required IReadOnlyList<string> InvalidReasons { get; init; }
}

internal sealed class BenchmarkProvenance
{
    public required string OrchestratorVersion { get; init; }
    public required GitRepositorySnapshot GatewayRepository { get; init; }
    public required GitRepositorySnapshot RealtimeRepository { get; init; }
    public required BenchmarkSnapshotBinding SnapshotBinding { get; init; }
}

internal sealed class BenchmarkSnapshotBinding
{
    public bool Required { get; init; }
    public bool Complete { get; init; }
    public string? RunId { get; init; }
    public string? RunRoot { get; init; }
    public string? SourceArchivePath { get; init; }
    public string? SourceArchiveSha256 { get; init; }
    public string? CanonicalFeedArchivePath { get; init; }
    public string? CanonicalFeedArchiveSha256 { get; init; }
    public string? DotnetExecutablePath { get; init; }
    public string? DotnetExecutableSha256 { get; init; }
}

internal sealed class GitRepositorySnapshot
{
    public required string CommitSha { get; init; }
    public required string Branch { get; init; }
    public bool WorkingTreeDirty { get; init; }
    public string? CaptureError { get; init; }
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
    // item 八：Linux /proc 与 cgroup-v2 内存压力信号（0 = 不可用）。
    public long MaximumVmRssBytes { get; init; }
    public long MaximumVmHwmBytes { get; init; }
    public long MaximumCgroupMemoryCurrentBytes { get; init; }
    public long MaximumCgroupMemoryPeakBytes { get; init; }
    public long CgroupOomEvents { get; init; }
    public long CgroupOomKillEvents { get; init; }
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

internal sealed record ProcessTimeline(
    string Label,
    int ProcessId,
    IReadOnlyList<(long TimestampTicks, long WorkingSetBytes)> WorkingSetSamples);

internal sealed record DockerTimeline(
    string Container,
    IReadOnlyList<long> SampleTimestamps);

internal sealed class ResourceSamplingSeriesCoverage
{
    public required string Kind { get; init; }
    public required string Series { get; init; }
    public int SamplesInMeasurement { get; init; }
    public int ExpectedSamplesInMeasurement { get; init; }
    public double CoveragePercent { get; init; }
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

/// <summary>
/// item 八：进程退出归因。区分托管的 .NET OOM、cgroup 控制器 OOM、内核 OOM
/// 与无法归因的 SIGKILL，避免把 exit 137 一律当作“Gateway 内存泄漏”。
/// </summary>
internal enum OomClassification
{
    /// <summary>进程正常退出或被编排器停止，无 OOM 归因。</summary>
    None,
    /// <summary>进程日志出现 .NET 托管 OOM（OutOfMemoryException / Out of memory）。</summary>
    ManagedOOM,
    /// <summary>cgroup-v2 memory.events 的 oom_kill 计数增加，被 cgroup 限制击杀。</summary>
    KilledByCgroupOOM,
    /// <summary>exit 137 但无 cgroup/日志证据，推断为内核级或外部 SIGKILL。</summary>
    KilledByKernelOOM,
    /// <summary>exit 137 但无法归因到上述任一明确来源。</summary>
    SIGKILLUnknown,
}
