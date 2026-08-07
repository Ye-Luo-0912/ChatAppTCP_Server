namespace ChatApp.TcpGateway.LoadGenerator.Diagnostics;

internal sealed class TcpLoadReport
{
    public required DateTimeOffset GeneratedAtUtc { get; init; }
    public required TcpLoadConfiguration Configuration { get; init; }
    public double ElapsedSeconds { get; init; }
    public double RampSeconds { get; init; }
    public double StabilizationSeconds { get; init; }
    public double MeasurementSeconds { get; init; }
    public double DeliveryDrainElapsedSeconds { get; init; }
    public bool DeliveryDrainCompleted { get; init; }
    public double TotalElapsedSeconds { get; init; }
    public required string TargetStrategy { get; init; }
    public int UniqueAuthenticatedUsers { get; init; }
    public int SuccessfulConnections { get; init; }
    public int FailedConnections { get; init; }
    public int TcpConnectSucceeded { get; init; }
    public int TcpConnectFailed { get; init; }
    public int AuthSucceeded { get; init; }
    public int AuthInvalidToken { get; init; }
    public int AuthDependencyUnavailable { get; init; }
    public int AuthOtherFailure { get; init; }
    public int AuthSucceededWithoutResumeToken { get; init; }
    public int ChatSendFailed { get; init; }
    public int ChatReceiveFailed { get; init; }
    public int ServerClosed { get; init; }
    public int ProtocolRejected { get; init; }
    public int CompletedNormally { get; init; }
    public int PeakActiveConnections { get; init; }
    public long Sent { get; init; }
    public long ExpectedDeliveries { get; init; }
    public long Received { get; init; }
    public long Acknowledged { get; init; }
    public long Rejected { get; init; }
    public long DuplicateAcknowledgements { get; init; }
    public long DuplicateDeliveries { get; init; }
    public int Outstanding { get; init; }
    public long TrackingExpired { get; init; }
    public long TrackingDropped { get; init; }
    public string? RuntimeFailure { get; init; }
    public double SentPerSecond { get; init; }
    public double ReceivedPerSecond { get; init; }
    public required string PrimaryLatencyKind { get; init; }
    public required TcpLatencySnapshot Latency { get; init; }
    public required TcpLatencySnapshot AcknowledgementLatency { get; init; }
    public required TcpLatencySnapshot DeliveryLatency { get; init; }
    public required TcpBucketSnapshot Healthy { get; init; }
    public required TcpBucketSnapshot Slow { get; init; }
    public required TcpGateSnapshot Gate { get; init; }
    public required IReadOnlyList<string> ErrorSamples { get; init; }

    public static TcpLoadReport Create(
        LoadOptions options,
        TimeSpan rampElapsed,
        TimeSpan stabilizationElapsed,
        TimeSpan measurementElapsed,
        TimeSpan deliveryDrainElapsed,
        bool deliveryDrainCompleted,
        TimeSpan totalElapsed,
        TargetPlan targetPlan,
        int successfulConnections,
        int failedConnections,
        int tcpConnectSucceeded,
        int tcpConnectFailed,
        int authSucceeded,
        int authInvalidToken,
        int authDependencyUnavailable,
        int authOtherFailure,
        int authSucceededWithoutResumeToken,
        int chatSendFailed,
        int chatReceiveFailed,
        int serverClosed,
        int protocolRejected,
        int completedNormally,
        int peakActiveConnections,
        long sent,
        long expectedDeliveries,
        long received,
        long acknowledged,
        long rejected,
        long duplicateAcknowledgements,
        long duplicateDeliveries,
        int outstanding,
        long trackingExpired,
        long trackingDropped,
        LatencyHistogramSnapshot latency,
        LatencyHistogramSnapshot acknowledgementLatency,
        LatencyHistogramSnapshot deliveryLatency,
        LatencyHistogramSnapshot healthyLatency,
        LatencyHistogramSnapshot slowLatency,
        int healthyCount,
        int slowCount,
        LoadGateEvaluation gate,
        string? runtimeFailure,
        IReadOnlyList<string> errors)
    {
        var measurementSeconds = measurementElapsed.TotalSeconds;
        return new TcpLoadReport
        {
            GeneratedAtUtc = DateTimeOffset.UtcNow,
            Configuration = new TcpLoadConfiguration
            {
                Host = options.Host,
                Port = options.Port,
                Connections = options.Connections,
                DurationSeconds = options.Duration.TotalSeconds,
                Mode = options.Mode.ToString(),
                AccessTokenCount = options.AccessTokens.Count,
                TargetUserId = options.TargetUserId,
                ActiveSenders = options.ActiveSenders,
                MessagesPerSecond = options.MessagesPerSecond,
                PayloadBytes = options.PayloadBytes,
                SlowReaders = options.SlowReaders,
                ConnectionsPerSecond = options.ConnectionsPerSecond,
                StabilizationSeconds = options.Stabilization.TotalSeconds,
                ConnectTimeoutSeconds = options.ConnectTimeout.TotalSeconds,
                MaxInflight = options.MaxInflight,
                InflightTtlSeconds = options.InflightTtl.TotalSeconds,
                DeliveryDrainSeconds = options.DeliveryDrain.TotalSeconds,
                InactiveHeartbeatSeconds = options.InactiveHeartbeatInterval.TotalSeconds,
                MinimumAcknowledgementRatio = options.MinimumAcknowledgementRatio,
                MinimumDeliveryRatio = options.MinimumDeliveryRatio,
                SlowlorisPhase = options.SlowlorisPhase?.ToString(),
                SlowlorisDelayMs = options.SlowlorisDelayMs
            },
            ElapsedSeconds = measurementSeconds,
            RampSeconds = rampElapsed.TotalSeconds,
            StabilizationSeconds = stabilizationElapsed.TotalSeconds,
            MeasurementSeconds = measurementSeconds,
            DeliveryDrainElapsedSeconds = deliveryDrainElapsed.TotalSeconds,
            DeliveryDrainCompleted = deliveryDrainCompleted,
            TotalElapsedSeconds = totalElapsed.TotalSeconds,
            TargetStrategy = targetPlan.Strategy,
            UniqueAuthenticatedUsers = targetPlan.UniqueUsers,
            SuccessfulConnections = successfulConnections,
            FailedConnections = failedConnections,
            TcpConnectSucceeded = tcpConnectSucceeded,
            TcpConnectFailed = tcpConnectFailed,
            AuthSucceeded = authSucceeded,
            AuthInvalidToken = authInvalidToken,
            AuthDependencyUnavailable = authDependencyUnavailable,
            AuthOtherFailure = authOtherFailure,
            AuthSucceededWithoutResumeToken = authSucceededWithoutResumeToken,
            ChatSendFailed = chatSendFailed,
            ChatReceiveFailed = chatReceiveFailed,
            ServerClosed = serverClosed,
            ProtocolRejected = protocolRejected,
            CompletedNormally = completedNormally,
            PeakActiveConnections = peakActiveConnections,
            Sent = sent,
            ExpectedDeliveries = expectedDeliveries,
            Received = received,
            Acknowledged = acknowledged,
            Rejected = rejected,
            DuplicateAcknowledgements = duplicateAcknowledgements,
            DuplicateDeliveries = duplicateDeliveries,
            Outstanding = outstanding,
            TrackingExpired = trackingExpired,
            TrackingDropped = trackingDropped,
            RuntimeFailure = runtimeFailure,
            SentPerSecond = measurementSeconds <= 0 ? 0 : sent / measurementSeconds,
            ReceivedPerSecond = measurementSeconds <= 0 ? 0 : received / measurementSeconds,
            PrimaryLatencyKind = options.Mode == LoadMode.Chat
                ? "peer-delivery"
                : "operation-round-trip",
            Latency = TcpLatencySnapshot.Create(latency),
            AcknowledgementLatency = TcpLatencySnapshot.Create(acknowledgementLatency),
            DeliveryLatency = TcpLatencySnapshot.Create(deliveryLatency),
            Healthy = TcpBucketSnapshot.Create(healthyCount, healthyLatency),
            Slow = TcpBucketSnapshot.Create(slowCount, slowLatency),
            Gate = new TcpGateSnapshot
            {
                Passed = gate.Passed,
                Failures = gate.Failures
            },
            ErrorSamples = errors
        };
    }
}

internal sealed class TcpLoadConfiguration
{
    public required string Host { get; init; }
    public int Port { get; init; }
    public int Connections { get; init; }
    public double DurationSeconds { get; init; }
    public required string Mode { get; init; }
    public int AccessTokenCount { get; init; }
    public long? TargetUserId { get; init; }
    public int ActiveSenders { get; init; }
    public double MessagesPerSecond { get; init; }
    public int PayloadBytes { get; init; }
    public int SlowReaders { get; init; }
    public int ConnectionsPerSecond { get; init; }
    public double StabilizationSeconds { get; init; }
    public double ConnectTimeoutSeconds { get; init; }
    public int MaxInflight { get; init; }
    public double InflightTtlSeconds { get; init; }
    public double DeliveryDrainSeconds { get; init; }
    public double InactiveHeartbeatSeconds { get; init; }
    public double MinimumAcknowledgementRatio { get; init; }
    public double MinimumDeliveryRatio { get; init; }
    public string? SlowlorisPhase { get; init; }
    public int SlowlorisDelayMs { get; init; }
}

internal sealed class TcpLatencySnapshot
{
    public long Count { get; init; }
    public double AverageMs { get; init; }
    public double P50Ms { get; init; }
    public double P95Ms { get; init; }
    public double P99Ms { get; init; }
    public double MaximumMs { get; init; }

    public static TcpLatencySnapshot Create(LatencyHistogramSnapshot snapshot) =>
        new()
        {
            Count = snapshot.Count,
            AverageMs = snapshot.AverageMs,
            P50Ms = snapshot.P50Ms,
            P95Ms = snapshot.P95Ms,
            P99Ms = snapshot.P99Ms,
            MaximumMs = snapshot.MaximumMs
        };
}

/// <summary>
/// 健康/慢连接分桶统计：记录该桶内的连接数与延迟分位数。
/// 用于 slow-consumer 场景独立评估慢消费者对健康连接的影响。
/// </summary>
internal sealed class TcpBucketSnapshot
{
    public int Connections { get; init; }
    public long Samples { get; init; }
    public double AverageMs { get; init; }
    public double P50Ms { get; init; }
    public double P95Ms { get; init; }
    public double P99Ms { get; init; }
    public double MaximumMs { get; init; }

    public static TcpBucketSnapshot Create(
        int connections,
        LatencyHistogramSnapshot snapshot) =>
        new()
        {
            Connections = connections,
            Samples = snapshot.Count,
            AverageMs = snapshot.AverageMs,
            P50Ms = snapshot.P50Ms,
            P95Ms = snapshot.P95Ms,
            P99Ms = snapshot.P99Ms,
            MaximumMs = snapshot.MaximumMs
        };
}

internal sealed class TcpGateSnapshot
{
    public bool Passed { get; init; }
    public required IReadOnlyList<string> Failures { get; init; }
}
