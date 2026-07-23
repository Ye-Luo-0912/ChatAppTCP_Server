namespace ChatApp.TcpGateway.LoadGenerator.Diagnostics;

internal sealed class TcpLoadReport
{
    public required DateTimeOffset GeneratedAtUtc { get; init; }
    public required TcpLoadConfiguration Configuration { get; init; }
    public double ElapsedSeconds { get; init; }
    public int SuccessfulConnections { get; init; }
    public int FailedConnections { get; init; }
    public long Sent { get; init; }
    public long Received { get; init; }
    public long Acknowledged { get; init; }
    public long Rejected { get; init; }
    public double SentPerSecond { get; init; }
    public double ReceivedPerSecond { get; init; }
    public required TcpLatencySnapshot Latency { get; init; }
    public required IReadOnlyList<string> ErrorSamples { get; init; }

    public static TcpLoadReport Create(
        LoadOptions options,
        TimeSpan elapsed,
        int successfulConnections,
        int failedConnections,
        long sent,
        long received,
        long acknowledged,
        long rejected,
        double[] sortedLatencies,
        IReadOnlyList<string> errors)
    {
        var elapsedSeconds = elapsed.TotalSeconds;
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
                MessagesPerSecond = options.MessagesPerSecond,
                PayloadBytes = options.PayloadBytes,
                SlowReaders = options.SlowReaders
            },
            ElapsedSeconds = elapsedSeconds,
            SuccessfulConnections = successfulConnections,
            FailedConnections = failedConnections,
            Sent = sent,
            Received = received,
            Acknowledged = acknowledged,
            Rejected = rejected,
            SentPerSecond = elapsedSeconds <= 0 ? 0 : sent / elapsedSeconds,
            ReceivedPerSecond = elapsedSeconds <= 0 ? 0 : received / elapsedSeconds,
            Latency = TcpLatencySnapshot.Create(sortedLatencies),
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
    public int MessagesPerSecond { get; init; }
    public int PayloadBytes { get; init; }
    public int SlowReaders { get; init; }
}

internal sealed class TcpLatencySnapshot
{
    public int Count { get; init; }
    public double AverageMs { get; init; }
    public double P50Ms { get; init; }
    public double P95Ms { get; init; }
    public double P99Ms { get; init; }
    public double MaximumMs { get; init; }

    public static TcpLatencySnapshot Create(double[] sortedValues)
    {
        if (sortedValues.Length == 0)
            return new TcpLatencySnapshot();

        return new TcpLatencySnapshot
        {
            Count = sortedValues.Length,
            AverageMs = sortedValues.Average(),
            P50Ms = Percentile(sortedValues, 0.50),
            P95Ms = Percentile(sortedValues, 0.95),
            P99Ms = Percentile(sortedValues, 0.99),
            MaximumMs = sortedValues[^1]
        };
    }

    private static double Percentile(double[] sortedValues, double percentile)
    {
        var index = (int)Math.Ceiling(percentile * sortedValues.Length) - 1;
        return sortedValues[Math.Clamp(index, 0, sortedValues.Length - 1)];
    }
}
