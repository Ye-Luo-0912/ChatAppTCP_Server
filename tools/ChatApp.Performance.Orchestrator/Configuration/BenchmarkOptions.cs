using System.Globalization;

namespace ChatApp.Performance.Orchestrator.Configuration;

internal sealed record BenchmarkOptions(
    string RepositoryRoot,
    string RealtimeRepositoryRoot,
    string BuildConfiguration,
    bool BuildBeforeRun,
    int GatewayCount,
    int GatewayBasePort,
    int RealtimePort,
    int RealtimeProcessingConcurrency,
    string NatsUrl,
    int JetStreamReplicas,
    bool SmokeNoopStorage,
    string? RealtimeDatabaseEnvironmentVariable,
    string? GarnetEnvironmentVariable,
    TimeSpan StartupTimeout,
    TimeSpan Warmup,
    TimeSpan Duration,
    TimeSpan SampleInterval,
    string TcpMode,
    int TcpConnections,
    int TcpActiveSenders,
    double TcpMessagesPerSecond,
    TimeSpan TcpDeliveryDrain,
    TimeSpan TcpInactiveHeartbeatInterval,
    double TcpMinimumAcknowledgementRatio,
    double TcpMinimumDeliveryRatio,
    int TcpPayloadBytes,
    int TcpSlowReaders,
    int TcpConnectionsPerSecond,
    string? TcpSlowlorisPhase,
    int TcpSlowlorisDelayMs,
    long? TcpInboundBudgetBytes,
    IReadOnlyList<string> TcpTokens,
    bool TcpBootstrapAuthentication,
    bool TcpCrossGateway,
    long TcpBootstrapUserId,
    long? TcpTargetUserId,
    bool PipelineEnabled,
    int PipelineConcurrency,
    int PipelineOperationsPerSecond,
    int PipelinePayloadBytes,
    TimeSpan PipelineOperationTimeout,
    long PipelineBaseUserId,
    string InboundTransportMode,
    string OutboundSendMode,
    string OutboundQueueMode,
    int OnDemandSendWorkerCount,
    int OnDemandSendBurstLimit,
    string ReportDirectory,
    IReadOnlyList<string> DockerContainers)
{
    public const string Usage =
        "Usage: [--repository-root PATH] [--realtime-root PATH] " +
        "[--configuration Release] [--no-build] [--gateway-count 1] " +
        "[--gateway-base-port 18888] [--realtime-port 18080] " +
        "[--realtime-processing-concurrency 4] " +
        "[--nats-url nats://127.0.0.1:4222] [--jetstream-replicas 1] [--smoke-noop-storage] " +
        "[--realtime-database-environment NAME] [--garnet-environment NAME] " +
        "[--startup-timeout-seconds 60] " +
        "[--warmup-seconds 10] [--duration-seconds 30] [--sample-interval-ms 1000] " +
        "[--tcp-mode connection|heartbeat|chat] [--tcp-connections 100] " +
        "[--tcp-active-senders N] " +
        "[--tcp-messages-per-second 10] [--tcp-payload-bytes 128] " +
        "[--tcp-delivery-drain-seconds 30] [--tcp-min-ack-ratio 0.95] " +
        "[--tcp-inactive-heartbeat-seconds 30] " +
        "[--tcp-min-delivery-ratio 0.90] " +
        "[--tcp-slow-readers 0] [--tcp-connections-per-second N] " +
        "[--tcp-slowloris-phase header|payload] " +
        "[--tcp-slowloris-delay-ms 1000] [--tcp-inbound-budget-bytes N] " +
        "[--tcp-token TOKEN] [--tcp-bootstrap-auth] " +
        "[--tcp-cross-gateway] " +
        "[--tcp-bootstrap-user-id 9300000000] [--tcp-target-user-id ID] " +
        "[--no-pipeline] [--pipeline-concurrency 4] [--pipeline-operations-per-second 0] " +
        "[--pipeline-payload-bytes 128] [--pipeline-operation-timeout-seconds 15] " +
        "[--pipeline-base-user-id 9200000000] " +
        "[--inbound-transport-mode Pipelines|DirectSocket] " +
        "[--outbound-send-mode PersistentSendLoop|OnDemandSendPump|PerSessionDrain] " +
        "[--outbound-queue-mode BoundedChannel|LazySegmented] " +
        "[--on-demand-send-worker-count 0] [--on-demand-send-burst-limit 16] " +
        "[--docker-container NAME] " +
        "[--report-directory .artifacts/performance]";

    public static BenchmarkOptions Parse(string[] args)
    {
        var repositoryRoot = FindRepositoryRoot(Environment.CurrentDirectory);
        string? realtimeRoot = null;
        var configuration = "Release";
        var build = true;
        var gatewayCount = 1;
        var gatewayBasePort = 18_888;
        var realtimePort = 18_080;
        var realtimeProcessingConcurrency = 4;
        var natsUrl = "nats://127.0.0.1:4222";
        var jetStreamReplicas = 1;
        var smokeNoopStorage = false;
        string? realtimeDatabaseEnvironmentVariable = null;
        string? garnetEnvironmentVariable = null;
        var startupTimeoutSeconds = 60;
        var warmupSeconds = 10;
        var durationSeconds = 30;
        var sampleIntervalMs = 1_000;
        var tcpMode = "connection";
        var tcpConnections = 100;
        var tcpActiveSenders = 0;
        var tcpMessagesPerSecond = 10d;
        var tcpDeliveryDrainSeconds = 30;
        var tcpInactiveHeartbeatSeconds = 30;
        var tcpMinimumAcknowledgementRatio = 0.95d;
        var tcpMinimumDeliveryRatio = 0.90d;
        var tcpPayloadBytes = 128;
        var tcpSlowReaders = 0;
        var tcpConnectionsPerSecond = 0;
        string? tcpSlowlorisPhase = null;
        var tcpSlowlorisDelayMs = 1000;
        long? tcpInboundBudgetBytes = null;
        var tcpTokens = new List<string>();
        var tcpBootstrapAuthentication = false;
        var tcpCrossGateway = false;
        long tcpBootstrapUserId = 9_300_000_000;
        long? tcpTargetUserId = null;
        var pipelineEnabled = true;
        var pipelineConcurrency = 4;
        var pipelineOperationsPerSecond = 0;
        var pipelinePayloadBytes = 128;
        var pipelineOperationTimeoutSeconds = 15;
        long pipelineBaseUserId = 9_200_000_000;
        var inboundTransportMode = "DirectSocket";
        // 出站发送模式 A/B：默认 PersistentSendLoop（与历史基线一致），
        // 切换为 OnDemandSendPump 时编排器会注入额外 TcpGateway 配置覆盖项。
        var outboundSendMode = "PersistentSendLoop";
        var outboundQueueMode = "BoundedChannel";
        var onDemandSendWorkerCount = 0;
        var onDemandSendBurstLimit = 16;
        string? reportDirectory = null;
        var dockerContainers = new List<string>();

        for (var index = 0; index < args.Length; index++)
        {
            var option = args[index];
            switch (option)
            {
                case "--help" or "-h":
                    throw new HelpRequestedException();
                case "--no-build":
                    build = false;
                    continue;
                case "--no-pipeline":
                    pipelineEnabled = false;
                    continue;
                case "--smoke-noop-storage":
                    smokeNoopStorage = true;
                    continue;
                case "--tcp-bootstrap-auth":
                    tcpBootstrapAuthentication = true;
                    continue;
                case "--tcp-cross-gateway":
                    tcpCrossGateway = true;
                    continue;
            }

            var value = GetValue(args, ref index, option);
            switch (option)
            {
                case "--repository-root":
                    repositoryRoot = Path.GetFullPath(value);
                    break;
                case "--realtime-root":
                    realtimeRoot = Path.GetFullPath(value);
                    break;
                case "--configuration":
                    configuration = value;
                    break;
                case "--gateway-count":
                    gatewayCount = ParseInt(value, option);
                    break;
                case "--gateway-base-port":
                    gatewayBasePort = ParseInt(value, option);
                    break;
                case "--realtime-port":
                    realtimePort = ParseInt(value, option);
                    break;
                case "--realtime-processing-concurrency":
                    realtimeProcessingConcurrency = ParseInt(value, option);
                    break;
                case "--nats-url":
                    natsUrl = value;
                    break;
                case "--jetstream-replicas":
                    jetStreamReplicas = ParseInt(value, option);
                    break;
                case "--realtime-database-environment":
                    realtimeDatabaseEnvironmentVariable = value;
                    break;
                case "--garnet-environment":
                    garnetEnvironmentVariable = value;
                    break;
                case "--startup-timeout-seconds":
                    startupTimeoutSeconds = ParseInt(value, option);
                    break;
                case "--warmup-seconds":
                    warmupSeconds = ParseInt(value, option);
                    break;
                case "--duration-seconds":
                    durationSeconds = ParseInt(value, option);
                    break;
                case "--sample-interval-ms":
                    sampleIntervalMs = ParseInt(value, option);
                    break;
                case "--tcp-mode":
                    tcpMode = value.ToLowerInvariant();
                    break;
                case "--tcp-connections":
                    tcpConnections = ParseInt(value, option);
                    break;
                case "--tcp-active-senders":
                    tcpActiveSenders = ParseInt(value, option);
                    break;
                case "--tcp-messages-per-second":
                    tcpMessagesPerSecond = ParsePositiveDouble(value, option);
                    break;
                case "--tcp-delivery-drain-seconds":
                    tcpDeliveryDrainSeconds = ParseInt(value, option);
                    break;
                case "--tcp-inactive-heartbeat-seconds":
                    tcpInactiveHeartbeatSeconds = ParseInt(value, option);
                    break;
                case "--tcp-min-ack-ratio":
                    tcpMinimumAcknowledgementRatio = ParseRatio(value, option);
                    break;
                case "--tcp-min-delivery-ratio":
                    tcpMinimumDeliveryRatio = ParseRatio(value, option);
                    break;
                case "--tcp-payload-bytes":
                    tcpPayloadBytes = ParseInt(value, option);
                    break;
                case "--tcp-slow-readers":
                    tcpSlowReaders = ParseInt(value, option);
                    break;
                case "--tcp-connections-per-second":
                    tcpConnectionsPerSecond = ParseInt(value, option);
                    break;
                case "--tcp-slowloris-phase":
                    tcpSlowlorisPhase = value;
                    break;
                case "--tcp-slowloris-delay-ms":
                    tcpSlowlorisDelayMs = ParseInt(value, option);
                    break;
                case "--tcp-inbound-budget-bytes":
                    tcpInboundBudgetBytes = ParseLong(value, option);
                    break;
                case "--tcp-token":
                    tcpTokens.Add(value);
                    break;
                case "--tcp-bootstrap-user-id":
                    tcpBootstrapUserId = ParseLong(value, option);
                    break;
                case "--tcp-target-user-id":
                    tcpTargetUserId = ParseLong(value, option);
                    break;
                case "--pipeline-concurrency":
                    pipelineConcurrency = ParseInt(value, option);
                    break;
                case "--pipeline-operations-per-second":
                    pipelineOperationsPerSecond = ParseInt(value, option);
                    break;
                case "--pipeline-payload-bytes":
                    pipelinePayloadBytes = ParseInt(value, option);
                    break;
                case "--pipeline-operation-timeout-seconds":
                    pipelineOperationTimeoutSeconds = ParseInt(value, option);
                    break;
                case "--pipeline-base-user-id":
                    pipelineBaseUserId = ParseLong(value, option);
                    break;
                case "--inbound-transport-mode":
                    inboundTransportMode = value;
                    break;
                case "--outbound-send-mode":
                    outboundSendMode = value;
                    break;
                case "--outbound-queue-mode":
                    outboundQueueMode = value;
                    break;
                case "--on-demand-send-worker-count":
                    onDemandSendWorkerCount = ParseInt(value, option);
                    break;
                case "--on-demand-send-burst-limit":
                    onDemandSendBurstLimit = ParseInt(value, option);
                    break;
                case "--docker-container":
                    dockerContainers.Add(value);
                    break;
                case "--report-directory":
                    reportDirectory = Path.GetFullPath(value);
                    break;
                default:
                    throw new ArgumentException($"Unknown option: {option}");
            }
        }

        repositoryRoot = Path.GetFullPath(repositoryRoot);
        realtimeRoot = Path.GetFullPath(realtimeRoot ?? Path.Combine(
            repositoryRoot,
            "..",
            "ChatApp.RealtimeServices"));
        reportDirectory = Path.GetFullPath(reportDirectory ?? Path.Combine(
            repositoryRoot,
            ".artifacts",
            "performance"));
        ValidateDirectory(repositoryRoot, "ChatApp.TcpGateway.csproj", "Gateway repository");
        ValidateDirectory(realtimeRoot, "ChatApp.RealtimeServices.slnx", "Realtime repository");
        if (configuration is not ("Debug" or "Release"))
            throw new ArgumentException("--configuration must be Debug or Release.");
        if (gatewayCount is <= 0 or > 16)
            throw new ArgumentOutOfRangeException(nameof(args), "Gateway count must be between 1 and 16.");
        ValidatePort(gatewayBasePort, nameof(gatewayBasePort));
        ValidatePort(gatewayBasePort + gatewayCount - 1, nameof(gatewayCount));
        ValidatePort(realtimePort, nameof(realtimePort));
        if (realtimeProcessingConcurrency is <= 0 or > 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Realtime processing concurrency must be between 1 and 1024.");
        }
        if (Enumerable.Range(gatewayBasePort, gatewayCount).Contains(realtimePort))
            throw new ArgumentException("Realtime port overlaps a Gateway port.");
        ArgumentException.ThrowIfNullOrWhiteSpace(natsUrl);
        if (smokeNoopStorage && pipelineEnabled)
            throw new ArgumentException("--smoke-noop-storage requires --no-pipeline.");
        ValidateEnvironmentVariable(realtimeDatabaseEnvironmentVariable, "Realtime database");
        ValidateEnvironmentVariable(garnetEnvironmentVariable, "Garnet");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(startupTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegative(warmupSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(durationSeconds);
        if (sampleIntervalMs is < 250 or > 60_000)
            throw new ArgumentOutOfRangeException(nameof(args), "Sample interval must be between 250 and 60000 ms.");
        if (tcpMode is not ("connection" or "heartbeat" or "chat" or "slowloris"))
            throw new ArgumentException(
                "TCP mode must be connection, heartbeat, chat, or slowloris.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpConnections);
        if (tcpConnections < gatewayCount)
            throw new ArgumentException("TCP connections cannot be fewer than Gateway instances.");
        ArgumentOutOfRangeException.ThrowIfNegative(tcpActiveSenders);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpMessagesPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegative(tcpDeliveryDrainSeconds);
        ArgumentOutOfRangeException.ThrowIfNegative(tcpInactiveHeartbeatSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpPayloadBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(tcpSlowReaders);
        if (tcpSlowReaders > tcpConnections)
            throw new ArgumentException("TCP slow readers cannot exceed TCP connections.");
        if (tcpSlowReaders != 0 && tcpMode != "chat")
            throw new ArgumentException("TCP slow readers are only valid in chat mode.");
        if (tcpActiveSenders != 0 && tcpMode is not ("heartbeat" or "chat"))
            throw new ArgumentException("--tcp-active-senders requires TCP heartbeat or chat mode.");
        if (tcpActiveSenders > tcpConnections - tcpSlowReaders)
        {
            throw new ArgumentException(
                "TCP active senders cannot exceed non-slow-reader connections.");
        }
        if (tcpMode is "heartbeat" or "chat" && tcpConnections - tcpSlowReaders <= 0)
        {
            throw new ArgumentException(
                "TCP heartbeat/chat requires at least one non-slow-reader connection.");
        }
        if (tcpActiveSenders is > 0 && tcpActiveSenders < gatewayCount)
        {
            throw new ArgumentException(
                "TCP active senders cannot be fewer than Gateway instances.");
        }
        ArgumentOutOfRangeException.ThrowIfNegative(tcpConnectionsPerSecond);
        if (tcpSlowlorisPhase is not null &&
            tcpSlowlorisPhase is not ("header" or "payload"))
            throw new ArgumentException(
                "--tcp-slowloris-phase must be header or payload.");
        if (tcpSlowlorisPhase is not null && tcpMode != "slowloris")
            throw new ArgumentException(
                "--tcp-slowloris-phase requires --tcp-mode slowloris.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpSlowlorisDelayMs);
        if (tcpInboundBudgetBytes is <= 0)
            throw new ArgumentException(
                "--tcp-inbound-budget-bytes must be positive.");
        if (tcpBootstrapAuthentication && tcpMode is not ("heartbeat" or "chat"))
            throw new ArgumentException("--tcp-bootstrap-auth requires TCP heartbeat or chat mode.");
        if (tcpBootstrapAuthentication && tcpTokens.Count != 0)
            throw new ArgumentException("Use either --tcp-token or --tcp-bootstrap-auth, not both.");
        if (tcpBootstrapAuthentication && garnetEnvironmentVariable is null)
            throw new ArgumentException("--tcp-bootstrap-auth requires --garnet-environment.");
        if (tcpBootstrapAuthentication && tcpMode == "chat" && realtimeDatabaseEnvironmentVariable is null)
        {
            throw new ArgumentException(
                "TCP chat bootstrap requires --realtime-database-environment for an isolated fresh performance database.");
        }
        if (tcpBootstrapAuthentication && tcpMode == "chat" && tcpTargetUserId is not null)
        {
            throw new ArgumentException(
                "TCP chat bootstrap builds a non-self ring per Gateway; do not specify --tcp-target-user-id.");
        }
        if (tcpCrossGateway && !tcpBootstrapAuthentication)
        {
            throw new ArgumentException("--tcp-cross-gateway requires --tcp-bootstrap-auth.");
        }
        if (tcpCrossGateway && tcpMode != "chat")
        {
            throw new ArgumentException("--tcp-cross-gateway requires --tcp-mode chat.");
        }
        if (tcpCrossGateway && gatewayCount < 2)
        {
            throw new ArgumentException("--tcp-cross-gateway requires at least two Gateway instances.");
        }
        if (tcpMode is "heartbeat" or "chat" && tcpTokens.Count == 0 && !tcpBootstrapAuthentication)
            throw new ArgumentException($"TCP mode {tcpMode} requires --tcp-token or --tcp-bootstrap-auth.");
        var distinctTcpTokens = tcpTokens.Distinct(StringComparer.Ordinal).ToArray();
        if (tcpMode == "chat" && !tcpBootstrapAuthentication && tcpTargetUserId is null && distinctTcpTokens.Length < 2)
        {
            throw new ArgumentException(
                "TCP chat ring mode requires at least two distinct --tcp-token values or an explicit non-self target.");
        }
        if (tcpBootstrapAuthentication && tcpMode == "chat" &&
            Enumerable.Range(0, gatewayCount).Any(index =>
                Divide(tcpConnections, gatewayCount, index) -
                Divide(tcpSlowReaders, gatewayCount, index) < 2))
        {
            throw new ArgumentException(
                "TCP chat bootstrap requires at least two healthy users per Gateway " +
                "load partition after slow-reader slots are reserved.");
        }
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpBootstrapUserId);
        if (tcpBootstrapAuthentication)
        {
            var bootstrapIdentityCount = checked(tcpConnections - tcpSlowReaders);
            _ = checked(tcpBootstrapUserId + bootstrapIdentityCount - 1L);
        }
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pipelineConcurrency);
        ArgumentOutOfRangeException.ThrowIfNegative(pipelineOperationsPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pipelinePayloadBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pipelineOperationTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pipelineBaseUserId);
        if (inboundTransportMode is not ("Pipelines" or "DirectSocket"))
            throw new ArgumentException(
                "--inbound-transport-mode must be Pipelines or DirectSocket.");
        // 校验出站模式 A/B 参数。三种模式均由 Gateway 配置支持，
        // 编排器统一透传给 Gateway 进程，无需模式特定逻辑。
        if (outboundSendMode is not ("PersistentSendLoop" or "OnDemandSendPump" or "PerSessionDrain"))
            throw new ArgumentException(
                "--outbound-send-mode must be PersistentSendLoop, OnDemandSendPump, or PerSessionDrain.");
        if (outboundQueueMode is not ("BoundedChannel" or "LazySegmented"))
            throw new ArgumentException(
                "--outbound-queue-mode must be BoundedChannel or LazySegmented.");
        ArgumentOutOfRangeException.ThrowIfNegative(onDemandSendWorkerCount);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(onDemandSendBurstLimit, 0);

        return new BenchmarkOptions(
            repositoryRoot,
            realtimeRoot,
            configuration,
            build,
            gatewayCount,
            gatewayBasePort,
            realtimePort,
            realtimeProcessingConcurrency,
            natsUrl,
            jetStreamReplicas,
            smokeNoopStorage,
            realtimeDatabaseEnvironmentVariable,
            garnetEnvironmentVariable,
            TimeSpan.FromSeconds(startupTimeoutSeconds),
            TimeSpan.FromSeconds(warmupSeconds),
            TimeSpan.FromSeconds(durationSeconds),
            TimeSpan.FromMilliseconds(sampleIntervalMs),
            tcpMode,
            tcpConnections,
            tcpActiveSenders,
            tcpMessagesPerSecond,
            TimeSpan.FromSeconds(tcpDeliveryDrainSeconds),
            TimeSpan.FromSeconds(tcpInactiveHeartbeatSeconds),
            tcpMinimumAcknowledgementRatio,
            tcpMinimumDeliveryRatio,
            tcpPayloadBytes,
            tcpSlowReaders,
            tcpConnectionsPerSecond,
            tcpSlowlorisPhase,
            tcpSlowlorisDelayMs,
            tcpInboundBudgetBytes,
            distinctTcpTokens,
            tcpBootstrapAuthentication,
            tcpCrossGateway,
            tcpBootstrapUserId,
            tcpTargetUserId,
            pipelineEnabled,
            pipelineConcurrency,
            pipelineOperationsPerSecond,
            pipelinePayloadBytes,
            TimeSpan.FromSeconds(pipelineOperationTimeoutSeconds),
            pipelineBaseUserId,
            inboundTransportMode,
            outboundSendMode,
            outboundQueueMode,
            onDemandSendWorkerCount,
            onDemandSendBurstLimit,
            reportDirectory,
            dockerContainers.Distinct(StringComparer.Ordinal).ToArray());
    }

    public int GetTcpConnections(int gatewayIndex) =>
        Divide(TcpConnections, GatewayCount, gatewayIndex);

    public int GetTcpSlowReaders(int gatewayIndex) =>
        Divide(TcpSlowReaders, GatewayCount, gatewayIndex);

    public int GetTcpBootstrapIdentityCount() =>
        TcpConnections - TcpSlowReaders;

    public int GetRealtimeProcessingQueueCapacity() =>
        Math.Max(512, checked(RealtimeProcessingConcurrency * 32));

    public int GetRealtimePrefetchMaxMessages() =>
        Math.Max(16, checked(RealtimeProcessingConcurrency * 4));

    public int GetRealtimeMaxAckPending() =>
        Math.Max(256, checked(RealtimeProcessingConcurrency * 4));

    public bool ShouldUseShardedRealtimeRouting() =>
        !SmokeNoopStorage
        && !string.IsNullOrWhiteSpace(GarnetEnvironmentVariable)
        && !string.IsNullOrWhiteSpace(
            Environment.GetEnvironmentVariable(GarnetEnvironmentVariable));

    public int GetEffectiveTcpActiveSenders() =>
        TcpMode is "heartbeat" or "chat"
            ? TcpActiveSenders == 0
                ? TcpConnections - TcpSlowReaders
                : TcpActiveSenders
            : 0;

    public int GetTcpActiveSenders(int gatewayIndex)
    {
        if (TcpMode is not ("heartbeat" or "chat"))
            return 0;

        var eligible = Enumerable.Range(0, GatewayCount)
            .Select(index => GetTcpConnections(index) -
                GetTcpSlowReaders(index))
            .ToArray();
        if (TcpActiveSenders == 0)
            return eligible[gatewayIndex];

        var allocated = new int[GatewayCount];
        var remaining = TcpActiveSenders;
        while (remaining > 0)
        {
            var madeProgress = false;
            for (var index = 0; index < allocated.Length && remaining > 0; index++)
            {
                if (allocated[index] >= eligible[index])
                    continue;
                allocated[index]++;
                remaining--;
                madeProgress = true;
            }

            if (!madeProgress)
                throw new InvalidOperationException("TCP active sender allocation exceeded eligible connections.");
        }

        return allocated[gatewayIndex];
    }

    /// <summary>
    /// The CLI rate is global. Each child gets its deterministic share; zero
    /// remains the explicit unlimited mode. A positive share is never emitted
    /// as zero because the child interprets zero as unlimited.
    /// </summary>
    public int GetTcpConnectionsPerSecond(int gatewayIndex) =>
        TcpConnectionsPerSecond == 0
            ? 0
            : Math.Max(1, Divide(TcpConnectionsPerSecond, GatewayCount, gatewayIndex));

    public TimeSpan GetEstimatedTcpRamp() =>
        TcpConnectionsPerSecond == 0
            ? TimeSpan.Zero
            : TimeSpan.FromSeconds(
                Math.Ceiling(TcpConnections / (double)TcpConnectionsPerSecond));

    public TimeSpan GetBootstrapTokenLifetime()
    {
        // Bootstrap happens before Gateway readiness. Include a complete
        // startup budget, ramp, stabilization, measurement, and a one-hour
        // operational buffer for slow CI/diagnostic hosts.
        var seconds = StartupTimeout.TotalSeconds * (GatewayCount + 1)
                      + GetEstimatedTcpRamp().TotalSeconds
                      + Warmup.TotalSeconds
                      + Duration.TotalSeconds
                      + 3_600;
        return TimeSpan.FromSeconds(Math.Ceiling(seconds));
    }

    private static string FindRepositoryRoot(string startDirectory)
    {
        var current = new DirectoryInfo(Path.GetFullPath(startDirectory));
        while (current is not null)
        {
            if (File.Exists(Path.Combine(current.FullName, "ChatApp.TcpGateway.csproj")))
                return current.FullName;
            current = current.Parent;
        }

        throw new DirectoryNotFoundException(
            "Cannot locate ChatApp.TcpGateway.csproj from the current directory.");
    }

    private static void ValidateDirectory(string directory, string marker, string label)
    {
        if (!File.Exists(Path.Combine(directory, marker)))
            throw new DirectoryNotFoundException($"{label} marker was not found: {Path.Combine(directory, marker)}");
    }

    private static void ValidateEnvironmentVariable(string? name, string label)
    {
        if (name is null)
            return;
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException($"{label} environment variable name cannot be empty.");
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name)))
            throw new ArgumentException($"{label} environment variable is not set: {name}");
    }

    private static void ValidatePort(int port, string name)
    {
        if (port is <= 0 or > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(name, "Port must be between 1 and 65535.");
    }

    private static int Divide(int total, int partitions, int index) =>
        total / partitions + (index < total % partitions ? 1 : 0);

    private static string GetValue(string[] args, ref int index, string option)
    {
        index++;
        if (index >= args.Length)
            throw new ArgumentException($"Missing value for {option}.");
        return args[index];
    }

    private static int ParseInt(string value, string option) =>
        int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new ArgumentException($"{option} requires an integer.");

    private static long ParseLong(string value, string option) =>
        long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new ArgumentException($"{option} requires an integer.");

    private static double ParsePositiveDouble(string value, string option) =>
        double.TryParse(value, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var parsed) &&
        double.IsFinite(parsed) &&
        parsed > 0d
            ? parsed
            : throw new ArgumentException($"{option} requires a positive finite number.");

    private static double ParseRatio(string value, string option) =>
        double.TryParse(value, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var parsed) &&
        double.IsFinite(parsed) &&
        parsed is >= 0d and <= 1d
            ? parsed
            : throw new ArgumentException($"{option} requires a finite number between 0 and 1.");
}

internal sealed class HelpRequestedException : Exception;
