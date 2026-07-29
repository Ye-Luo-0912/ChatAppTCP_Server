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
    int TcpMessagesPerSecond,
    int TcpPayloadBytes,
    int TcpSlowReaders,
    IReadOnlyList<string> TcpTokens,
    bool TcpBootstrapAuthentication,
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
    int OnDemandSendWorkerCount,
    int OnDemandSendBurstLimit,
    string ReportDirectory,
    IReadOnlyList<string> DockerContainers)
{
    public const string Usage =
        "Usage: [--repository-root PATH] [--realtime-root PATH] " +
        "[--configuration Release] [--no-build] [--gateway-count 1] " +
        "[--gateway-base-port 18888] [--realtime-port 18080] " +
        "[--nats-url nats://127.0.0.1:4222] [--jetstream-replicas 1] [--smoke-noop-storage] " +
        "[--realtime-database-environment NAME] [--garnet-environment NAME] " +
        "[--startup-timeout-seconds 60] " +
        "[--warmup-seconds 10] [--duration-seconds 30] [--sample-interval-ms 1000] " +
        "[--tcp-mode connection|heartbeat|chat] [--tcp-connections 100] " +
        "[--tcp-messages-per-second 10] [--tcp-payload-bytes 128] " +
        "[--tcp-slow-readers 0] [--tcp-token TOKEN] [--tcp-bootstrap-auth] " +
        "[--tcp-bootstrap-user-id 9300000000] [--tcp-target-user-id ID] " +
        "[--no-pipeline] [--pipeline-concurrency 4] [--pipeline-operations-per-second 0] " +
        "[--pipeline-payload-bytes 128] [--pipeline-operation-timeout-seconds 15] " +
        "[--pipeline-base-user-id 9200000000] " +
        "[--inbound-transport-mode Pipelines|DirectSocket] " +
        "[--outbound-send-mode PersistentSendLoop|OnDemandSendPump|PerSessionDrain] " +
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
        var tcpMessagesPerSecond = 10;
        var tcpPayloadBytes = 128;
        var tcpSlowReaders = 0;
        var tcpTokens = new List<string>();
        var tcpBootstrapAuthentication = false;
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
                case "--tcp-messages-per-second":
                    tcpMessagesPerSecond = ParseInt(value, option);
                    break;
                case "--tcp-payload-bytes":
                    tcpPayloadBytes = ParseInt(value, option);
                    break;
                case "--tcp-slow-readers":
                    tcpSlowReaders = ParseInt(value, option);
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
        if (tcpMode is not ("connection" or "heartbeat" or "chat"))
            throw new ArgumentException("TCP mode must be connection, heartbeat, or chat.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpConnections);
        if (tcpConnections < gatewayCount)
            throw new ArgumentException("TCP connections cannot be fewer than Gateway instances.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpMessagesPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpPayloadBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(tcpSlowReaders);
        if (tcpSlowReaders > tcpConnections)
            throw new ArgumentException("TCP slow readers cannot exceed TCP connections.");
        if (tcpBootstrapAuthentication && tcpMode is not ("heartbeat" or "chat"))
            throw new ArgumentException("--tcp-bootstrap-auth requires TCP heartbeat or chat mode.");
        if (tcpBootstrapAuthentication && tcpTokens.Count != 0)
            throw new ArgumentException("Use either --tcp-token or --tcp-bootstrap-auth, not both.");
        if (tcpBootstrapAuthentication && garnetEnvironmentVariable is null)
            throw new ArgumentException("--tcp-bootstrap-auth requires --garnet-environment.");
        if (tcpMode is "heartbeat" or "chat" && tcpTokens.Count == 0 && !tcpBootstrapAuthentication)
            throw new ArgumentException($"TCP mode {tcpMode} requires --tcp-token or --tcp-bootstrap-auth.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tcpBootstrapUserId);
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
            tcpMessagesPerSecond,
            tcpPayloadBytes,
            tcpSlowReaders,
            tcpTokens,
            tcpBootstrapAuthentication,
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
            onDemandSendWorkerCount,
            onDemandSendBurstLimit,
            reportDirectory,
            dockerContainers.Distinct(StringComparer.Ordinal).ToArray());
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
}

internal sealed class HelpRequestedException : Exception;
