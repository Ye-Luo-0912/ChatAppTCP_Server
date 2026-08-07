using System.Globalization;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.LoadGenerator;

internal enum LoadMode
{
    Connection,
    Heartbeat,
    Chat,
    InvalidPacket,
    Slowloris
}

/// <summary>
/// Slowloris 攻击阶段：只发送不完整 Header（Header 装配 deadline），
/// 或发送完整 Header 后分段缓慢发送 Payload（Payload 装配 deadline）。
/// 用于验证 Gateway 的帧装配 deadline 能及时关闭慢速攻击连接。
/// </summary>
internal enum SlowlorisPhase
{
    Header,
    Payload
}

internal sealed record LoadOptions(
    string Host,
    int Port,
    int Connections,
    TimeSpan Duration,
    LoadMode Mode,
    IReadOnlyList<string> AccessTokens,
    ulong? DeviceIdHash,
    long? TargetUserId,
    int ActiveSenders,
    double MessagesPerSecond,
    int PayloadBytes,
    int SlowReaders,
    int ConnectionsPerSecond,
    TimeSpan Stabilization,
    TimeSpan ConnectTimeout,
    int MaxInflight,
    TimeSpan InflightTtl,
    TimeSpan DeliveryDrain,
    TimeSpan InactiveHeartbeatInterval,
    double MinimumAcknowledgementRatio,
    double MinimumDeliveryRatio,
    SlowlorisPhase? SlowlorisPhase,
    int SlowlorisDelayMs,
    string? TargetRingFilePath,
    string? ReportDirectory)
{
    public static LoadOptions Parse(string[] args)
    {
        var host = "127.0.0.1";
        var port = 8888;
        var connections = 100;
        var durationSeconds = 10;
        LoadMode? mode = null;
        var accessTokens = new List<string>();
        ulong? deviceIdHash = null;
        long? targetUserId = null;
        int? activeSenders = null;
        var messagesPerSecond = 10d;
        var payloadBytes = 128;
        var slowReaders = 0;
        var connectionsPerSecond = 0;
        var stabilizationSeconds = 0;
        var connectTimeoutSeconds = 30;
        var maxInflight = 1_000_000;
        var inflightTtlSeconds = 120;
        var deliveryDrainSeconds = 30;
        var inactiveHeartbeatSeconds = 30;
        var minimumAcknowledgementRatio = 0.95d;
        var minimumDeliveryRatio = 0.90d;
        SlowlorisPhase? slowlorisPhase = null;
        var slowlorisDelayMs = 1_000;
        string? targetRingFilePath = null;
        string? reportDirectory = null;

        for (var index = 0; index < args.Length; index++)
        {
            var value = GetValue(args, ref index);
            switch (args[index - 1])
            {
                case "--host":
                    host = value;
                    break;
                case "--port":
                    port = ParseInt(value, "port");
                    break;
                case "--connections":
                    connections = ParseInt(value, "connections");
                    break;
                case "--duration-seconds":
                    durationSeconds = ParseInt(value, "duration-seconds");
                    break;
                case "--mode":
                    mode = ParseMode(value);
                    break;
                case "--token":
                    if (string.IsNullOrWhiteSpace(value))
                        throw new ArgumentException("Token cannot be empty.");
                    accessTokens.Add(value);
                    break;
                case "--token-file":
                    accessTokens.AddRange(ReadTokensFromFile(value));
                    break;
                case "--device-id":
                    deviceIdHash = ulong.Parse(
                        value,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture);
                    break;
                case "--target-user-id":
                    targetUserId = long.Parse(
                        value,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture);
                    break;
                case "--active-senders":
                    activeSenders = ParseInt(value, "active-senders");
                    break;
                case "--messages-per-second":
                    messagesPerSecond = ParsePositiveDouble(value, "messages-per-second");
                    break;
                case "--payload-bytes":
                    payloadBytes = ParseInt(value, "payload-bytes");
                    break;
                case "--slow-readers":
                    slowReaders = ParseInt(value, "slow-readers");
                    break;
                case "--connections-per-second":
                    connectionsPerSecond = ParseInt(value, "connections-per-second");
                    break;
                case "--stabilization-seconds":
                    stabilizationSeconds = ParseInt(value, "stabilization-seconds");
                    break;
                case "--connect-timeout-seconds":
                    connectTimeoutSeconds = ParseInt(value, "connect-timeout-seconds");
                    break;
                case "--max-inflight":
                    maxInflight = ParseInt(value, "max-inflight");
                    break;
                case "--inflight-ttl-seconds":
                    inflightTtlSeconds = ParseInt(value, "inflight-ttl-seconds");
                    break;
                case "--delivery-drain-seconds":
                    deliveryDrainSeconds = ParseInt(value, "delivery-drain-seconds");
                    break;
                case "--inactive-heartbeat-seconds":
                    inactiveHeartbeatSeconds = ParseInt(value, "inactive-heartbeat-seconds");
                    break;
                case "--min-ack-ratio":
                    minimumAcknowledgementRatio = ParseRatio(value, "min-ack-ratio");
                    break;
                case "--min-delivery-ratio":
                    minimumDeliveryRatio = ParseRatio(value, "min-delivery-ratio");
                    break;
                case "--slowloris-phase":
                    slowlorisPhase = ParseSlowlorisPhase(value);
                    break;
                case "--slowloris-delay-ms":
                    slowlorisDelayMs = ParseInt(value, "slowloris-delay-ms");
                    break;
                case "--target-ring-file":
                    targetRingFilePath = string.IsNullOrWhiteSpace(value)
                        ? throw new ArgumentException(
                            "Target ring file path cannot be empty.")
                        : value;
                    break;
                case "--report-directory":
                    reportDirectory = string.IsNullOrWhiteSpace(value)
                        ? throw new ArgumentException(
                            "Report directory cannot be empty.")
                        : value;
                    break;
                default:
                    throw new ArgumentException(
                        $"Unknown option: {args[index - 1]}");
            }
        }

        if (port is <= 0 or > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Port must be between 1 and 65535.");
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(connections);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(durationSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(messagesPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(payloadBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(slowReaders);
        ArgumentOutOfRangeException.ThrowIfNegative(connectionsPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegative(stabilizationSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(connectTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxInflight);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(inflightTtlSeconds);
        ArgumentOutOfRangeException.ThrowIfNegative(deliveryDrainSeconds);
        ArgumentOutOfRangeException.ThrowIfNegative(inactiveHeartbeatSeconds);

        if (slowReaders > connections)
        {
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Slow readers cannot exceed total connections.");
        }

        if (payloadBytes > PacketProtocol.MaxPayloadSize - 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Chat payload leaves insufficient room for JSON metadata.");
        }

        if (targetUserId is <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Target user id must be positive.");
        }

        var selectedMode = mode ??
            (accessTokens.Count == 0
                ? LoadMode.Connection
                : LoadMode.Heartbeat);

        if (selectedMode is LoadMode.Heartbeat or LoadMode.Chat &&
            accessTokens.Count == 0)
        {
            throw new ArgumentException(
                $"Mode {selectedMode} requires at least one --token.");
        }

        if (selectedMode != LoadMode.Chat && slowReaders != 0)
        {
            throw new ArgumentException(
                "--slow-readers is only valid in chat mode.");
        }

        if (targetRingFilePath is not null && selectedMode != LoadMode.Chat)
        {
            throw new ArgumentException(
                "--target-ring-file is only valid in chat mode.");
        }

        if (selectedMode == LoadMode.Chat &&
            slowReaders > 0 &&
            inactiveHeartbeatSeconds == 0)
        {
            throw new ArgumentException(
                "Chat slow readers require --inactive-heartbeat-seconds greater " +
                "than zero so idle disconnects cannot produce a false pass.");
        }

        if (activeSenders is not null &&
            selectedMode is not (LoadMode.Heartbeat or LoadMode.Chat))
        {
            throw new ArgumentException(
                "--active-senders is only valid in heartbeat or chat mode.");
        }

        var effectiveActiveSenders = selectedMode is LoadMode.Heartbeat or LoadMode.Chat
            ? activeSenders ?? (connections - slowReaders)
            : 0;
        if (selectedMode is LoadMode.Heartbeat or LoadMode.Chat &&
            effectiveActiveSenders <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Heartbeat/chat mode requires at least one active sender.");
        }
        if (effectiveActiveSenders > connections - slowReaders)
        {
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Active senders cannot exceed non-slow-reader connections.");
        }

        var inactiveAuthenticatedClients = selectedMode switch
        {
            LoadMode.Heartbeat => connections - effectiveActiveSenders,
            LoadMode.Chat => connections - effectiveActiveSenders - slowReaders,
            _ => 0
        };
        if (inactiveAuthenticatedClients > 0 && inactiveHeartbeatSeconds == 0)
        {
            throw new ArgumentException(
                "Inactive authenticated clients require " +
                "--inactive-heartbeat-seconds greater than zero.");
        }

        if (selectedMode == LoadMode.Slowloris && slowlorisPhase is null)
        {
            throw new ArgumentException(
                "--slowloris-phase is required in slowloris mode.");
        }

        if (selectedMode != LoadMode.Slowloris && slowlorisPhase is not null)
        {
            throw new ArgumentException(
                "--slowloris-phase is only valid in slowloris mode.");
        }

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(slowlorisDelayMs);

        return new LoadOptions(
            host,
            port,
            connections,
            TimeSpan.FromSeconds(durationSeconds),
            selectedMode,
            accessTokens,
            deviceIdHash,
            targetUserId,
            effectiveActiveSenders,
            messagesPerSecond,
            payloadBytes,
            slowReaders,
            connectionsPerSecond,
            TimeSpan.FromSeconds(stabilizationSeconds),
            TimeSpan.FromSeconds(connectTimeoutSeconds),
            maxInflight,
            TimeSpan.FromSeconds(inflightTtlSeconds),
            TimeSpan.FromSeconds(deliveryDrainSeconds),
            TimeSpan.FromSeconds(inactiveHeartbeatSeconds),
            minimumAcknowledgementRatio,
            minimumDeliveryRatio,
            slowlorisPhase,
            slowlorisDelayMs,
            targetRingFilePath,
            reportDirectory);
    }

    private static SlowlorisPhase ParseSlowlorisPhase(string value) =>
        value.ToLowerInvariant() switch
        {
            "header" => global::ChatApp.TcpGateway.LoadGenerator.SlowlorisPhase.Header,
            "payload" => global::ChatApp.TcpGateway.LoadGenerator.SlowlorisPhase.Payload,
            _ => throw new ArgumentException(
                $"Unknown slowloris phase: {value}")
        };

    private static LoadMode ParseMode(string value) =>
        value.ToLowerInvariant() switch
        {
            "connection" => LoadMode.Connection,
            "heartbeat" => LoadMode.Heartbeat,
            "chat" => LoadMode.Chat,
            "invalid-packet" => LoadMode.InvalidPacket,
            "slowloris" => LoadMode.Slowloris,
            _ => throw new ArgumentException(
                $"Unknown load mode: {value}")
        };

    private static int ParseInt(string value, string optionName)
    {
        if (int.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var result))
        {
            return result;
        }

        throw new ArgumentException(
            $"Option --{optionName} requires an integer value.");
    }

    private static double ParseRatio(string value, string optionName)
    {
        if (double.TryParse(
                value,
                NumberStyles.AllowDecimalPoint,
                CultureInfo.InvariantCulture,
                out var result) &&
            result is >= 0d and <= 1d)
        {
            return result;
        }

        throw new ArgumentException(
            $"Option --{optionName} requires a number between 0 and 1.");
    }

    private static double ParsePositiveDouble(string value, string optionName)
    {
        if (double.TryParse(
                value,
                NumberStyles.AllowDecimalPoint,
                CultureInfo.InvariantCulture,
                out var result) &&
            double.IsFinite(result) &&
            result > 0d)
        {
            return result;
        }

        throw new ArgumentException(
            $"Option --{optionName} requires a positive finite number.");
    }

    private static string[] ReadTokensFromFile(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Token file path cannot be empty.");
        if (!File.Exists(path))
            throw new FileNotFoundException("Token file was not found.", path);

        var tokens = File.ReadLines(path)
            .Select(static token => token.Trim())
            .Where(static token => !string.IsNullOrWhiteSpace(token))
            .ToArray();
        if (tokens.Length == 0)
            throw new ArgumentException("Token file does not contain any tokens.");

        return tokens;
    }

    private static string GetValue(string[] args, ref int index)
    {
        var option = args[index];
        index++;
        if (index >= args.Length)
            throw new ArgumentException($"Missing value for option {option}.");
        return args[index];
    }
}
