using System.Globalization;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.LoadGenerator;

internal enum LoadMode
{
    Connection,
    Heartbeat,
    Chat,
    InvalidPacket
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
    int MessagesPerSecond,
    int PayloadBytes,
    int SlowReaders,
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
        var messagesPerSecond = 10;
        var payloadBytes = 128;
        var slowReaders = 0;
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
                case "--messages-per-second":
                    messagesPerSecond = ParseInt(value, "messages-per-second");
                    break;
                case "--payload-bytes":
                    payloadBytes = ParseInt(value, "payload-bytes");
                    break;
                case "--slow-readers":
                    slowReaders = ParseInt(value, "slow-readers");
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

        return new LoadOptions(
            host,
            port,
            connections,
            TimeSpan.FromSeconds(durationSeconds),
            selectedMode,
            accessTokens,
            deviceIdHash,
            targetUserId,
            messagesPerSecond,
            payloadBytes,
            slowReaders,
            reportDirectory);
    }

    private static LoadMode ParseMode(string value) =>
        value.ToLowerInvariant() switch
        {
            "connection" => LoadMode.Connection,
            "heartbeat" => LoadMode.Heartbeat,
            "chat" => LoadMode.Chat,
            "invalid-packet" => LoadMode.InvalidPacket,
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
