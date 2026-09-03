using System.Globalization;

namespace ChatApp.BinaryPayloadShortTest;

/// <summary>负载 harness 的 wire 格式。</summary>
internal enum WireFormat
{
    Json,
    Binary
}

/// <summary>
/// CLI 选项：<c>--format json|binary --rates 80,320,640 --seconds-per-rate 180,180,240</c>
/// （默认值即此）。允许覆盖以便调试；正式数据要求每 phase ≥ 60s。
/// </summary>
internal sealed record HarnessOptions
{
    public WireFormat Format { get; init; } = WireFormat.Binary;
    public IReadOnlyList<int> Rates { get; init; } = [80, 320, 640];
    public IReadOnlyList<int> SecondsPerRate { get; init; } = [180, 180, 240];
    public IReadOnlyList<int> SendersPerRate { get; init; } = [1, 1, 2];
    public int IdleConnections { get; init; } = 8;
    public int WarmupPerSender { get; init; } = 50;
    public int DrainSeconds { get; init; } = 60;
    public string OutputPath { get; init; } = "scratch/binary-shorttest-results.jsonl";
    public string Label { get; init; } = string.Empty;

    public bool EnableBinary => Format == WireFormat.Binary;

    public string Describe() =>
        $"format={Format} rates=[{Join(Rates)}] seconds=[{Join(SecondsPerRate)}] " +
        $"senders=[{Join(SendersPerRate)}] idle={IdleConnections} warmup={WarmupPerSender} " +
        $"drain={DrainSeconds}s label='{Label}' out='{OutputPath}'";

    public const string Usage =
        "usage: ChatApp.BinaryPayloadShortTest [--format json|binary] " +
        "[--rates 80,320,640] [--seconds-per-rate 180,180,240] [--senders-per-rate 1,1,2] " +
        "[--idle-connections 8] [--warmup-per-sender 50] [--drain-seconds 60] " +
        "[--out path.jsonl] [--label text]";

    public static HarnessOptions Parse(IReadOnlyList<string> args)
    {
        var format = WireFormat.Binary;
        int[]? rates = null;
        int[]? seconds = null;
        int[]? senders = null;
        int idle = 8, warmup = 50, drain = 60;
        string? output = null;
        var label = string.Empty;

        for (var i = 0; i < args.Count; i++)
        {
            var value = i + 1 < args.Count ? args[i + 1] : null;
            switch (args[i])
            {
                case "--format" when value is not null:
                    format = value.Equals("json", StringComparison.OrdinalIgnoreCase)
                        ? WireFormat.Json
                        : value.Equals("binary", StringComparison.OrdinalIgnoreCase)
                            ? WireFormat.Binary
                            : throw new ArgumentException("--format must be 'json' or 'binary'.");
                    i++;
                    break;
                case "--rates" when value is not null:
                    rates = ParseIntList(value);
                    i++;
                    break;
                case "--seconds-per-rate" when value is not null:
                    seconds = ParseIntList(value);
                    i++;
                    break;
                case "--senders-per-rate" when value is not null:
                    senders = ParseIntList(value);
                    i++;
                    break;
                case "--idle-connections" when value is not null:
                    idle = ParsePositive(value, "--idle-connections");
                    i++;
                    break;
                case "--warmup-per-sender" when value is not null:
                    warmup = ParsePositive(value, "--warmup-per-sender");
                    i++;
                    break;
                case "--drain-seconds" when value is not null:
                    drain = ParsePositive(value, "--drain-seconds");
                    i++;
                    break;
                case "--out" when value is not null:
                    output = value;
                    i++;
                    break;
                case "--label" when value is not null:
                    label = value;
                    i++;
                    break;
                default:
                    throw new ArgumentException($"unknown or value-less argument '{args[i]}'.");
            }
        }

        rates ??= [80, 320, 640];
        seconds ??= [180, 180, 240];
        senders ??= [1, 1, 2];

        Guard.Ensure(
            rates.Length == seconds.Length && rates.Length == senders.Length,
            "--rates / --seconds-per-rate / --senders-per-rate must have equal lengths.");
        for (var i = 0; i < rates.Length; i++)
        {
            Guard.Ensure(rates[i] > 0, $"rate #{i + 1} must be positive.");
            Guard.Ensure(seconds[i] > 0, $"seconds #{i + 1} must be positive.");
            Guard.Ensure(senders[i] > 0 && rates[i] % senders[i] == 0,
                $"senders #{i + 1} must divide rate {rates[i]} evenly.");
        }

        return new HarnessOptions
        {
            Format = format,
            Rates = rates,
            SecondsPerRate = seconds,
            SendersPerRate = senders,
            IdleConnections = idle,
            WarmupPerSender = warmup,
            DrainSeconds = drain,
            OutputPath = output ?? "scratch/binary-shorttest-results.jsonl",
            Label = label
        };
    }

    private static int[] ParseIntList(string value)
    {
        try
        {
            return value.Split(',').Select(static part => int.Parse(part, CultureInfo.InvariantCulture)).ToArray();
        }
        catch (FormatException exception)
        {
            throw new ArgumentException($"invalid integer list '{value}'.", exception);
        }
    }

    private static int ParsePositive(string value, string name)
    {
        if (!int.TryParse(value, out var parsed) || parsed <= 0)
        {
            throw new ArgumentException($"{name} must be a positive integer, got '{value}'.");
        }

        return parsed;
    }

    private static string Join(IReadOnlyList<int> values) => string.Join(',', values);
}
