using System.Globalization;

namespace ChatApp.Realtime.PipelineLoadGenerator.Configuration;

internal sealed record PipelineLoadOptions(
    string NatsUrl,
    TimeSpan Warmup,
    TimeSpan Duration,
    int Concurrency,
    int OperationsPerSecond,
    int PayloadBytes,
    TimeSpan OperationTimeout,
    long BaseUserId,
    string? ReportDirectory)
{
    public const string Usage =
        "Usage: --nats-url nats://127.0.0.1:4222 " +
        "[--warmup-seconds 5] [--duration-seconds 30] " +
        "[--concurrency 4] [--operations-per-second 0] " +
        "[--payload-bytes 128] [--operation-timeout-seconds 15] " +
        "[--base-user-id 9200000000] [--report-directory PATH]";

    public static PipelineLoadOptions Parse(string[] args)
    {
        var natsUrl = "nats://127.0.0.1:4222";
        var warmupSeconds = 5;
        var durationSeconds = 30;
        var concurrency = 4;
        var operationsPerSecond = 0;
        var payloadBytes = 128;
        var operationTimeoutSeconds = 15;
        long baseUserId = 9_200_000_000;
        string? reportDirectory = null;

        for (var index = 0; index < args.Length; index++)
        {
            var option = args[index];
            if (option is "--help" or "-h")
                throw new HelpRequestedException();

            var value = GetValue(args, ref index, option);
            switch (option)
            {
                case "--nats-url":
                    natsUrl = value;
                    break;
                case "--warmup-seconds":
                    warmupSeconds = ParseInt(value, option);
                    break;
                case "--duration-seconds":
                    durationSeconds = ParseInt(value, option);
                    break;
                case "--concurrency":
                    concurrency = ParseInt(value, option);
                    break;
                case "--operations-per-second":
                    operationsPerSecond = ParseInt(value, option);
                    break;
                case "--payload-bytes":
                    payloadBytes = ParseInt(value, option);
                    break;
                case "--operation-timeout-seconds":
                    operationTimeoutSeconds = ParseInt(value, option);
                    break;
                case "--base-user-id":
                    baseUserId = ParseLong(value, option);
                    break;
                case "--report-directory":
                    reportDirectory = string.IsNullOrWhiteSpace(value)
                        ? throw new ArgumentException(
                            "--report-directory cannot be empty.")
                        : value;
                    break;
                default:
                    throw new ArgumentException($"Unknown option: {option}");
            }
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(natsUrl);
        ArgumentOutOfRangeException.ThrowIfNegative(warmupSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(durationSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(concurrency);
        ArgumentOutOfRangeException.ThrowIfNegative(operationsPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(payloadBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(operationTimeoutSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(baseUserId);

        if (concurrency > 1_024)
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Concurrency cannot exceed 1024.");
        if (payloadBytes > 65_536)
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Payload cannot exceed 65536 bytes.");
        if (operationsPerSecond > 1_000_000)
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Operations per second cannot exceed 1000000.");
        if (baseUserId > long.MaxValue - concurrency * 2L - 1)
            throw new ArgumentOutOfRangeException(
                nameof(args),
                "Base user id leaves insufficient room for worker users.");

        return new PipelineLoadOptions(
            natsUrl,
            TimeSpan.FromSeconds(warmupSeconds),
            TimeSpan.FromSeconds(durationSeconds),
            concurrency,
            operationsPerSecond,
            payloadBytes,
            TimeSpan.FromSeconds(operationTimeoutSeconds),
            baseUserId,
            reportDirectory);
    }

    private static string GetValue(
        string[] args,
        ref int index,
        string option)
    {
        index++;
        if (index >= args.Length)
            throw new ArgumentException($"Missing value for {option}.");
        return args[index];
    }

    private static int ParseInt(string value, string option) =>
        int.TryParse(
            value,
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out var parsed)
            ? parsed
            : throw new ArgumentException($"{option} requires an integer.");

    private static long ParseLong(string value, string option) =>
        long.TryParse(
            value,
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out var parsed)
            ? parsed
            : throw new ArgumentException($"{option} requires an integer.");
}

internal sealed class HelpRequestedException : Exception
{
}
