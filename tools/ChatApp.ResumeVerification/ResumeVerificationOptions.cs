using System.Globalization;

namespace ChatApp.ResumeVerification;

/// <summary>
/// Resume 验证工具的命令行选项。
/// </summary>
internal sealed record ResumeVerificationOptions(
    IReadOnlyList<GatewayEndpoint> GatewayEndpoints,
    IReadOnlyList<string> MetricsUrls,
    string RedisConnectionString,
    IReadOnlyList<string> Scenarios,
    int UserCount,
    int StormSize,
    int RedisDownDelaySeconds,
    int RedisRecoveryDelaySeconds,
    string ReportDirectory,
    long BootstrapUserIdStart,
    int WarmupSeconds)
{
    /// <summary>用法提示。</summary>
    public const string Usage =
        "Usage: --gateway-endpoint HOST:PORT [--gateway-endpoint HOST:PORT ...] " +
        "--redis-connection-string CONNECTION " +
        "[--metrics-url URL] " +
        "[--scenario NAME ...] " +
        "[--user-count 50] [--storm-size 1000] " +
        "[--redis-down-delay-seconds 0] [--redis-recovery-delay-seconds 0] " +
        "[--report-directory .artifacts/resume-verification] " +
        "[--bootstrap-user-id-start 9400000000] [--warmup-seconds 3]\n" +
        "Storm sizes up to 10000 are supported; concurrency and deadline scale automatically.";

    /// <summary>所有可用场景名称（按顺序）。</summary>
    public static readonly string[] AllScenarios =
    [
        "concurrent-replay",
        "redis-failover",
        "circuit-breaker",
        "takeover-competition",
        "reconnect-storm",
        "recovery-convergence"
    ];

    /// <summary>解析命令行参数。</summary>
    public static ResumeVerificationOptions Parse(string[] args)
    {
        var endpoints = new List<GatewayEndpoint>();
        var metricsUrls = new List<string>();
        string? redisConnectionString = null;
        var scenarios = new List<string>();
        var userCount = 50;
        var stormSize = 1000;
        var redisDownDelaySeconds = 0;
        var redisRecoveryDelaySeconds = 0;
        string? reportDirectory = null;
        long bootstrapUserIdStart = 9_400_000_000;
        var warmupSeconds = 3;

        for (var index = 0; index < args.Length; index++)
        {
            var option = args[index];
            if (option is "--help" or "-h")
            {
                throw new HelpRequestedException();
            }

            var value = GetValue(args, ref index, option);
            switch (option)
            {
                case "--gateway-endpoint":
                    endpoints.Add(ParseEndpoint(value, option));
                    break;
                case "--metrics-url":
                    metricsUrls.Add(value);
                    break;
                case "--redis-connection-string":
                    redisConnectionString = value;
                    break;
                case "--scenario":
                    scenarios.Add(value);
                    break;
                case "--user-count":
                    userCount = ParseInt(value, option);
                    break;
                case "--storm-size":
                    stormSize = ParseInt(value, option);
                    break;
                case "--redis-down-delay-seconds":
                    redisDownDelaySeconds = ParseInt(value, option);
                    break;
                case "--redis-recovery-delay-seconds":
                    redisRecoveryDelaySeconds = ParseInt(value, option);
                    break;
                case "--report-directory":
                    reportDirectory = Path.GetFullPath(value);
                    break;
                case "--bootstrap-user-id-start":
                    bootstrapUserIdStart = ParseLong(value, option);
                    break;
                case "--warmup-seconds":
                    warmupSeconds = ParseInt(value, option);
                    break;
                default:
                    throw new ArgumentException($"Unknown option: {option}");
            }
        }

        if (endpoints.Count == 0)
        {
            throw new ArgumentException(
                "At least one --gateway-endpoint HOST:PORT is required.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(redisConnectionString);

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(stormSize);
        ArgumentOutOfRangeException.ThrowIfNegative(redisDownDelaySeconds);
        ArgumentOutOfRangeException.ThrowIfNegative(redisRecoveryDelaySeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(bootstrapUserIdStart);
        ArgumentOutOfRangeException.ThrowIfNegative(warmupSeconds);

        // 未指定 scenario 时运行全部。
        var selectedScenarios = scenarios.Count == 0
            ? AllScenarios.ToArray()
            : ValidateScenarios(scenarios);

        reportDirectory = Path.GetFullPath(reportDirectory ?? Path.Combine(
            Environment.CurrentDirectory,
            ".artifacts",
            "resume-verification"));

        return new ResumeVerificationOptions(
            endpoints,
            metricsUrls,
            redisConnectionString,
            selectedScenarios,
            userCount,
            stormSize,
            redisDownDelaySeconds,
            redisRecoveryDelaySeconds,
            reportDirectory,
            bootstrapUserIdStart,
            warmupSeconds);
    }

    private static string[] ValidateScenarios(List<string> scenarios)
    {
        var known = new HashSet<string>(AllScenarios, StringComparer.Ordinal);
        foreach (var scenario in scenarios)
        {
            if (!known.Contains(scenario))
            {
                throw new ArgumentException(
                    $"Unknown scenario: {scenario}. Available: {string.Join(", ", AllScenarios)}");
            }
        }

        return scenarios.Distinct(StringComparer.Ordinal).ToArray();
    }

    private static GatewayEndpoint ParseEndpoint(string value, string option)
    {
        var separator = value.LastIndexOf(':');
        if (separator <= 0 || separator == value.Length - 1)
        {
            throw new ArgumentException(
                $"{option} must be HOST:PORT, got '{value}'.");
        }

        var host = value[..separator];
        if (string.IsNullOrWhiteSpace(host))
        {
            throw new ArgumentException($"{option} host cannot be empty.");
        }

        if (!int.TryParse(
                value[(separator + 1)..],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var port) ||
            port is <= 0 or > ushort.MaxValue)
        {
            throw new ArgumentException(
                $"{option} port must be between 1 and 65535, got '{value[(separator + 1)..]}'.");
        }

        return new GatewayEndpoint(host, port);
    }

    private static string GetValue(string[] args, ref int index, string option)
    {
        index++;
        if (index >= args.Length)
        {
            throw new ArgumentException($"Missing value for {option}.");
        }

        return args[index];
    }

    private static int ParseInt(string value, string option) =>
        int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new ArgumentException($"{option} requires an integer, got '{value}'.");

    private static long ParseLong(string value, string option) =>
        long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new ArgumentException($"{option} requires an integer, got '{value}'.");
}

/// <summary>用户请求 --help 时抛出。</summary>
internal sealed class HelpRequestedException : Exception;

/// <summary>网关端点（HOST + PORT）。重复定义于 Scenarios 命名空间以避免跨命名空间引用歧义。</summary>
internal sealed record GatewayEndpoint(string Host, int Port);
