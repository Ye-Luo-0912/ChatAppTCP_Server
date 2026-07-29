using System.Diagnostics;
using ChatApp.ResumeVerification;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;
using ChatApp.ResumeVerification.Scenarios;

ResumeVerificationOptions options;
try
{
    options = ResumeVerificationOptions.Parse(args);
}
catch (HelpRequestedException)
{
    Console.WriteLine(ResumeVerificationOptions.Usage);
    return 0;
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception.Message);
    Console.Error.WriteLine(ResumeVerificationOptions.Usage);
    return 2;
}

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

var startedAtUtc = DateTimeOffset.UtcNow;
Console.WriteLine($"ChatApp Resume fault stress verification");
Console.WriteLine($"Gateways: {string.Join(", ", options.GatewayEndpoints.Select(e => $"{e.Host}:{e.Port}"))}");
Console.WriteLine($"Scenarios: {string.Join(", ", options.Scenarios)}");
Console.WriteLine($"User count: {options.UserCount}, storm size: {options.StormSize}");

// 预热：等待网关就绪。
if (options.WarmupSeconds > 0)
{
    Console.WriteLine($"Warming up {options.WarmupSeconds}s...");
    try
    {
        await Task.Delay(TimeSpan.FromSeconds(options.WarmupSeconds), cts.Token)
            .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
        return 130;
    }
}

var metricsSampler = options.MetricsUrls.Count > 0
    ? new MetricsSampler(options.MetricsUrls[0])
    : null;

var context = new ResumeScenarioContext
{
    GatewayEndpoints = options.GatewayEndpoints,
    RedisConnectionString = options.RedisConnectionString,
    // P0-B：注入真实 DeviceIdHash（按 userId 确定性派生），使 AccessToken 与
    // AuthenticationRequest 携带一致的设备指纹，触发 same-device fencing 路径。
    // 旧实现传 null，网关跳过设备绑定校验，fencing 路径永远不被执行。
    BootstrapFactory = (userId, ct) => ResumeTokenBootstrap.CreateAsync(
        options.RedisConnectionString, userId, deviceIdHash: DeriveDeviceIdHash(userId), ct),
    MetricsSampler = metricsSampler,
    TimeProvider = TimeProvider.System,
    Options = options
};

var scenarios = BuildScenarios(options.Scenarios);
Console.WriteLine($"Running {scenarios.Count} scenario(s)...");
var results = await ResumeScenarioRunner.RunAsync(scenarios, context, cts.Token)
    .ConfigureAwait(false);

var allPassed = results.All(static r => r.Passed);
var completedAtUtc = DateTimeOffset.UtcNow;

var report = new ResumeVerificationReport
{
    StartedAtUtc = startedAtUtc,
    CompletedAtUtc = completedAtUtc,
    Configuration = new ResumeVerificationConfiguration
    {
        GatewayEndpoints = options.GatewayEndpoints
            .Select(e => $"{e.Host}:{e.Port}").ToList(),
        UserCount = options.UserCount,
        StormSize = options.StormSize,
        RedisDownDelaySeconds = options.RedisDownDelaySeconds,
        RedisRecoveryDelaySeconds = options.RedisRecoveryDelaySeconds,
        BootstrapUserIdStart = options.BootstrapUserIdStart,
        WarmupSeconds = options.WarmupSeconds
    },
    Scenarios = results,
    AllPassed = allPassed
};

var paths = ResumeVerificationReportWriter.Write(report, options.ReportDirectory);
Console.WriteLine($"JSON report: {paths.JsonPath}");
Console.WriteLine($"Markdown report: {paths.MarkdownPath}");
Console.WriteLine($"Overall: {(allPassed ? "PASSED" : "FAILED")}");
return allPassed ? 0 : 1;

static List<IResumeScenario> BuildScenarios(IReadOnlyList<string> names)
{
    var byName = new Dictionary<string, IResumeScenario>(StringComparer.Ordinal)
    {
        ["concurrent-replay"] = new ConcurrentReplayScenario(),
        ["redis-failover"] = new RedisFailoverScenario(),
        ["circuit-breaker"] = new CircuitBreakerScenario(),
        ["takeover-competition"] = new TakeoverCompetitionScenario(),
        ["reconnect-storm"] = new ReconnectStormScenario(),
        ["recovery-convergence"] = new RecoveryConvergenceScenario()
    };

    var scenarios = new List<IResumeScenario>(names.Count);
    foreach (var name in names)
    {
        if (byName.TryGetValue(name, out var scenario))
        {
            scenarios.Add(scenario);
        }
    }

    return scenarios;
}

// 按 userId 确定性派生 DeviceIdHash：同一用户跨连接/跨网关使用同一设备指纹，
// 使 same-device fencing（租约接管校验）路径被实际执行。
// 使用黄金比例乘数 + 1 保证分布且 userId=0 时仍非零（0 在某些路径被当作"未设置"）。
static ulong DeriveDeviceIdHash(long userId) =>
    unchecked((ulong)userId * 0x9E3779B97F4A7C15UL + 1);
