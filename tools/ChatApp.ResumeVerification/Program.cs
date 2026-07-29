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
    BootstrapFactory = (userId, ct) => ResumeTokenBootstrap.CreateAsync(
        options.RedisConnectionString, userId, deviceIdHash: null, ct),
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
