using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 1：concurrent-replay。
/// 在网关 A 认证 N 个会话并获取 ResumeToken，断开后对每个 Token 在网关 A 与网关 B
/// 并发尝试 Resume，断言每个 Token 恰好一次成功（另一侧 ResumeFailed）。
/// </summary>
internal sealed class ConcurrentReplayScenario : IResumeScenario
{
    public string Name => "concurrent-replay";

    public async Task<ScenarioResult> ExecuteAsync(
        ResumeScenarioContext context,
        CancellationToken cancellationToken)
    {
        var startedAt = Stopwatch.StartNew();
        var endpoints = context.GatewayEndpoints;
        var endpointA = endpoints[0];
        var endpointB = endpoints.Count > 1 ? endpoints[1] : endpoints[0];
        var userCount = context.Options.UserCount;
        var errors = new List<string>();
        var metrics = new List<MetricSample>();

        var sessions = new List<(long UserId, string ResumeToken, ResumeTokenBootstrap Bootstrap)>();
        try
        {
            // 阶段 1：在网关 A 认证 N 个会话，捕获 ResumeToken，随后断开连接。
            for (var i = 0; i < userCount; i++)
            {
                var userId = context.Options.BootstrapUserIdStart + i;
                var connection = await ResumeScenarioRunner.AuthenticateAsync(
                        endpointA, userId, context, cancellationToken)
                    .ConfigureAwait(false);

                if (string.IsNullOrEmpty(connection.Session.ResumeToken))
                {
                    errors.Add(
                        $"User {userId} did not receive a ResumeToken; " +
                        "server may not support resume.");
                    await connection.DisposeAsync().ConfigureAwait(false);
                    continue;
                }

                sessions.Add((userId, connection.Session.ResumeToken!, connection.Bootstrap));
                // 仅释放 TCP 客户端，保留 Bootstrap（Redis 连接 + AccessToken 缓存键）。
                // Bootstrap 在 finally 块中统一释放，避免重复 Dispose 导致 Redis 连接访问异常。
                await connection.Client.DisposeAsync().ConfigureAwait(false);
            }

            if (sessions.Count == 0)
            {
                return ScenarioHelpers.Fail(
                    Name,
                    startedAt,
                    "No ResumeTokens captured; cannot run concurrent replay.",
                    metrics,
                    errors);
            }

            // 阶段 2：对每个 Token，在 A 与 B 并发 Resume。
            var tasks = sessions.Select(
                s => TryConcurrentResumeAsync(
                    endpointA, endpointB, s.ResumeToken, cancellationToken)).ToArray();
            var outcomes = await Task.WhenAll(tasks).ConfigureAwait(false);

            var successCount = 0;
            var failureCount = 0;
            var mismatchCount = 0;
            foreach (var outcome in outcomes)
            {
                var successes = (outcome.OutcomeA == ResumeAttemptOutcome.Success ? 1 : 0) +
                                (outcome.OutcomeB == ResumeAttemptOutcome.Success ? 1 : 0);

                if (successes == 1)
                {
                    successCount++;
                }
                else
                {
                    mismatchCount++;
                    if (successes == 0)
                    {
                        failureCount++;
                    }
                    else
                    {
                        errors.Add(
                            $"Token resumed successfully on BOTH gateways: {outcome.Detail}");
                    }
                }
            }

            metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                .ConfigureAwait(false));
            var now = context.TimeProvider.GetUtcNow();
            metrics.Add(new MetricSample { Name = "rv_concurrent_replay_success_total", Value = successCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_concurrent_replay_failure_total", Value = failureCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_concurrent_replay_mismatch_total", Value = mismatchCount, SampledAtUtc = now });

            var passed = mismatchCount == 0 && successCount == sessions.Count;
            var summary = passed
                ? $"All {sessions.Count} tokens resumed exactly once."
                : $"success={successCount}, failure={failureCount}, mismatch={mismatchCount} of {sessions.Count} tokens.";

            return passed
                ? ScenarioHelpers.Pass(Name, startedAt, summary, metrics, errors)
                : ScenarioHelpers.Fail(Name, startedAt, summary, metrics, errors);
        }
        finally
        {
            foreach (var session in sessions)
            {
                await session.Bootstrap.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private static async Task<ConcurrentResumeOutcome> TryConcurrentResumeAsync(
        GatewayEndpoint endpointA,
        GatewayEndpoint endpointB,
        string resumeToken,
        CancellationToken cancellationToken)
    {
        var taskA = ResumeScenarioRunner.TryResumeAsync(endpointA, resumeToken, cancellationToken);
        var taskB = ResumeScenarioRunner.TryResumeAsync(endpointB, resumeToken, cancellationToken);
        var results = await Task.WhenAll(taskA, taskB).ConfigureAwait(false);
        return new ConcurrentResumeOutcome(
            results[0].Outcome,
            results[1].Outcome,
            $"A={results[0].Outcome}; B={results[1].Outcome}");
    }

    private sealed record ConcurrentResumeOutcome(
        ResumeAttemptOutcome OutcomeA,
        ResumeAttemptOutcome OutcomeB,
        string Detail);
}
