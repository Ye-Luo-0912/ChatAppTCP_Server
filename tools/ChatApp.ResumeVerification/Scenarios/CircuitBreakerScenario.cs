using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 3：circuit-breaker。
/// 认证会话并捕获 ResumeToken；等待外部 Redis 故障后快速连续发起多次 Resume，
/// 触发网关 Redis 熔断器进入 Open 状态，验证后续 Resume 快速失败（无 Redis 调用）；
/// 等待恢复窗口后验证熔断器恢复（HalfOpen → Closed）并 Resume 成功。
/// </summary>
internal sealed class CircuitBreakerScenario : IResumeScenario
{
    private const int RapidAttemptCount = 10;
    private static readonly TimeSpan PerAttemptTimeout = TimeSpan.FromSeconds(10);

    public string Name => "circuit-breaker";

    public async Task<ScenarioResult> ExecuteAsync(
        ResumeScenarioContext context,
        CancellationToken cancellationToken)
    {
        var startedAt = Stopwatch.StartNew();
        var endpoint = context.GatewayEndpoints[0];
        var errors = new List<string>();
        var metrics = new List<MetricSample>();
        var downDelay = context.Options.RedisDownDelaySeconds;
        var recoveryDelay = context.Options.RedisRecoveryDelaySeconds;
        var faultInjectionEnabled = downDelay > 0 || recoveryDelay > 0;

        var sessions = new List<(long UserId, string ResumeToken, ResumeTokenBootstrap Bootstrap)>();
        try
        {
            for (var i = 0; i < 2; i++)
            {
                var userId = context.Options.BootstrapUserIdStart + i;
                var connection = await ResumeScenarioRunner.AuthenticateAsync(
                        endpoint, userId, context, cancellationToken)
                    .ConfigureAwait(false);

                if (string.IsNullOrEmpty(connection.Session.ResumeToken))
                {
                    errors.Add($"User {userId} did not receive a ResumeToken.");
                    await connection.DisposeAsync().ConfigureAwait(false);
                    continue;
                }

                sessions.Add((userId, connection.Session.ResumeToken!, connection.Bootstrap));
                // 仅释放 TCP 客户端，保留 Bootstrap 供 finally 块统一释放。
                await connection.Client.DisposeAsync().ConfigureAwait(false);
            }

            if (sessions.Count == 0)
            {
                return ScenarioHelpers.Fail(
                    Name, startedAt, "No ResumeTokens captured.", metrics, errors);
            }

            if (!faultInjectionEnabled)
            {
                var basicResult = await TryResumeWithTimeoutAsync(
                    endpoint, sessions[0].ResumeToken, cancellationToken).ConfigureAwait(false);
                var basicPassed = basicResult.Outcome == ResumeAttemptOutcome.Success;
                var basicSummary = basicPassed
                    ? "No fault injection configured; basic resume succeeded."
                    : $"No fault injection configured; basic resume failed: {basicResult.ErrorMessage}";
                if (!basicPassed)
                {
                    errors.Add(basicResult.ErrorMessage ?? "basic resume failed");
                }

                metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                    .ConfigureAwait(false));
                return basicPassed
                    ? ScenarioHelpers.Pass(Name, startedAt, basicSummary, metrics, errors)
                    : ScenarioHelpers.Fail(Name, startedAt, basicSummary, metrics, errors);
            }

            // 阶段 1：等待 Redis 故障注入。
            if (downDelay > 0)
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(downDelay), context.TimeProvider, cancellationToken).ConfigureAwait(false);
            }

            // 阶段 2：快速连续发起多次 Resume，测量 fail-fast 时长。
            var failFastDurationsMs = new List<double>();
            var downtimeFailures = 0;
            if (downDelay > 0)
            {
                var resumeToken = sessions[0].ResumeToken;
                for (var i = 0; i < RapidAttemptCount; i++)
                {
                    var attemptStartedAt = Stopwatch.GetTimestamp();
                    var result = await TryResumeWithTimeoutAsync(
                        endpoint, resumeToken, cancellationToken).ConfigureAwait(false);
                    var attemptMs = Stopwatch.GetElapsedTime(attemptStartedAt).TotalMilliseconds;

                    if (result.Outcome != ResumeAttemptOutcome.Success)
                    {
                        downtimeFailures++;
                        failFastDurationsMs.Add(attemptMs);
                    }
                    else
                    {
                        errors.Add(
                            $"Rapid attempt {i} succeeded during Redis downtime; expected failure.");
                    }
                }

                if (failFastDurationsMs.Count != 0)
                {
                    var sorted = failFastDurationsMs.Order().ToArray();
                    metrics.Add(new MetricSample
                    {
                        Name = "rv_circuit_breaker_failfast_p50_ms",
                        Value = sorted[sorted.Length / 2],
                        SampledAtUtc = context.TimeProvider.GetUtcNow()
                    });
                    metrics.Add(new MetricSample
                    {
                        Name = "rv_circuit_breaker_failfast_min_ms",
                        Value = sorted[0],
                        SampledAtUtc = context.TimeProvider.GetUtcNow()
                    });
                    metrics.Add(new MetricSample
                    {
                        Name = "rv_circuit_breaker_downtime_failures",
                        Value = downtimeFailures,
                        SampledAtUtc = context.TimeProvider.GetUtcNow()
                    });
                }
            }

            // 阶段 3：等待恢复窗口。
            if (recoveryDelay > 0)
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(recoveryDelay), context.TimeProvider, cancellationToken).ConfigureAwait(false);
            }

            // 阶段 4：恢复后 Resume，应成功（熔断器 HalfOpen → Closed）。
            var recoveryResult = await TryResumeWithTimeoutAsync(
                endpoint, sessions[Math.Min(1, sessions.Count - 1)].ResumeToken,
                cancellationToken).ConfigureAwait(false);
            var recoveryPassed = recoveryResult.Outcome == ResumeAttemptOutcome.Success;
            if (!recoveryPassed)
            {
                errors.Add($"Resume after recovery failed: {recoveryResult.ErrorMessage}");
            }

            metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                .ConfigureAwait(false));

            // 断言：故障期间全部快速失败 + 恢复后成功。fail-fast 时长应低于阈值（采集但不强制）。
            var downtimePassed = downDelay == 0 || downtimeFailures == RapidAttemptCount;
            var passed = downtimePassed && recoveryPassed;

            var failFastNote = failFastDurationsMs.Count != 0
                ? $"failfast min={failFastDurationsMs.Min():F1}ms"
                : "no failfast samples";
            var summary = $"downtime_failures={downtimeFailures}/{RapidAttemptCount}, " +
                          $"recovery={recoveryResult.Outcome}, {failFastNote}";

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

    private static async Task<ResumeAttemptResult> TryResumeWithTimeoutAsync(
        GatewayEndpoint endpoint,
        string resumeToken,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(PerAttemptTimeout);
        try
        {
            return await ResumeScenarioRunner.TryResumeAsync(
                    endpoint, resumeToken, timeoutCts.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested &&
                                                 !cancellationToken.IsCancellationRequested)
        {
            return new ResumeAttemptResult(
                ResumeAttemptOutcome.Failed,
                Session: null,
                ErrorMessage: "Resume timed out (possible hang).");
        }
        catch (System.IO.IOException ex)
        {
            return new ResumeAttemptResult(
                ResumeAttemptOutcome.Failed,
                Session: null,
                ErrorMessage: $"Connection error: {ex.Message}");
        }
    }
}
