using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 2：redis-failover。
/// 认证会话并捕获 ResumeToken；等待外部 Redis 暂停后尝试 Resume（应快速失败 ResumeFailed，
/// 不挂起）；等待 Redis 恢复后再次 Resume（应成功）。当未配置故障注入（两个延迟均为 0）时，
/// 仅验证基本 Resume 成功。
/// </summary>
internal sealed class RedisFailoverScenario : IResumeScenario
{
    private static readonly TimeSpan PerAttemptTimeout = TimeSpan.FromSeconds(10);

    public string Name => "redis-failover";

    public async Task<ScenarioResult> ExecuteAsync(
        ResumeScenarioContext context,
        CancellationToken cancellationToken)
    {
        var startedAt = Stopwatch.StartNew();
        var endpoint = context.GatewayEndpoints[0];
        var errors = new List<string>();
        var metrics = new List<MetricSample>();
        var sessionCount = Math.Min(context.Options.UserCount, 5);
        var downDelay = context.Options.RedisDownDelaySeconds;
        var recoveryDelay = context.Options.RedisRecoveryDelaySeconds;
        var faultInjectionEnabled = downDelay > 0 || recoveryDelay > 0;

        var sessions = new List<(long UserId, string ResumeToken, ResumeTokenBootstrap Bootstrap)>();
        try
        {
            for (var i = 0; i < sessionCount; i++)
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

            // 无故障注入：仅验证基本 Resume 成功。
            if (!faultInjectionEnabled)
            {
                var basicResult = await TryResumeWithTimeoutAsync(
                    endpoint, sessions[0].ResumeToken, cancellationToken).ConfigureAwait(false);
                metrics.Add(new MetricSample
                {
                    Name = "rv_redis_failover_basic_resume_outcome",
                    Value = (int)basicResult.Outcome,
                    SampledAtUtc = context.TimeProvider.GetUtcNow()
                });

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

            // 阶段 2：故障期间 Resume，应快速失败（ResumeFailed），不挂起。
            var downOutcome = ResumeAttemptOutcome.Success;
            if (downDelay > 0)
            {
                var downResult = await TryResumeWithTimeoutAsync(
                    endpoint, sessions[0].ResumeToken, cancellationToken).ConfigureAwait(false);
                downOutcome = downResult.Outcome;
                metrics.Add(new MetricSample
                {
                    Name = "rv_redis_failover_down_outcome",
                    Value = (int)downOutcome,
                    SampledAtUtc = context.TimeProvider.GetUtcNow()
                });

                if (downOutcome == ResumeAttemptOutcome.Success)
                {
                    errors.Add("Resume succeeded during Redis downtime; expected failure.");
                }
            }

            // 阶段 3：等待 Redis 恢复。
            if (recoveryDelay > 0)
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(recoveryDelay), context.TimeProvider, cancellationToken).ConfigureAwait(false);
            }

            // 阶段 4：恢复后 Resume，应成功。
            var recoveryResult = await TryResumeWithTimeoutAsync(
                endpoint, sessions[Math.Min(1, sessions.Count - 1)].ResumeToken,
                cancellationToken).ConfigureAwait(false);
            metrics.Add(new MetricSample
            {
                Name = "rv_redis_failover_recovery_outcome",
                Value = (int)recoveryResult.Outcome,
                SampledAtUtc = context.TimeProvider.GetUtcNow()
            });

            var recoveryPassed = recoveryResult.Outcome == ResumeAttemptOutcome.Success;
            if (!recoveryPassed)
            {
                errors.Add($"Resume after recovery failed: {recoveryResult.ErrorMessage}");
            }

            metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                .ConfigureAwait(false));

            var passed = (downDelay == 0 || downOutcome != ResumeAttemptOutcome.Success) && recoveryPassed;
            var summary = $"down={downOutcome}, recovery={recoveryResult.Outcome}";
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
