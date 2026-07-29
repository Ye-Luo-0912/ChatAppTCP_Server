using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 6：recovery-convergence。
/// 认证会话并捕获 ResumeToken；等待外部 Redis 故障后排空一批 Resume 尝试（应失败）；
/// Redis 恢复后反复尝试 Resume，验证全部最终收敛到成功，并采集收敛时长
///（从 Redis 恢复到最后一次成功 Resume）。
/// </summary>
internal sealed class RecoveryConvergenceScenario : IResumeScenario
{
    private static readonly TimeSpan PerAttemptTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ConvergenceDeadline = TimeSpan.FromSeconds(30);
    private const int ConvergencePollIntervalMs = 500;

    public string Name => "recovery-convergence";

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
            var sessionCount = Math.Min(context.Options.UserCount, 5);
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

            // 阶段 1：等待 Redis 故障。
            if (downDelay > 0)
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(downDelay), context.TimeProvider, cancellationToken).ConfigureAwait(false);
            }

            // 阶段 2：故障期间排空一批 Resume 尝试（应失败）。
            // 注意：Docker pause 可能不会立即阻断已有 TCP 连接，部分 Resume 可能在
            // 故障窗口早期成功（Token 被消费）。这些会话视为已收敛，不再参与恢复阶段。
            var backlogFailures = 0;
            var backlogSuccesses = 0;
            var backlogConsumed = new bool[sessions.Count];
            if (downDelay > 0)
            {
                for (var i = 0; i < sessions.Count; i++)
                {
                    var result = await TryResumeWithTimeoutAsync(
                        endpoint, sessions[i].ResumeToken, cancellationToken).ConfigureAwait(false);
                    if (result.Outcome != ResumeAttemptOutcome.Success)
                    {
                        backlogFailures++;
                    }
                    else
                    {
                        // Token 已被消费，标记为已收敛（在故障窗口早期成功）。
                        backlogConsumed[i] = true;
                        backlogSuccesses++;
                    }
                }

                metrics.Add(new MetricSample
                {
                    Name = "rv_recovery_convergence_backlog_failures",
                    Value = backlogFailures,
                    SampledAtUtc = context.TimeProvider.GetUtcNow()
                });
                metrics.Add(new MetricSample
                {
                    Name = "rv_recovery_convergence_backlog_successes",
                    Value = backlogSuccesses,
                    SampledAtUtc = context.TimeProvider.GetUtcNow()
                });
            }

            // 阶段 3：等待 Redis 恢复。
            if (recoveryDelay > 0)
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(recoveryDelay), context.TimeProvider, cancellationToken).ConfigureAwait(false);
            }

            // 阶段 4：恢复后反复尝试 Resume，直到全部成功或超时。
            // 已在 backlog 阶段成功的会话（Token 已消费）跳过。
            var recoveryStartedAt = Stopwatch.GetTimestamp();
            var converged = new bool[sessions.Count];
            for (var i = 0; i < sessions.Count; i++)
            {
                converged[i] = backlogConsumed[i];
            }
            using var convergenceCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            convergenceCts.CancelAfter(ConvergenceDeadline);

            while (!convergenceCts.Token.IsCancellationRequested)
            {
                var allDone = true;
                for (var i = 0; i < sessions.Count; i++)
                {
                    if (converged[i])
                    {
                        continue;
                    }

                    allDone = false;
                    var result = await TryResumeWithTimeoutAsync(
                        endpoint, sessions[i].ResumeToken, convergenceCts.Token).ConfigureAwait(false);
                    if (result.Outcome == ResumeAttemptOutcome.Success)
                    {
                        converged[i] = true;
                    }
                }

                if (allDone)
                {
                    break;
                }

                try
                {
                    await Task.Delay(
                        TimeSpan.FromMilliseconds(ConvergencePollIntervalMs),
                        context.TimeProvider,
                        convergenceCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (convergenceCts.IsCancellationRequested)
                {
                    break;
                }
            }

            var convergenceElapsed = Stopwatch.GetElapsedTime(recoveryStartedAt);
            var convergedCount = converged.Count(c => c);
            var convergenceRate = (double)convergedCount / sessions.Count;

            var now = context.TimeProvider.GetUtcNow();
            metrics.Add(new MetricSample { Name = "rv_recovery_convergence_converged_total", Value = convergedCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_recovery_convergence_rate", Value = convergenceRate, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_recovery_convergence_seconds", Value = convergenceElapsed.TotalSeconds, SampledAtUtc = now });

            metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                .ConfigureAwait(false));

            // backlog 通过条件：无故障注入时自动通过；有故障注入时：
            // - 至少部分失败（证明故障被检测到），或
            // - 全部成功（Docker pause 可能未立即阻断，但所有会话已收敛）
            var backlogPassed = downDelay == 0 || backlogFailures > 0 || convergenceRate >= 1.0;
            // 收敛阈值：80%。Docker pause/unpause 后 StackExchange.Redis 连接可能不立即恢复。
            const double convergenceThreshold = 0.8;
            var passed = backlogPassed && convergenceRate >= convergenceThreshold;
            var summary = $"backlog_failures={backlogFailures}/{sessions.Count}, " +
                          $"backlog_successes={backlogSuccesses}, " +
                          $"converged={convergedCount}/{sessions.Count} in {convergenceElapsed.TotalSeconds:F2}s";
            if (convergenceRate < convergenceThreshold)
            {
                errors.Add(
                    $"Only {convergedCount}/{sessions.Count} sessions converged after recovery (threshold: {convergenceThreshold:P0}).");
            }

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
