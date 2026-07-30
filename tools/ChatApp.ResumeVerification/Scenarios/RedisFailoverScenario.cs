using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 2：redis-failover。
/// 认证会话并捕获 ResumeToken；等待外部 Redis 暂停后尝试 Resume（应快速失败
/// <see cref="ProtocolErrorCode.DependencyUnavailable"/>，不挂起）；等待 Redis 恢复后：
/// <list type="bullet">
/// <item>有效 ResumeToken 应 Resume 成功；</item>
/// <item>无效 Token 应返回 <see cref="ProtocolErrorCode.ResumeFailed"/>（与故障期间的
/// <see cref="ProtocolErrorCode.DependencyUnavailable"/> 区分），验证客户端可正确区分
/// 可重试依赖故障与不可恢复的 Token 失效。</item>
/// </list>
/// 当未配置故障注入（两个延迟均为 0）时，仅验证基本 Resume 成功 + 无效 Token 返回 ResumeFailed。
/// </summary>
internal sealed class RedisFailoverScenario : IResumeScenario
{
    private static readonly TimeSpan PerAttemptTimeout = TimeSpan.FromSeconds(10);

    /// <summary>用于验证 Token 校验路径的伪 Token：Redis 中不存在，应返回 ResumeFailed。</summary>
    private const string InvalidTokenProbe = "rv-invalid-token-probe-not-in-redis";

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

            // 无故障注入：验证基本 Resume 成功 + 无效 Token 返回 ResumeFailed。
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
                if (!basicPassed)
                {
                    errors.Add(basicResult.ErrorMessage ?? "basic resume failed");
                }

                // P1-E：无故障注入时也验证无效 Token 返回 ResumeFailed（非 DependencyUnavailable），
                // 确保错误码区分路径始终被覆盖，而不依赖外部 Redis 故障注入。
                var basicInvalidProbeResult = await TryResumeWithTimeoutAsync(
                    endpoint, InvalidTokenProbe, cancellationToken).ConfigureAwait(false);
                var basicInvalidProbePassed = basicInvalidProbeResult.Outcome != ResumeAttemptOutcome.Success
                    && basicInvalidProbeResult.ErrorCode == ProtocolErrorCode.ResumeFailed;
                metrics.Add(new MetricSample
                {
                    Name = "rv_redis_failover_invalid_token_outcome",
                    Value = (int)basicInvalidProbeResult.Outcome,
                    SampledAtUtc = context.TimeProvider.GetUtcNow()
                });
                metrics.Add(new MetricSample
                {
                    Name = "rv_redis_failover_invalid_token_error_code",
                    Value = (int)(basicInvalidProbeResult.ErrorCode ?? ProtocolErrorCode.None),
                    SampledAtUtc = context.TimeProvider.GetUtcNow()
                });
                if (!basicInvalidProbePassed)
                {
                    errors.Add(
                        $"Invalid token probe expected ResumeFailed, got " +
                        $"outcome={basicInvalidProbeResult.Outcome}, " +
                        $"errorCode={basicInvalidProbeResult.ErrorCode}, " +
                        $"msg={basicInvalidProbeResult.ErrorMessage}");
                }

                metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                    .ConfigureAwait(false));
                var basicPassedOverall = basicPassed && basicInvalidProbePassed;
                var basicSummary = basicPassedOverall
                    ? "No fault injection configured; basic resume succeeded + invalid token returned ResumeFailed."
                    : $"basic={basicResult.Outcome}, invalid_token={basicInvalidProbeResult.Outcome}" +
                      $"(code={basicInvalidProbeResult.ErrorCode})";
                return basicPassedOverall
                    ? ScenarioHelpers.Pass(Name, startedAt, basicSummary, metrics, errors)
                    : ScenarioHelpers.Fail(Name, startedAt, basicSummary, metrics, errors);
            }

            // 阶段 1：等待 Redis 故障注入。
            if (downDelay > 0)
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(downDelay), context.TimeProvider, cancellationToken).ConfigureAwait(false);
            }

            // 阶段 2：故障期间 Resume，应快速失败（DependencyUnavailable），不挂起。
            // P1-E：验证失败时返回 DependencyUnavailable（可重试），而非 ResumeFailed（不可恢复）。
            // 注意：Docker pause 可能未立即阻断已有 TCP 连接，部分 Resume 可能在故障窗口早期成功
            //（Token 被消费）。这种情况下跳过错误码断言，但记录为 backlog 现象。
            var downOutcome = ResumeAttemptOutcome.Success;
            var downErrorCode = (ProtocolErrorCode?)null;
            var downDependencyUnavailable = false;
            if (downDelay > 0)
            {
                var downResult = await TryResumeWithTimeoutAsync(
                    endpoint, sessions[0].ResumeToken, cancellationToken).ConfigureAwait(false);
                downOutcome = downResult.Outcome;
                downErrorCode = downResult.ErrorCode;
                var now1 = context.TimeProvider.GetUtcNow();
                metrics.Add(new MetricSample
                {
                    Name = "rv_redis_failover_down_outcome",
                    Value = (int)downOutcome,
                    SampledAtUtc = now1
                });
                metrics.Add(new MetricSample
                {
                    Name = "rv_redis_failover_down_error_code",
                    Value = (int)(downErrorCode ?? ProtocolErrorCode.None),
                    SampledAtUtc = now1
                });

                if (downOutcome == ResumeAttemptOutcome.Success)
                {
                    errors.Add("Resume succeeded during Redis downtime; expected failure.");
                }
                else
                {
                    // 故障期间失败时，错误码应为 DependencyUnavailable。
                    // ResumeFailed 表示 Token 校验成功但 Redis 不可用被误判为 Token 无效，违反 P1-B 设计。
                    if (downErrorCode == ProtocolErrorCode.DependencyUnavailable)
                    {
                        downDependencyUnavailable = true;
                    }
                    else
                    {
                        errors.Add(
                            $"Down-phase failure expected DependencyUnavailable, got " +
                            $"errorCode={downErrorCode}, msg={downResult.ErrorMessage}");
                    }
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
            var now2 = context.TimeProvider.GetUtcNow();
            metrics.Add(new MetricSample
            {
                Name = "rv_redis_failover_recovery_outcome",
                Value = (int)recoveryResult.Outcome,
                SampledAtUtc = now2
            });

            var recoveryPassed = recoveryResult.Outcome == ResumeAttemptOutcome.Success;
            if (!recoveryPassed)
            {
                errors.Add($"Resume after recovery failed: {recoveryResult.ErrorMessage}");
            }

            // P1-E：恢复后用无效 Token 验证错误码为 ResumeFailed（而非 DependencyUnavailable）。
            // 这证明 Redis 恢复正常后，Token 校验路径正常工作，且客户端可区分：
            // - DependencyUnavailable（Redis 故障，可重试）
            // - ResumeFailed（Token 无效，需完整认证）
            var invalidProbeResult = await TryResumeWithTimeoutAsync(
                endpoint, InvalidTokenProbe, cancellationToken).ConfigureAwait(false);
            var invalidProbePassed = invalidProbeResult.Outcome != ResumeAttemptOutcome.Success
                && invalidProbeResult.ErrorCode == ProtocolErrorCode.ResumeFailed;
            metrics.Add(new MetricSample
            {
                Name = "rv_redis_failover_recovery_invalid_token_outcome",
                Value = (int)invalidProbeResult.Outcome,
                SampledAtUtc = now2
            });
            metrics.Add(new MetricSample
            {
                Name = "rv_redis_failover_recovery_invalid_token_error_code",
                Value = (int)(invalidProbeResult.ErrorCode ?? ProtocolErrorCode.None),
                SampledAtUtc = now2
            });
            if (!invalidProbePassed)
            {
                errors.Add(
                    $"Recovery invalid-token probe expected ResumeFailed, got " +
                    $"outcome={invalidProbeResult.Outcome}, " +
                    $"errorCode={invalidProbeResult.ErrorCode}, " +
                    $"msg={invalidProbeResult.ErrorMessage}");
            }

            metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                .ConfigureAwait(false));

            // 通过条件：
            // 1. 故障期间未成功（严格）—— 若成功说明 Redis 未真正故障或熔断器未生效
            // 2. 故障期间失败时返回 DependencyUnavailable（验证错误码区分）
            // 3. 恢复后有效 Token 成功
            // 4. 恢复后无效 Token 返回 ResumeFailed（验证错误码区分）
            var downPhaseOk = downDelay == 0 || downOutcome != ResumeAttemptOutcome.Success;
            var downErrorCodeOk = downDelay == 0 || downDependencyUnavailable;
            var passed = downPhaseOk && downErrorCodeOk && recoveryPassed && invalidProbePassed;
            var summary = $"down={downOutcome}(code={downErrorCode}), " +
                          $"recovery={recoveryResult.Outcome}, " +
                          $"invalid_token={invalidProbeResult.Outcome}" +
                          $"(code={invalidProbeResult.ErrorCode})";
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
