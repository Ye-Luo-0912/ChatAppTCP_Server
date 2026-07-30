using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 5：reconnect-storm。
/// 认证大量会话（storm-size）并捕获 ResumeToken，全部断开后并发尝试 Resume，
/// 使用有界并发控制，验证 &gt;95% Resume 在截止时间内成功，并采集收敛时长。
/// <para>
/// Phase F 增强：支持 10,000 客户端重连风暴：
/// <list type="bullet">
/// <item>并发度与截止时间随 storm-size 自适应缩放；</item>
/// <item>采集完整 <see cref="ResumeAttemptResult"/>（含错误码），统计
/// <see cref="ProtocolErrorCode.DependencyUnavailable"/> 与 <see cref="ProtocolErrorCode.ResumeFailed"/>
/// 分布，验证大规模风暴下无 Token 校验异常；</item>
/// <item>分离认证/恢复阶段时长与吞吐指标。</item>
/// </list>
/// </para>
/// </summary>
internal sealed class ReconnectStormScenario : IResumeScenario
{
    /// <summary>基础并发度上限。storm-size 较大时按比例放大。</summary>
    private const int BaseConcurrency = 256;

    /// <summary>最大并发度上限，防止 10k 风暴压垮测试客户端自身。</summary>
    private const int MaxConcurrencyCap = 512;

    /// <summary>基础截止时间（秒）。storm-size 较大时按比例放大。</summary>
    private const int BaseDeadlineSeconds = 30;

    /// <summary>最大截止时间上限（秒），防止超大规模风暴无限等待。</summary>
    private const int MaxDeadlineSeconds = 180;

    /// <summary>每增加这么多客户端，截止时间延长 1 秒。</summary>
    private const int ClientsPerExtraSecond = 200;

    public string Name => "reconnect-storm";

    public async Task<ScenarioResult> ExecuteAsync(
        ResumeScenarioContext context,
        CancellationToken cancellationToken)
    {
        var startedAt = Stopwatch.StartNew();
        var endpoints = context.GatewayEndpoints;
        var stormSize = context.Options.StormSize;
        var errors = new List<string>();
        var metrics = new List<MetricSample>();

        // Phase F：自适应并发度与截止时间。
        // 1k → 256 并发 / 30s；5k → 512 并发 / 55s；10k → 512 并发 / 80s。
        var maxConcurrency = Math.Min(
            MaxConcurrencyCap,
            Math.Max(BaseConcurrency, stormSize / 8));
        var stormDeadline = TimeSpan.FromSeconds(Math.Min(
            MaxDeadlineSeconds,
            BaseDeadlineSeconds + stormSize / ClientsPerExtraSecond));

        var sessions = new List<(long UserId, string ResumeToken, ResumeTokenBootstrap Bootstrap)>();
        var authSemaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        var resumeSemaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        try
        {
            // 阶段 1：有界并发认证 stormSize 个会话。
            var authStartedAt = Stopwatch.GetTimestamp();
            var authTasks = new List<Task<(long UserId, string? ResumeToken, ResumeTokenBootstrap? Bootstrap, string? Error)>>();
            for (var i = 0; i < stormSize; i++)
            {
                var userId = context.Options.BootstrapUserIdStart + i;
                authTasks.Add(AuthenticateOneAsync(
                    endpoints[0], userId, context, authSemaphore, cancellationToken));
            }

            var authResults = await Task.WhenAll(authTasks).ConfigureAwait(false);
            var authElapsed = Stopwatch.GetElapsedTime(authStartedAt);
            var authSuccessCount = 0;
            var authFailureCount = 0;
            foreach (var result in authResults)
            {
                if (result.ResumeToken is null || result.Bootstrap is null)
                {
                    authFailureCount++;
                    if (result.Error is not null && errors.Count < 20)
                    {
                        errors.Add($"User {result.UserId}: {result.Error}");
                    }
                    continue;
                }
                authSuccessCount++;
                sessions.Add((result.UserId, result.ResumeToken, result.Bootstrap));
            }

            var authNow = context.TimeProvider.GetUtcNow();
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_auth_total", Value = stormSize, SampledAtUtc = authNow });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_auth_success", Value = authSuccessCount, SampledAtUtc = authNow });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_auth_failure", Value = authFailureCount, SampledAtUtc = authNow });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_auth_seconds", Value = authElapsed.TotalSeconds, SampledAtUtc = authNow });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_auth_throughput_per_sec", Value = authSuccessCount / Math.Max(authElapsed.TotalSeconds, 0.001), SampledAtUtc = authNow });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_max_concurrency", Value = maxConcurrency, SampledAtUtc = authNow });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_deadline_seconds", Value = stormDeadline.TotalSeconds, SampledAtUtc = authNow });

            if (sessions.Count == 0)
            {
                return ScenarioHelpers.Fail(
                    Name, startedAt, "No sessions authenticated.", metrics, errors);
            }

            // 阶段 2：全部断开（认证阶段已断开，此处仅做短暂延迟）。
            await Task.Delay(
                TimeSpan.FromMilliseconds(200), context.TimeProvider, cancellationToken).ConfigureAwait(false);

            // 阶段 3：有界并发 Resume，跨网关 round-robin。
            using var stormCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            stormCts.CancelAfter(stormDeadline);

            var resumeStartedAt = Stopwatch.GetTimestamp();
            var resumeTasks = sessions.Select((session, index) => TryStormResumeAsync(
                endpoints[index % endpoints.Count],
                session.ResumeToken,
                resumeSemaphore,
                stormCts.Token)).ToArray();
            var resumeResults = await Task.WhenAll(resumeTasks).ConfigureAwait(false);
            var stormElapsed = Stopwatch.GetElapsedTime(resumeStartedAt);

            // Phase F：按结果与错误码分类统计。
            var successCount = 0;
            var failureCount = 0;
            var dependencyUnavailableCount = 0;
            var resumeFailedCount = 0;
            var otherFailureCount = 0;
            var timeoutCount = 0;
            foreach (var result in resumeResults)
            {
                if (result.Outcome == ResumeAttemptOutcome.Success)
                {
                    successCount++;
                }
                else
                {
                    failureCount++;
                    if (result.ErrorCode == ProtocolErrorCode.DependencyUnavailable)
                        dependencyUnavailableCount++;
                    else if (result.ErrorCode == ProtocolErrorCode.ResumeFailed)
                        resumeFailedCount++;
                    else if (result.ErrorMessage?.Contains("timed out", StringComparison.OrdinalIgnoreCase) == true)
                        timeoutCount++;
                    else
                        otherFailureCount++;
                }
            }

            var totalResults = resumeResults.Length;
            var successRate = (double)successCount / totalResults;
            var throughputPerSec = successCount / Math.Max(stormElapsed.TotalSeconds, 0.001);

            var now = context.TimeProvider.GetUtcNow();
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_success_total", Value = successCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_failure_total", Value = failureCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_success_rate", Value = successRate, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_converge_seconds", Value = stormElapsed.TotalSeconds, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_throughput_per_sec", Value = throughputPerSec, SampledAtUtc = now });
            // Phase F：错误码分布——验证大规模风暴下无异常 Token 校验失败。
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_dependency_unavailable", Value = dependencyUnavailableCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_resume_failed", Value = resumeFailedCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_other_failure", Value = otherFailureCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_timeout", Value = timeoutCount, SampledAtUtc = now });

            metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                .ConfigureAwait(false));

            // 通过条件：
            // 1. 成功率 > 95%
            // 2. 在截止时间内完成
            // 3. 无 ResumeFailed（Token 校验异常）——大规模风暴下 Token 不应被误判无效
            //    DependencyUnavailable 可接受（Redis 压力下熔断器可能开路），但应低于 5%
            var passed = successRate > 0.95
                && stormElapsed <= stormDeadline
                && resumeFailedCount == 0
                && dependencyUnavailableCount < totalResults * 0.05;
            var summary = $"storm={totalResults}, success={successCount} " +
                          $"({successRate:P1}), converge={stormElapsed.TotalSeconds:F2}s, " +
                          $"throughput={throughputPerSec:F1}/s, " +
                          $"failures(dep_unavailable={dependencyUnavailableCount}, " +
                          $"resume_failed={resumeFailedCount}, " +
                          $"timeout={timeoutCount}, " +
                          $"other={otherFailureCount})";

            return passed
                ? ScenarioHelpers.Pass(Name, startedAt, summary, metrics, errors)
                : ScenarioHelpers.Fail(Name, startedAt, summary, metrics, errors);
        }
        finally
        {
            authSemaphore.Dispose();
            resumeSemaphore.Dispose();
            foreach (var session in sessions)
            {
                await session.Bootstrap.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private static async Task<(long UserId, string? ResumeToken, ResumeTokenBootstrap? Bootstrap, string? Error)>
        AuthenticateOneAsync(
            GatewayEndpoint endpoint,
            long userId,
            ResumeScenarioContext context,
            SemaphoreSlim semaphore,
            CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var connection = await ResumeScenarioRunner.AuthenticateAsync(
                    endpoint, userId, context, cancellationToken)
                .ConfigureAwait(false);
            var token = connection.Session.ResumeToken;
            if (string.IsNullOrEmpty(token))
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                return (userId, null, null, "no ResumeToken issued");
            }

            var result = (userId, token!, connection.Bootstrap, (string?)null);
            // 仅释放 TCP 客户端，保留 Bootstrap 供 finally 块统一释放。
            await connection.Client.DisposeAsync().ConfigureAwait(false);
            return result;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return (userId, null, null, exception.Message);
        }
        finally
        {
            semaphore.Release();
        }
    }

    /// <summary>
    /// Phase F：返回完整 <see cref="ResumeAttemptResult"/>（含错误码），
    /// 供风暴后错误码分布统计。
    /// </summary>
    private static async Task<ResumeAttemptResult> TryStormResumeAsync(
        GatewayEndpoint endpoint,
        string resumeToken,
        SemaphoreSlim semaphore,
        CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await ResumeScenarioRunner.TryResumeAsync(
                    endpoint, resumeToken, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return new ResumeAttemptResult(
                ResumeAttemptOutcome.Failed,
                Session: null,
                ErrorMessage: "Resume timed out (storm deadline).");
        }
        catch (Exception ex)
        {
            return new ResumeAttemptResult(
                ResumeAttemptOutcome.Failed,
                Session: null,
                ErrorMessage: $"Connection error: {ex.Message}");
        }
        finally
        {
            semaphore.Release();
        }
    }
}
