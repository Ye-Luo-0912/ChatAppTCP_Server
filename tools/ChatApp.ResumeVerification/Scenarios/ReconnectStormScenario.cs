using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 5：reconnect-storm。
/// 认证大量会话（storm-size）并捕获 ResumeToken，全部断开后并发尝试 Resume，
/// 使用有界并发控制，验证 &gt;95% Resume 在 30 秒内成功，并采集收敛时长。
/// </summary>
internal sealed class ReconnectStormScenario : IResumeScenario
{
    private const int MaxConcurrency = 256;
    private static readonly TimeSpan StormDeadline = TimeSpan.FromSeconds(30);

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

        var sessions = new List<(long UserId, string ResumeToken, ResumeTokenBootstrap Bootstrap)>();
        var semaphore = new SemaphoreSlim(MaxConcurrency, MaxConcurrency);
        try
        {
            // 阶段 1：有界并发认证 stormSize 个会话。
            var authTasks = new List<Task<(long UserId, string? ResumeToken, ResumeTokenBootstrap? Bootstrap, string? Error)>>();
            for (var i = 0; i < stormSize; i++)
            {
                var userId = context.Options.BootstrapUserIdStart + i;
                authTasks.Add(AuthenticateOneAsync(
                    endpoints[0], userId, context, semaphore, cancellationToken));
            }

            var authResults = await Task.WhenAll(authTasks).ConfigureAwait(false);
            foreach (var result in authResults)
            {
                if (result.ResumeToken is null || result.Bootstrap is null)
                {
                    if (result.Error is not null)
                    {
                        errors.Add($"User {result.UserId}: {result.Error}");
                    }
                    continue;
                }
                sessions.Add((result.UserId, result.ResumeToken, result.Bootstrap));
            }

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
            stormCts.CancelAfter(StormDeadline);

            var resumeStartedAt = Stopwatch.GetTimestamp();
            var resumeTasks = sessions.Select((session, index) => TryStormResumeAsync(
                endpoints[index % endpoints.Count],
                session.ResumeToken,
                semaphore,
                stormCts.Token)).ToArray();
            var resumeResults = await Task.WhenAll(resumeTasks).ConfigureAwait(false);
            var stormElapsed = Stopwatch.GetElapsedTime(resumeStartedAt);

            var successCount = resumeResults.Count(r => r == ResumeAttemptOutcome.Success);
            var failureCount = resumeResults.Length - successCount;
            var successRate = (double)successCount / resumeResults.Length;

            var now = context.TimeProvider.GetUtcNow();
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_success_total", Value = successCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_failure_total", Value = failureCount, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_success_rate", Value = successRate, SampledAtUtc = now });
            metrics.Add(new MetricSample { Name = "rv_reconnect_storm_converge_seconds", Value = stormElapsed.TotalSeconds, SampledAtUtc = now });

            metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                .ConfigureAwait(false));

            var passed = successRate > 0.95 && stormElapsed <= StormDeadline;
            var summary = $"storm={resumeResults.Length}, success={successCount} " +
                          $"({successRate:P1}), converge={stormElapsed.TotalSeconds:F2}s";

            return passed
                ? ScenarioHelpers.Pass(Name, startedAt, summary, metrics, errors)
                : ScenarioHelpers.Fail(Name, startedAt, summary, metrics, errors);
        }
        finally
        {
            semaphore.Dispose();
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

    private static async Task<ResumeAttemptOutcome> TryStormResumeAsync(
        GatewayEndpoint endpoint,
        string resumeToken,
        SemaphoreSlim semaphore,
        CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var result = await ResumeScenarioRunner.TryResumeAsync(
                    endpoint, resumeToken, cancellationToken)
                .ConfigureAwait(false);
            return result.Outcome;
        }
        catch (OperationCanceledException)
        {
            return ResumeAttemptOutcome.Failed;
        }
        catch (Exception)
        {
            return ResumeAttemptOutcome.Failed;
        }
        finally
        {
            semaphore.Release();
        }
    }
}
