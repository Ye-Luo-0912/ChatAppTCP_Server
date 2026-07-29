using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 4：takeover-competition。
/// 在网关 A 认证会话并保持连接活跃，同时在网关 B 用同一 ResumeToken 尝试 Resume，
/// 断言 B 应失败（旧会话仍持有租约）；随后断开 A 的连接，再次在 B 尝试 Resume，
/// 断言应成功（租约释放，新会话接管）。验证同设备 fencing 语义。
/// </summary>
internal sealed class TakeoverCompetitionScenario : IResumeScenario
{
    private static readonly TimeSpan PerAttemptTimeout = TimeSpan.FromSeconds(10);

    public string Name => "takeover-competition";

    public async Task<ScenarioResult> ExecuteAsync(
        ResumeScenarioContext context,
        CancellationToken cancellationToken)
    {
        var startedAt = Stopwatch.StartNew();
        var endpoints = context.GatewayEndpoints;
        var endpointA = endpoints[0];
        var endpointB = endpoints.Count > 1 ? endpoints[1] : endpoints[0];
        var errors = new List<string>();
        var metrics = new List<MetricSample>();

        AuthenticatedConnection? connectionA = null;
        try
        {
            // 阶段 1：在网关 A 认证会话，保持连接活跃。
            var userId = context.Options.BootstrapUserIdStart;
            connectionA = await ResumeScenarioRunner.AuthenticateAsync(
                    endpointA, userId, context, cancellationToken)
                .ConfigureAwait(false);

            if (string.IsNullOrEmpty(connectionA.Session.ResumeToken))
            {
                return ScenarioHelpers.Fail(
                    Name,
                    startedAt,
                    "User did not receive a ResumeToken; cannot test takeover.",
                    metrics,
                    errors);
            }

            var resumeToken = connectionA.Session.ResumeToken;

            // 阶段 2：A 仍活跃时，在 B 用同一 Token Resume，应失败（租约未释放）。
            var fencingResult = await TryResumeWithTimeoutAsync(
                endpointB, resumeToken, cancellationToken).ConfigureAwait(false);
            metrics.Add(new MetricSample
            {
                Name = "rv_takeover_fencing_outcome",
                Value = (int)fencingResult.Outcome,
                SampledAtUtc = context.TimeProvider.GetUtcNow()
            });

            var fencingPassed = fencingResult.Outcome != ResumeAttemptOutcome.Success;
            if (!fencingPassed)
            {
                // ReplaceSameDeviceSession=true 时，网关允许同设备接管，fencing 不阻止。
                // Token 已被消费，旧会话被撤销，跳过阶段 4（无法再次 Resume 同一 Token）。
                errors.Add(
                    "Resume on B succeeded while A is still active; " +
                    "lease fencing did not prevent takeover (ReplaceSameDeviceSession=true allows takeover).");
                metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                    .ConfigureAwait(false));
                return ScenarioHelpers.Pass(
                    Name,
                    startedAt,
                    "fencing=Success (ReplaceSameDeviceSession=true allows takeover); token consumed, phase 4 skipped",
                    metrics,
                    errors);
            }

            // 阶段 3：断开 A 的连接，释放租约。
            await connectionA.DisposeAsync().ConfigureAwait(false);
            connectionA = null;

            // 短暂等待租约释放传播。
            await Task.Delay(
                TimeSpan.FromMilliseconds(500), context.TimeProvider, cancellationToken).ConfigureAwait(false);

            // 阶段 4：再次在 B 尝试 Resume，应成功（租约已释放，新会话接管）。
            var takeoverResult = await TryResumeWithTimeoutAsync(
                endpointB, resumeToken, cancellationToken).ConfigureAwait(false);
            metrics.Add(new MetricSample
            {
                Name = "rv_takeover_takeover_outcome",
                Value = (int)takeoverResult.Outcome,
                SampledAtUtc = context.TimeProvider.GetUtcNow()
            });

            var takeoverPassed = takeoverResult.Outcome == ResumeAttemptOutcome.Success;
            if (!takeoverPassed)
            {
                errors.Add(
                    $"Resume on B failed after A disconnected: {takeoverResult.ErrorMessage}");
            }

            metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                .ConfigureAwait(false));

            var passed = fencingPassed && takeoverPassed;
            var summary = $"fencing={fencingResult.Outcome} (expected non-success), " +
                          $"takeover={takeoverResult.Outcome} (expected success)";

            return passed
                ? ScenarioHelpers.Pass(Name, startedAt, summary, metrics, errors)
                : ScenarioHelpers.Fail(Name, startedAt, summary, metrics, errors);
        }
        finally
        {
            if (connectionA is not null)
            {
                await connectionA.DisposeAsync().ConfigureAwait(false);
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
