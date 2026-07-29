using System.Diagnostics;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Scenarios;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.ResumeVerification.Runtime;

/// <summary>
/// 场景运行器：顺序执行场景，捕获异常并采集时长。同时提供场景共用的认证辅助方法。
/// </summary>
internal static class ResumeScenarioRunner
{
    /// <summary>
    /// 默认协商的能力位：命令能力门控 + 会话恢复。
    /// </summary>
    public const uint DefaultFeatureBits =
        (uint)(GatewayFeature.CommandCapabilities | GatewayFeature.SessionResume);

    /// <summary>
    /// 顺序执行场景列表，返回每个场景的结果。单个场景抛出异常时记为失败并继续后续场景。
    /// </summary>
    public static async Task<List<ScenarioResult>> RunAsync(
        IReadOnlyList<IResumeScenario> scenarios,
        ResumeScenarioContext context,
        CancellationToken cancellationToken)
    {
        var results = new List<ScenarioResult>(scenarios.Count);
        foreach (var scenario in scenarios)
        {
            var startedAt = Stopwatch.GetTimestamp();
            ScenarioResult result;
            try
            {
                result = await scenario
                    .ExecuteAsync(context, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                var elapsed = Stopwatch.GetElapsedTime(startedAt);
                result = new ScenarioResult
                {
                    Name = scenario.Name,
                    Passed = false,
                    Summary = $"Scenario threw: {exception.Message}",
                    Metrics = new List<MetricSample>(),
                    Errors = new List<string> { exception.ToString() },
                    DurationSeconds = elapsed.TotalSeconds
                };
            }

            results.Add(result);
            Console.WriteLine(
                $"[{scenario.Name}] {(result.Passed ? "PASSED" : "FAILED")} - {result.Summary}");
        }

        return results;
    }

    /// <summary>
    /// 在指定网关上完成握手 + 认证，返回已认证连接（含 ResumeToken）。
    /// 调用方负责释放返回的 <see cref="AuthenticatedConnection"/>。
    /// </summary>
    public static async Task<AuthenticatedConnection> AuthenticateAsync(
        GatewayEndpoint endpoint,
        long userId,
        ResumeScenarioContext context,
        CancellationToken cancellationToken)
    {
        var bootstrap = await context
            .BootstrapFactory(userId, cancellationToken)
            .ConfigureAwait(false);

        var client = new ResumeCapableProtocolClient();
        try
        {
            await client.ConnectAsync(
                    endpoint.Host,
                    endpoint.Port,
                    cancellationToken)
                .ConfigureAwait(false);
            await client.HandshakeAsync(DefaultFeatureBits, cancellationToken)
                .ConfigureAwait(false);
            var session = await client.AuthenticateAsync(
                    bootstrap.Token,
                    deviceIdHash: null,
                    cancellationToken)
                .ConfigureAwait(false);

            return new AuthenticatedConnection(client, session, bootstrap);
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            await bootstrap.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// 尝试在指定网关上用 ResumeToken 恢复会话，返回尝试结果。连接在返回前已释放。
    /// </summary>
    public static async Task<ResumeAttemptResult> TryResumeAsync(
        GatewayEndpoint endpoint,
        string resumeToken,
        CancellationToken cancellationToken)
    {
        await using var client = new ResumeCapableProtocolClient();
        await client.ConnectAsync(
                endpoint.Host,
                endpoint.Port,
                cancellationToken)
            .ConfigureAwait(false);
        return await client.ResumeAsync(
                resumeToken,
                DefaultFeatureBits,
                cancellationToken)
            .ConfigureAwait(false);
    }
}

/// <summary>
/// 已认证连接，聚合客户端、会话信息与 Redis 引导，便于场景统一释放。
/// </summary>
internal sealed record AuthenticatedConnection(
    ResumeCapableProtocolClient Client,
    AuthenticatedSession Session,
    ResumeTokenBootstrap Bootstrap) : IAsyncDisposable
{
    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync().ConfigureAwait(false);
        await Bootstrap.DisposeAsync().ConfigureAwait(false);
    }
}
