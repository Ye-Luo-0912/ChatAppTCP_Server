using System.Diagnostics;
using System.Net.Sockets;
using ChatApp.ResumeVerification.Diagnostics;
using ChatApp.ResumeVerification.Runtime;

namespace ChatApp.ResumeVerification.Scenarios;

/// <summary>
/// 场景 4：takeover-competition。
/// 在网关 A 认证会话并保持连接活跃，同时在网关 B 用同一 ResumeToken 尝试 Resume。
/// <para>
/// 两种路径：
/// <list type="bullet">
/// <item><term>ReplaceSameDeviceSession=false（fencing 严格模式）</term>
///   <description>B 应失败（旧会话仍持有租约）；随后断开 A，再次在 B Resume 应成功。</description></item>
/// <item><term>ReplaceSameDeviceSession=true（默认，允许同设备接管）</term>
///   <description>B Resume 成功后，Phase G 断言：保留旧 Socket A，验证其读写均失败
///   （旧 TCP 连接已被网关关闭），确保 TakeOver 形成完整闭环——旧 Transport 无法继续发送
///   Heartbeat 或接收消息。</description></item>
/// </list>
/// </para>
/// </summary>
internal sealed class TakeoverCompetitionScenario : IResumeScenario
{
    private static readonly TimeSpan PerAttemptTimeout = TimeSpan.FromSeconds(10);

    /// <summary>旧连接关闭探测的总体超时。网关关闭旧连接需要本机 RevokeSessionAsync
    /// 或跨 Gateway NATS SessionRevoked 事件往返，给足时间。</summary>
    private static readonly TimeSpan OldSocketProbeTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Resume 成功后等待网关关闭旧连接的缓冲时间。</summary>
    private static readonly TimeSpan TakeoverPropagationDelay = TimeSpan.FromMilliseconds(800);

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

            // 阶段 2：A 仍活跃时，在 B 用同一 Token Resume。
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
                // ReplaceSameDeviceSession=true：网关允许同设备接管，B Resume 成功。
                // Phase G：不立即返回，保留旧 Socket A 并执行读写断言，验证 TakeOver 闭环。
                // Token 已被消费，旧会话应被网关关闭。
                // 注意：errors 中仅记录现象，不作为失败条件——接管被允许是配置选择。
                errors.Add(
                    "Resume on B succeeded while A is still active; " +
                    "lease fencing did not prevent takeover (ReplaceSameDeviceSession=true allows takeover).");

                // Phase G：等待网关关闭旧连接（本机 RevokeSessionAsync 或跨 Gateway NATS 事件）。
                await Task.Delay(
                    TakeoverPropagationDelay, context.TimeProvider, cancellationToken).ConfigureAwait(false);

                // Phase G：对旧 Socket A 执行读写断言。
                var probeResult = await ProbeOldConnectionClosedAsync(
                    connectionA.Client, context, cancellationToken).ConfigureAwait(false);

                var probeNow = context.TimeProvider.GetUtcNow();
                metrics.Add(new MetricSample
                {
                    Name = "rv_takeover_old_socket_read_closed",
                    Value = probeResult.ReadClosed ? 1 : 0,
                    SampledAtUtc = probeNow
                });
                metrics.Add(new MetricSample
                {
                    Name = "rv_takeover_old_socket_write_closed",
                    Value = probeResult.WriteClosed ? 1 : 0,
                    SampledAtUtc = probeNow
                });
                metrics.Add(new MetricSample
                {
                    Name = "rv_takeover_old_socket_probe_elapsed_ms",
                    Value = probeResult.ElapsedMs,
                    SampledAtUtc = probeNow
                });

                // 通过条件：旧连接的读和写都已关闭。
                // 这证明 TakeOver 形成完整闭环——旧 Transport 无法继续发送 Heartbeat 或接收消息。
                var oldSocketClosed = probeResult.ReadClosed && probeResult.WriteClosed;
                if (!oldSocketClosed)
                {
                    errors.Add(
                        $"Old socket A not fully closed after takeover: " +
                        $"read_closed={probeResult.ReadClosed}, " +
                        $"write_closed={probeResult.WriteClosed}, " +
                        $"detail={probeResult.Detail}");
                }

                metrics.AddRange(await ScenarioHelpers.SampleMetricsAsync(context, cancellationToken)
                    .ConfigureAwait(false));

                var takeoverSummary = oldSocketClosed
                    ? "fencing=Success (ReplaceSameDeviceSession=true), old_socket=closed (read+write failed)"
                    : $"fencing=Success, old_socket=NOT closed (read={probeResult.ReadClosed}, write={probeResult.WriteClosed})";
                return oldSocketClosed
                    ? ScenarioHelpers.Pass(Name, startedAt, takeoverSummary, metrics, errors)
                    : ScenarioHelpers.Fail(Name, startedAt, takeoverSummary, metrics, errors);
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

    /// <summary>
    /// Phase G：探测旧连接是否已被网关关闭。
    /// <para>
    /// 执行两项断言：
    /// <list type="number">
    /// <item><term>读断言</term>
    ///   <description>尝试在旧 Socket 上接收帧。若抛出 <see cref="EndOfStreamException"/>
    ///   或 <see cref="IOException"/> 或 <see cref="SocketException"/>，说明网关已关闭连接
    ///   （<c>ReadClosed=true</c>）。若成功收到帧，说明旧连接仍可接收消息（闭环未完成）。</description></item>
    /// <item><term>写断言</term>
    ///   <description>尝试在旧 Socket 上发送 Heartbeat。若抛出 <see cref="IOException"/>
    ///   或 <see cref="SocketException"/>，说明网关已关闭连接的写端
    ///   （<c>WriteClosed=true</c>）。</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// 两项均失败（连接已关闭）时 <c>ReadClosed &amp;&amp; WriteClosed = true</c>，
    /// 证明 TakeOver 闭环完成：旧 Transport 无法继续发送 Heartbeat 或接收消息。
    /// </para>
    /// </summary>
    private static async Task<OldSocketProbeResult> ProbeOldConnectionClosedAsync(
        ResumeCapableProtocolClient client,
        ResumeScenarioContext context,
        CancellationToken cancellationToken)
    {
        var readClosed = false;
        var writeClosed = false;
        var detail = string.Empty;
        var probeStartedAt = Stopwatch.GetTimestamp();

        // 读断言：尝试接收帧，短超时。
        using var readCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        readCts.CancelAfter(OldSocketProbeTimeout);
        try
        {
            var frame = await client.ReceiveFrameAsync(readCts.Token).ConfigureAwait(false);
            // 成功收到帧——旧连接仍可接收消息，闭环未完成。
            detail = $"received frame {frame.Command} on old socket (expected connection closed)";
        }
        catch (OperationCanceledException) when (readCts.IsCancellationRequested &&
                                                  !cancellationToken.IsCancellationRequested)
        {
            // 读超时——无法判断连接是否关闭（可能只是没有数据）。
            // 不标记 ReadClosed，但也不报错；写断言仍可判定。
            detail = "read timed out";
        }
        catch (EndOfStreamException)
        {
            // 网关关闭了连接——读端已关闭。
            readClosed = true;
        }
        catch (IOException)
        {
            // Socket 异常——读端已关闭。
            readClosed = true;
        }
        catch (SocketException)
        {
            // Socket 异常——读端已关闭。
            readClosed = true;
        }

        // 写断言：尝试发送 Heartbeat，短超时。
        using var writeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        writeCts.CancelAfter(OldSocketProbeTimeout);
        try
        {
            await client.SendHeartbeatAsync(writeCts.Token).ConfigureAwait(false);
            // 发送成功——旧连接写端仍可用，闭环未完成。
            // 注意：TCP 发送可能成功即使远端已关闭（FIN 只影响读），需结合读断言判断。
            if (!readClosed)
            {
                detail = detail.Length > 0
                    ? detail + "; heartbeat send succeeded on old socket"
                    : "heartbeat send succeeded on old socket";
            }
        }
        catch (OperationCanceledException) when (writeCts.IsCancellationRequested &&
                                                  !cancellationToken.IsCancellationRequested)
        {
            // 写超时——Socket 发送缓冲区可能满或连接半关闭。
            // 结合读断言判断：如果读也关闭了，连接确实已关闭。
            if (readClosed)
            {
                writeClosed = true;
            }
            detail = detail.Length > 0 ? detail + "; write timed out" : "write timed out";
        }
        catch (IOException)
        {
            // 写失败——连接写端已关闭。
            writeClosed = true;
        }
        catch (SocketException)
        {
            // 写失败——连接写端已关闭。
            writeClosed = true;
        }

        var elapsedMs = Stopwatch.GetElapsedTime(probeStartedAt).TotalMilliseconds;
        return new OldSocketProbeResult(readClosed, writeClosed, elapsedMs, detail);
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

/// <summary>
/// Phase G：旧 Socket 探测结果。
/// </summary>
/// <param name="ReadClosed">读端是否已关闭（收到 EndOfStream/IO/Socket 异常）。</param>
/// <param name="WriteClosed">写端是否已关闭（发送 Heartbeat 抛出 IO/Socket 异常）。</param>
/// <param name="ElapsedMs">探测耗时（毫秒）。</param>
/// <param name="Detail">附加诊断信息。</param>
internal sealed record OldSocketProbeResult(
    bool ReadClosed,
    bool WriteClosed,
    double ElapsedMs,
    string Detail);
