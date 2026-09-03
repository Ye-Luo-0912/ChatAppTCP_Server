using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Transport;

/// <summary>
/// TCP 监听器宿主：封装 Socket Listener 生命周期、Accept 循环、连接准入与 drain 协调。
/// <para>
/// 由 <see cref="TcpGatewayService"/> 持有并驱动；不实现 IHostedService 以避免启动顺序复杂性。
/// Accept 成功且准入通过后，通过 <paramref name="onConnectionAccepted"/> 回调将
/// (connectionId, socket, remoteIp) 交给上层服务创建 session 并启动 HandleClientAsync。
/// 回调返回的 <see cref="Task"/> 由本类型注册到 <c>_clientTasks</c> 用于停机 WhenAll。
/// 回调返回 <c>null</c> 表示 session 创建失败（如 connectionId 冲突），本类型会回滚准入与槽位。
/// </para>
/// </summary>
internal sealed class TcpListenerHost : IDisposable
{
    private readonly TcpGatewayOptions _options;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;
    private readonly IPayloadCodec<GoAway>? _goAwayCodec;
    private readonly Func<IEnumerable<TcpClientSession>> _getSessions;
    private readonly Func<uint, Socket, string, CancellationToken, ValueTask<Task?>> _onConnectionAccepted;

    private readonly SemaphoreSlim _connectionSlots;
    private readonly ConnectionAdmissionTracker _admissionTracker;
    private readonly ConcurrentDictionary<uint, Task> _clientTasks = new();

    private readonly TaskCompletionSource _listenerReady = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private Socket? _listener;
    private CancellationTokenSource? _acceptLoopCts;
    private int _isDraining;
    private uint _connectionId;

    public TcpListenerHost(
        TcpGatewayOptions options,
        GatewayMetrics metrics,
        ILogger logger,
        IPayloadCodec<GoAway>? goAwayCodec,
        Func<IEnumerable<TcpClientSession>> getSessions,
        Func<uint, Socket, string, CancellationToken, ValueTask<Task?>> onConnectionAccepted)
    {
        _options = options;
        _metrics = metrics;
        _logger = logger;
        _goAwayCodec = goAwayCodec;
        _getSessions = getSessions;
        _onConnectionAccepted = onConnectionAccepted;

        _connectionSlots = new SemaphoreSlim(
            _options.MaxConnections,
            _options.MaxConnections);

        _admissionTracker = new ConnectionAdmissionTracker(
            _options.MaxUnauthenticatedConnections,
            _options.MaxConnectionsPerIp,
            _options.MaxAuthenticationAttemptsPerIp,
            _options.AuthenticationRateWindow);
    }

    /// <summary>
    /// 监听器就绪 Task：Bind/Listen 成功后完成，失败时 faulted。
    /// 由 <see cref="TcpGatewayService.StartAsync"/> 等待。
    /// </summary>
    public Task ListenerReady => _listenerReady.Task;

    /// <summary>是否处于 draining 状态（停止接入新连接）。</summary>
    public bool IsDraining => Volatile.Read(ref _isDraining) != 0;

    /// <summary>
    /// Bind/Listen → Accept 循环 → drain 等待。在 executionToken 取消时退出。
    /// <para>
    /// Bind 失败时直接抛出（不记录 GatewayFatal）；
    /// Accept 循环中的非取消异常会记录 GatewayFatal 并设置 ExitCode=1 后抛出。
    /// </para>
    /// </summary>
    public async Task RunAsync(CancellationToken executionToken)
    {
        var endpoint = new IPEndPoint(
            IPAddress.Parse(_options.ListenAddress),
            _options.Port);

        var listener = new Socket(
            endpoint.AddressFamily,
            SocketType.Stream,
            ProtocolType.Tcp);

        listener.SetSocketOption(
            SocketOptionLevel.Socket,
            SocketOptionName.ReuseAddress,
            optionValue: true);
        try
        {
            listener.Bind(endpoint);
            listener.Listen(_options.ListenBacklog);
        }
        catch (Exception exception)
        {
            listener.Dispose();
            _listenerReady.TrySetException(exception);
            throw;
        }

        Volatile.Write(ref _listener, listener);
        _listenerReady.TrySetResult();

        using var acceptLoopCts = CancellationTokenSource.CreateLinkedTokenSource(
            executionToken);
        Volatile.Write(ref _acceptLoopCts, acceptLoopCts);

        _logger.GatewayStarted(endpoint, _options.MaxConnections);

        try
        {
            while (!executionToken.IsCancellationRequested)
            {
                if (Volatile.Read(ref _isDraining) != 0 ||
                    acceptLoopCts.IsCancellationRequested)
                {
                    // 已停止接入：等待 StopAsync 完成 GoAway 排空后再由 base.StopAsync 取消 executionToken。
                    try
                    {
                        await Task.Delay(Timeout.Infinite, executionToken)
                            .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                        when (executionToken.IsCancellationRequested)
                    {
                    }

                    break;
                }

                Socket socket;
                try
                {
                    socket = await listener
                        .AcceptAsync(acceptLoopCts.Token)
                        .ConfigureAwait(false);
                }
                catch (ObjectDisposedException)
                {
                    // StopAsync 已关闭 listener；转入排空等待。
                    continue;
                }
                catch (OperationCanceledException)
                    when (acceptLoopCts.IsCancellationRequested &&
                          !executionToken.IsCancellationRequested)
                {
                    // Accept 已取消但仍在 draining；转入排空等待。
                    continue;
                }
                catch (SocketException) when (Volatile.Read(ref _isDraining) != 0 ||
                                              acceptLoopCts.IsCancellationRequested)
                {
                    continue;
                }

                if (Volatile.Read(ref _isDraining) != 0)
                {
                    socket.Dispose();
                    continue;
                }

                if (!await _connectionSlots.WaitAsync(0, CancellationToken.None))
                {
                    _metrics.ConnectionRejected();
                    socket.Dispose();
                    continue;
                }

                // 提取远程 IP 用于准入检查。
                string remoteIp = "unknown";
                try
                {
                    if (socket.RemoteEndPoint is IPEndPoint ep)
                        remoteIp = ep.Address.ToString();
                }
                catch
                {
                    // 获取失败时用 "unknown" 作为 key，仍受全局未认证限制。
                }

                // 连接准入检查（未认证数 + 每 IP 连接数 + 每 IP 认证失败率）。
                var admission = _admissionTracker.TryAdmit(remoteIp);
                if (admission != AdmissionResult.Admitted)
                {
                    _metrics.ConnectionRejected();
                    switch (admission)
                    {
                        case AdmissionResult.RejectedUnauthenticatedLimit:
                            _metrics.ConnectionRejectedUnauthLimit();
                            break;
                        case AdmissionResult.RejectedPerIpConnectionLimit:
                            _metrics.ConnectionRejectedPerIpLimit();
                            break;
                        case AdmissionResult.RejectedPerIpAuthRateLimit:
                            _metrics.AuthenticationRejectedPerIpRate();
                            break;
                    }
                    socket.Dispose();
                    _connectionSlots.Release();
                    continue;
                }

                _metrics.UnauthenticatedConnectionAccepted();

                var connectionId = NextConnectionId();
                Task? clientTask;
                try
                {
                    clientTask = await _onConnectionAccepted(
                            connectionId, socket, remoteIp, executionToken)
                        .ConfigureAwait(false);
                }
                catch
                {
                    socket.Dispose();
                    _connectionSlots.Release();
                    _admissionTracker.Release(remoteIp, wasAuthenticated: false);
                    _metrics.UnauthenticatedConnectionClosed();
                    throw;
                }

                if (clientTask is null)
                {
                    // Session 创建失败（如 connectionId 冲突）；释放准入与槽位。
                    _connectionSlots.Release();
                    _admissionTracker.Release(remoteIp, wasAuthenticated: false);
                    _metrics.UnauthenticatedConnectionClosed();
                    continue;
                }

                _clientTasks[connectionId] = clientTask;
                _ = clientTask.ContinueWith(
                    static (completedTask, state) =>
                    {
                        var context = (ClientTaskContext)state!;
                        context.Tasks.TryRemove(
                            context.ConnectionId,
                            out _);
                    },
                    new ClientTaskContext(_clientTasks, connectionId),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }
        catch (OperationCanceledException)
            when (executionToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
        catch (Exception exception)
        {
            Environment.ExitCode = 1;
            _logger.GatewayFatal(exception);
            throw;
        }
        finally
        {
            Volatile.Write(ref _acceptLoopCts, null);
            Volatile.Write(ref _listener, null);
            listener.Dispose();
        }
    }

    /// <summary>
    /// 触发 drain：设置 _isDraining，取消 Accept，关闭 listener，广播 GoAway 并等待 drain 超时。
    /// 由 <see cref="TcpGatewayService.StopAsync"/> 在调用 base.StopAsync 之前调用。
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // 1. 先进入 draining：停止接入新连接，再通知已有连接排空。
        Interlocked.Exchange(ref _isDraining, 1);
        try
        {
            _acceptLoopCts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Accept loop 可能已结束。
        }

        var listener = Interlocked.Exchange(ref _listener, null);
        listener?.Dispose();

        // 2. 优雅停机：通知所有活跃连接重连其他实例。
        if (_goAwayCodec is not null)
        {
            var sessions = _getSessions().ToList();
            if (sessions.Count != 0)
            {
                var drainTimeout = _options.GoAwayDrainTimeout;
                var goAway = new GoAway
                {
                    RetryAfterMs = (int)drainTimeout.TotalMilliseconds,
                    Reason = "shutdown",
                    ServerHint = null
                };

                foreach (var session in sessions)
                {
                    using var frame = OutboundFrameFactory.Create(
                        PacketCommand.GoAway,
                        _goAwayCodec,
                        session,
                        goAway);
                    session.TryQueue(frame);
                }

                // 等待客户端断开或超时（期间不再 Accept）。
                try
                {
                    await Task.Delay(drainTimeout, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // 停机令牌已取消，立即进入强制关闭流程。
                }
            }
        }
    }

    /// <summary>
    /// 等待所有活跃连接 Task 完成（drain）。
    /// 由 <see cref="TcpGatewayService"/> 在关闭所有 session 后调用。
    /// </summary>
    public async Task WaitForClientTasksAsync()
    {
        var activeTasks = _clientTasks.Values.ToArray();
        if (activeTasks.Length != 0)
        {
            await Task.WhenAll(activeTasks).ConfigureAwait(false);
        }
    }

    /// <summary>连接结束后归还 connection slot（由 service 在 HandleClientAsync finally 调用）。</summary>
    public void ReleaseConnectionSlot()
    {
        _connectionSlots.Release();
    }

    /// <summary>连接结束后释放准入跟踪器槽位（由 service 在 HandleClientAsync finally 调用）。</summary>
    public void ReleaseAdmission(string remoteIp, bool wasAuthenticated)
    {
        _admissionTracker.Release(remoteIp, wasAuthenticated);
    }

    /// <summary>记录一次认证失败（由 service 在 HandleAuthenticationAsync / HandleClientHelloAsync 调用）。</summary>
    public void RecordAuthenticationFailure(string remoteIp)
    {
        _admissionTracker.RecordAuthenticationFailure(remoteIp);
    }

    /// <summary>标记连接已认证成功，递减未认证计数（由 service 在认证成功路径调用）。</summary>
    public void MarkAuthenticated()
    {
        _admissionTracker.MarkAuthenticated();
    }

    /// <summary>清理已过期的认证失败桶与零计数 IP 条目（由 service 心跳循环调用）。</summary>
    public void SweepAdmission()
    {
        _admissionTracker.SweepExpiredEntries(DateTimeOffset.UtcNow);
    }

    private uint NextConnectionId()
    {
        while (true)
        {
            var next = Interlocked.Increment(ref _connectionId);
            if (next != 0)
            {
                return next;
            }
        }
    }

    public void Dispose()
    {
        Volatile.Read(ref _listener)?.Dispose();
        _connectionSlots.Dispose();
    }

    private sealed record ClientTaskContext(
        ConcurrentDictionary<uint, Task> Tasks,
        uint ConnectionId);
}
