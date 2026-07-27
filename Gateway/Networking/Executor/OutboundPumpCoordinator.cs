using System.Threading.Channels;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Executor;

/// <summary>
/// OnDemandSendPump 模式的共享出站 pump 协调器：单例，所有连接共享一个 ready queue + worker 池。
/// <para>
/// 取代每连接永久 <c>SendLoop</c> Task。空闲连接不占用任何 Task；仅当 <see cref="TcpClientSession.TryQueue"/>
/// 或 <see cref="TcpClientSession.TryQueueEphemeral"/> 入队后通过 <see cref="TrySchedule"/> 唤醒一个 worker。
/// </para>
/// <para>
/// 公平调度：ready queue 是 FIFO <see cref="Channel{TcpClientSession}"/>，先入先出。
/// 单连接 burst 上限由 <see cref="TcpClientSession.PumpOutboundAsync"/> 强制；
/// 达到上限后会话重新 <see cref="TrySchedule"/> 自身（排到队尾），让出 worker 给后续会话。
/// </para>
/// <para>
/// 慢消费者隔离：<see cref="TcpClientSession.SendFrameAsync"/> 已通过 <see cref="DeadlineWheel"/>
/// 注册 SendTimeout，慢 socket 会被超时关闭；多 worker 并行保证一个慢连接不阻塞其他连接。
/// </para>
/// <para>
/// 重复调度幂等：<see cref="TcpClientSession"/> 通过 <c>_sendScheduled</c> CAS 标志保证
/// 同一连接同时只在 ready queue 中存在一份引用（worker pump 结束时通过 CAS 清除标志 +
/// re-check 处理"清除标志与新入队"竞态，避免丢失唤醒或重复调度）。
/// </para>
/// </summary>
internal sealed class OutboundPumpCoordinator : IDisposable
{
    private readonly Channel<TcpClientSession> _ready;
    private readonly int _burstLimit;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private Task[] _workers = Array.Empty<Task>();

    // 可观测计数器：当前 ready queue 中待 pump 的 session 数（已 CAS 调度但 worker 尚未消费）。
    // 通过 Interlocked 维护，避免依赖 ChannelReader.Count 的实现细节。
    private long _readyQueueCount;
    // 累计成功调度的 session 次数（含重新入队的轮转）。用于 A/B 评估 OnDemandSendPump 的唤醒频率。
    private long _totalScheduled;

    public OutboundPumpCoordinator(int burstLimit, int readyQueueCapacity, ILogger logger)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(burstLimit, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(readyQueueCapacity, 0);
        _burstLimit = burstLimit;
        _logger = logger;

        _ready = Channel.CreateBounded<TcpClientSession>(
            new BoundedChannelOptions(readyQueueCapacity)
            {
                // SingleReader=false: 多 worker 并发消费。
                // SingleWriter=false: 多连接并发 TrySchedule。
                // Wait 模式在 ready queue 满时阻塞入队者；但实际上 CAS 标志保证每连接
                // 至多一份在 ready queue 中，且 readyQueueCapacity 通常 ≥ MaxConnections，
                // 因此 Wait 几乎不会触发。使用 DropWrite 会静默丢失唤醒，不可接受。
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
    }

    /// <summary>
    /// 启动 worker 池。在 listener 接受连接前调用，避免首个连接 TrySchedule 时无 worker。
    /// </summary>
    public Task StartAsync(int workerCount, CancellationToken cancellationToken)
    {
        if (_workers.Length > 0)
            return Task.CompletedTask;

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(workerCount, 0);
        var token = _cts.Token;
        _workers = new Task[workerCount];
        for (var i = 0; i < workerCount; i++)
        {
            _workers[i] = Task.Run(() => RunWorkerAsync(token), token);
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// 停止 worker 池：完成 ready channel、等待所有 worker 退出。
    /// 由 <see cref="Networking.TcpGatewayService"/> 在 ExecuteAsync finally 中调用。
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        _ready.Writer.TryComplete();

        foreach (var worker in _workers)
        {
            if (worker is null)
                continue;
            try
            {
                await worker.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Worker 异常已由 RunWorkerAsync 内部记录；此处仅等待退出。
            }
        }

        // 排空 ready queue 中残留的 session 引用（无需释放资源，session 由 HandleClientAsync 清理）。
        while (_ready.Reader.TryRead(out _)) { }
        Interlocked.Exchange(ref _readyQueueCount, 0);
    }

    /// <summary>
    /// 将 session 调度到 worker 池。调用方（<see cref="TcpClientSession"/>)必须已通过
    /// CAS <c>_sendScheduled</c> 0→1 成功，保证同一 session 不会重复入队。
    /// <para>
    /// ready channel 关闭后（停机）此方法返回 false，调用方应重置 CAS 标志以避免状态泄漏。
    /// </para>
    /// </summary>
    public bool TrySchedule(TcpClientSession session)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (!_ready.Writer.TryWrite(session))
            return false;
        Interlocked.Increment(ref _readyQueueCount);
        Interlocked.Increment(ref _totalScheduled);
        return true;
    }

    /// <summary>当前 ready queue 中待 pump 的 session 数（已调度但 worker 尚未消费）。</summary>
    public long ReadyQueueCount => Interlocked.Read(ref _readyQueueCount);

    /// <summary>累计成功调度的 session 次数（含重新入队的轮转）。</summary>
    public long TotalScheduled => Interlocked.Read(ref _totalScheduled);

    /// <summary>当前 worker 数量（启动后不变）。</summary>
    public int WorkerCount => _workers.Length;

    private async Task RunWorkerAsync(CancellationToken token)
    {
        try
        {
            await foreach (var session in _ready.Reader.ReadAllAsync(token)
                              .ConfigureAwait(false))
            {
                if (session is null)
                    continue;

                // 出队后立即递减 ready queue 计数；PumpOutboundAsync 可能耗时，
                // 不应在计数中保留正在 pump 的 session。
                Interlocked.Decrement(ref _readyQueueCount);

                try
                {
                    await session
                        .PumpOutboundAsync(_burstLimit, token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    // PumpOutboundAsync 内部已捕获预期异常并 Close 连接；
                    // 此处仅兜底未预期异常，避免 worker 因单连接错误退出。
                    _logger.TransportFailed(
                        GatewayTransportOperation.SendLoop,
                        session.ConnectionId,
                        exception);
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (ChannelClosedException)
        {
            // StopAsync completed the channel.
        }
    }

    public void Dispose()
    {
        _cts.Dispose();
    }
}
