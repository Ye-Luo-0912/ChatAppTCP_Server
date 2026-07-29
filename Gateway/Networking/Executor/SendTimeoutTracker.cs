using System.Collections.Concurrent;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Gateway.Networking.Executor;

/// <summary>
/// 发送超时扫描器：替代 <see cref="DeadlineWheel"/> 处理每帧发送超时。
/// <para>
/// 与 DeadlineWheel 的区别：
/// <list type="bullet">
/// <item><b>无每帧闭包分配</b>：超时检查逻辑在 <see cref="TcpClientSession.CheckSendTimeout"/> 中，
/// 不为每帧创建捕获 Session 的 <c>Action</c> 委托；</item>
/// <item><b>无全局锁</b>：活跃发送方集合用 <see cref="ConcurrentDictionary{TKey,TValue}"/>，
/// <see cref="OnSendStart"/>/<see cref="OnSendComplete"/> 为无锁 CAS，不竞争 DeadlineWheel 的全局 <c>Lock</c>；</item>
/// <item><b>聚焦扫描</b>：只扫描当前正在发送的少量 Session（活跃发送方集合），
/// 而非全部连接。空闲时集合为空，扫描近乎零开销；</item>
/// <item><b>无 <c>_fired</c> 增长</b>：不维护按 id 追踪的已触发集合，
/// 避免发送超时 id 在 <see cref="DeadlineWheel"/> 中无限增长。</item>
/// </list>
/// </para>
/// <para>
/// 生命周期：
/// <list type="bullet">
/// <item><see cref="OnSendStart"/>：SendFrameAsync 开始时 TryAdd（幂等，burst 内连续帧为廉价 no-op）；</item>
/// <item><see cref="OnSendComplete"/>：SendFrameAsync 完成时 TryRemove；</item>
/// <item><see cref="OnSessionClosed"/>：连接 Close 时 TryRemove，防止残留引用。</item>
/// </list>
/// 扫描线程周期性遍历活跃集合，对每个 Session 调用
/// <see cref="TcpClientSession.CheckSendTimeout"/>。后者复用既有的 generation CAS +
/// 单调时钟校验，超时则 CAS 关闭连接。
/// </para>
/// <para>
/// Auth/Idle 超时仍由 <see cref="DeadlineWheel"/> 管理（低频，符合其设计假设）。
/// </para>
/// </summary>
internal sealed class SendTimeoutTracker : IAsyncDisposable
{
    // 活跃发送方集合：仅包含当前正在执行 Socket.SendAsync 的 Session。
    // 大多数时刻为空或很小（只有慢客户端会长期停留在此集合中）。
    private readonly ConcurrentDictionary<TcpClientSession, byte> _activeSenders = new();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _scanInterval;
    private readonly CancellationTokenSource _cts = new();
    private Task? _loopTask;
    private volatile bool _stopping;

    public static readonly TimeSpan DefaultScanInterval = TimeSpan.FromMilliseconds(100);

    public SendTimeoutTracker(
        TimeProvider? timeProvider = null,
        TimeSpan? scanInterval = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _scanInterval = scanInterval ?? DefaultScanInterval;
    }

    /// <summary>当前正在发送的 Session 数（近似值，用于观测）。</summary>
    public int ActiveSenderCount => _activeSenders.Count;

    /// <summary>
    /// 标记 Session 开始发送。幂等：burst 内连续帧调用为廉价 TryAdd no-op。
    /// </summary>
    public void OnSendStart(TcpClientSession session)
        => _activeSenders.TryAdd(session, 0);

    /// <summary>
    /// 标记 Session 发送完成。从活跃集合移除，后续扫描不再检查此 Session。
    /// </summary>
    public void OnSendComplete(TcpClientSession session)
        => _activeSenders.TryRemove(session, out _);

    /// <summary>
    /// 连接关闭时清理：确保 Session 不残留在活跃集合中。
    /// </summary>
    public void OnSessionClosed(TcpClientSession session)
        => _activeSenders.TryRemove(session, out _);

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_loopTask is not null)
            return Task.CompletedTask;
        _loopTask = RunScanLoopAsync();
        return Task.CompletedTask;
    }

    private async Task RunScanLoopAsync()
    {
        using var timer = new PeriodicTimer(_scanInterval, _timeProvider);
        var token = _cts.Token;
        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                // 空闲时集合为空，跳过枚举近乎零开销。
                if (_activeSenders.IsEmpty)
                    continue;

                // 遍历活跃发送方：对每个 Session 执行 generation-aware 超时检查。
                // CheckSendTimeout 内部用 volatile 读 + CAS，非发送中立即返回。
                foreach (var session in _activeSenders.Keys)
                    session.CheckSendTimeout();
            }
        }
        catch (OperationCanceledException)
        {
            // 停机：正常退出。
        }
    }

    public async Task StopAsync()
    {
        if (_stopping)
            return;
        _stopping = true;

        await _cts.CancelAsync().ConfigureAwait(false);
        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.ConfigureAwait(false);
            }
            catch
            {
                // 扫描循环异常已被 catch，此处忽略。
            }
        }

        // 清空残留活跃发送方（连接 Close 路径会各自调用 OnSessionClosed，此处兜底）。
        _activeSenders.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts.Dispose();
    }
}
