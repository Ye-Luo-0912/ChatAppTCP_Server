using System.Net.Sockets;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 连接生命周期与传输超时管理。
/// <para>
/// 包含：主动关闭（<see cref="Close"/>）、异步释放（<see cref="DisposeAsync"/>）、
/// 空闲 deadline 注册（<see cref="RegisterIdleDeadline"/>）、单帧发送与发送超时 deadline（<see cref="SendFrameAsync"/>）。
/// </para>
/// <para>
/// 发送超时使用单调时钟（<see cref="TimeProvider.GetElapsedTime(long)"/>）+ generation
/// （<c>_sendInProgress</c> CompareExchange）防止墙钟回拨与跨发送代次误关。
/// </para>
/// </summary>
internal sealed partial class TcpClientSession
{
    public void Close(SessionCloseReason reason)
    {
        if (Interlocked.CompareExchange(ref _closeState, 1, 0) != 0)
        {
            return;
        }

        Volatile.Write(ref _closeReason, (int)reason);
        _outbound.TryComplete();
        _lifetime.Cancel();

        // 取消未触发的 Auth/Idle deadline，避免 close 后回调误执行。
        // 已触发的回调通过 IsConnected 检查防御。
        // 发送超时改由 SendTimeoutTracker 扫描管理：从活跃集合移除即可，无需取消 deadline。
        if (_deadlineWheel is not null)
        {
            if (_authDeadlineRegistration.Id != 0)
            {
                _deadlineWheel.Cancel(_authDeadlineRegistration);
                _authDeadlineRegistration = default;
            }
            if (_idleDeadlineRegistration.Id != 0)
            {
                _deadlineWheel.Cancel(_idleDeadlineRegistration);
                _idleDeadlineRegistration = default;
            }
        }
        _sendTimeoutTracker?.OnSessionClosed(this);
        _frameAssemblyTracker?.OnSessionClosed(this);

        try
        {
            _socket.Shutdown(SocketShutdown.Both);
        }
        catch (SocketException)
        {
            // The peer may already have closed the connection.
        }
        catch (ObjectDisposedException)
        {
            // Another close path already released the socket.
        }

        _socket.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        Close(SessionCloseReason.ApplicationStopping);

        try
        {
            // PersistentSendLoop 模式：等待 SendLoop 退出（其 finally 会排空 FIFO + mailbox）。
            // OnDemandSendPump 模式：无永久 Task，直接排空残留（in-flight pump 会因
            // _lifetime 取消而快速退出，其 PumpOutboundAsync 的 finally 也会排空）。
            // PerSessionDrain 模式：等待活跃 drain 退出（其 finally 会排空 FIFO + mailbox）。
            if (_sendLoop is not null)
            {
                await _sendLoop.ConfigureAwait(false);
            }
            else if (_usePerSessionDrain)
            {
                // 八.1：等待 PerSessionDrain 活跃 drain 退出。
                // _drainStateGen packed state+gen 单次 CAS 原子发布，消除旧实现窗口。
                // Close 已设置 IsConnected=false，新 TryQueue 会失败，故最多一个活跃 drain 需等待。
                // Close 后无新 drain 启动 → MRVTSC Version 稳定 → ValueTask 不会因 Reset 失效。
                // drain 的 finally 在 Complete(gen) 前已排空 FIFO + 归位 _drainStateGen + 释放 Tracker，
                // 故 await 返回后可直接进入 _lifetime.Dispose()。
                while (true)
                {
                    var stateGen = Interlocked.Read(ref _drainStateGen);
                    if ((stateGen & DrainStateRunningBit) == 0)
                        break; // Idle：无活跃 drain。

                    var currentGen = (int)(stateGen & 0xFFFFFFFF);
                    var op = Volatile.Read(ref _drainOp);

                    // P0-2：op 尚未发布或属上一代时，SpinWait 等待当前代次 op 发布。
                    // 修复窗口 A（op==null 提前退出）和窗口 B（上一代 op busy-loop）。
                    if (op is null || op.ActiveGeneration != currentGen)
                    {
                        var spin = new SpinWait();
                        for (var i = 0; i < 1000; i++)
                        {
                            op = Volatile.Read(ref _drainOp);
                            if (op is not null && op.ActiveGeneration == currentGen)
                                break;
                            spin.SpinOnce();
                        }
                        if (op is null || op.ActiveGeneration != currentGen)
                            continue; // 重新检查状态（可能已 Idle 或换代）。
                    }

                    try
                    {
                        await op.WaitAsync(currentGen).ConfigureAwait(false);
                    }
                    catch
                    {
                        // drain 内部已处理异常并记录日志；此处仅等待其退出。
                    }
                }
                // 防御性排空：覆盖 Close→Read 窗口内入队但 drain 未消费的残留帧。
                // 幂等，与 drain finally 的 DrainOutboundOnClose 安全并发。
                DrainOutboundOnClose();
            }
            else
            {
                DrainOutboundOnClose();
            }
        }
        finally
        {
            // 取消未触发的 Auth/Idle deadline（防止关闭后回调误进入 Close）。
            // 发送超时已由 SendTimeoutTracker 管理，Close 中已从活跃集合移除。
            if (_deadlineWheel is not null)
            {
                if (_authDeadlineRegistration.Id != 0)
                    _deadlineWheel.Cancel(_authDeadlineRegistration);
                if (_idleDeadlineRegistration.Id != 0)
                    _deadlineWheel.Cancel(_idleDeadlineRegistration);
            }
            _lifetime.Dispose();
        }
    }

    /// <summary>
    /// 注册空闲超时 deadline。到期时检查活跃度：
    /// 若已超时则关闭连接；若仍活跃则按剩余时间 re-register。
    /// check-on-fire 模式避免每包 re-register，deadline 至多每 idleTimeout 周期触发一次。
    /// </summary>
    private DeadlineRegistration RegisterIdleDeadline()
    {
        return _deadlineWheel!.Register(
            _idleTimeout,
            () =>
            {
                if (!IsConnected)
                    return;

                var age = LastInboundAge;
                if (age >= _idleTimeout)
                {
                    Close(SessionCloseReason.IdleTimedOut);
                    return;
                }

                // 仍活跃：按剩余时间 re-register。
                // age < idleTimeout 保证 remaining > 0。
                var remaining = _idleTimeout - age;
                if (remaining > TimeSpan.Zero)
                {
                    _idleDeadlineRegistration = RegisterIdleDeadline();
                }
            });
    }

    /// <summary>
    /// 发送超时检查：由 <see cref="Executor.SendTimeoutTracker"/> 扫描线程周期调用。
    /// <para>
    /// generation-aware：通过 <c>_sendInProgress</c> CAS 防止跨发送代次误关。
    /// 单调时钟校验防止墙钟回拨误判。非发送中立即返回（廉价 volatile 读）。
    /// </para>
    /// <para>
    /// 此方法从原 <see cref="SendFrameAsync"/> 闭包中提取，消除每帧 Action 委托分配。
    /// </para>
    /// </summary>
    internal void CheckSendTimeout()
    {
        if (Volatile.Read(ref _sendInProgress) != 1)
            return;

        var startedAt = Volatile.Read(ref _sendStartedAt);
        if (startedAt == 0)
            return;

        if (_timeProvider.GetElapsedTime(startedAt) < _sendTimeout)
            return;

        // CAS 抢占关闭所有权：仅一个调用方（扫描线程或旧闭包残留）能进入 Close。
        if (Interlocked.CompareExchange(ref _sendInProgress, 0, 1) == 1)
            Close(SessionCloseReason.SendTimedOut);
    }

    private async ValueTask SendFrameAsync(
        ReadOnlyMemory<byte> frame,
        CancellationToken lifetimeToken)
    {
        // 发送超时注册粒度为发送所有权周期（drain/pump/loop 入口/出口），
        // 帧内仅更新 _sendStartedAt / _sendInProgress，无 ConcurrentDictionary 操作。
        // 顺序：先记 startedAt，再标记发送中。先记 startedAt 确保扫描看到 _sendInProgress=1
        // 时 startedAt 已更新为本代次的值。单调时钟避免墙钟回拨导致的死锁。
        Volatile.Write(ref _sendStartedAt, _timeProvider.GetTimestamp());
        Interlocked.Exchange(ref _sendInProgress, 1);

        var sent = 0;
        try
        {
            while (sent < frame.Length)
            {
                var bytesSent = await _socket.SendAsync(
                        frame[sent..],
                        SocketFlags.None,
                        lifetimeToken)
                    .ConfigureAwait(false);

                if (bytesSent <= 0)
                {
                    throw new SocketException(
                        (int)SocketError.ConnectionReset);
                }

                sent += bytesSent;
            }
        }
        finally
        {
            // 标记发送完成并清除 startedAt。所有权注销由 drain/pump/loop finally 负责。
            Volatile.Write(ref _sendStartedAt, 0);
            Interlocked.Exchange(ref _sendInProgress, 0);
        }
    }
}
