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
        _outbound.Writer.TryComplete();
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
            // PerSessionDrain 模式：等待活跃 drain Task 退出（其 finally 会排空 FIFO + mailbox）。
            if (_sendLoop is not null)
            {
                await _sendLoop.ConfigureAwait(false);
            }
            else if (_perSessionDrainTask is not null)
            {
                await _perSessionDrainTask.ConfigureAwait(false);
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
        // 发送超时改由 SendTimeoutTracker 扫描管理（替代每帧 DeadlineWheel.Register）。
        // 顺序：先记 startedAt，再标记发送中，最后通知 tracker 开始监控。
        // 先记 startedAt 确保扫描看到 _sendInProgress=1 时 startedAt 已更新为本代次的值。
        // 单调时钟避免墙钟回拨导致的死锁。
        Volatile.Write(ref _sendStartedAt, _timeProvider.GetTimestamp());
        Interlocked.Exchange(ref _sendInProgress, 1);
        // TryAdd 幂等：burst 内连续帧为廉价 no-op（仅 hash 查找，无分配、无全局锁）。
        _sendTimeoutTracker?.OnSendStart(this);

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
            // 标记发送完成并清除 startedAt；从 tracker 活跃集合移除。
            Volatile.Write(ref _sendStartedAt, 0);
            Interlocked.Exchange(ref _sendInProgress, 0);
            _sendTimeoutTracker?.OnSendComplete(this);
        }
    }
}
