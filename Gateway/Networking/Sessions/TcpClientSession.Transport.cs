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
            // OnDemandSendPump 模式：无永久 Task；先撤销 Queued 或等待 Running pump
            // 释放单消费者所有权，再由 Dispose 唯一排空。禁止与 pump 并发 Drain。
            // PerSessionDrain 模式：等待活跃 drain 完整退出，再由 Dispose 唯一排空。
            if (_sendLoop is not null)
            {
                await _sendLoop.ConfigureAwait(false);
            }
            else if (_usePerSessionDrain)
            {
                await WaitForPerSessionDrainOwnershipReleaseAsync().ConfigureAwait(false);
                // 防御性串行排空：覆盖 Close→Read 窗口内入队但 drain 未消费的残留帧。
                // Idle 是 Complete 后发布的稳定 handoff，不会与 drain/Reset 并发。
                DrainOutboundOnClose();
            }
            else if (_outboundPump is not null)
            {
                await WaitForOutboundPumpOwnershipReleaseAsync().ConfigureAwait(false);
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
    /// Close 后等待 PerSessionDrain 当前代次完整交还单消费者所有权。
    /// Publishing 阶段不能读取 MRVTSC（Reset 尚未完成）；Running/Finalizing 则等待
    /// matching generation Complete，并继续重读 phase，直到 Complete→Idle 的稳定 handoff。
    /// </summary>
    private async ValueTask WaitForPerSessionDrainOwnershipReleaseAsync()
    {
        var hasAwaitedGeneration = false;
        var awaitedGeneration = 0;

        while (true)
        {
            var stateGen = Interlocked.Read(ref _drainStateGen);
            var phase = stateGen & DrainStatePhaseMask;
            if (phase == 0)
                return;

            var generation = (int)(stateGen & uint.MaxValue);
            if (phase == DrainStatePublishing)
            {
                // Publisher 会在 Reset 完成后发布 Running，或在 Close 竞态下自行
                // Complete/撤销本代。shutdown-only 等待，不增加 per-session waiter。
                await Task.Yield();
                continue;
            }

            if (!hasAwaitedGeneration || awaitedGeneration != generation)
            {
                var op = Volatile.Read(ref _drainOp);
                if (op is null || op.ActiveGeneration != generation)
                {
                    // Running 仅应在 ActiveGeneration 最后发布后可见；保留防御性重读。
                    await Task.Yield();
                    continue;
                }

                try
                {
                    await op.WaitAsync(generation).ConfigureAwait(false);
                }
                catch
                {
                    // drain 内部已处理异常并记录日志；此处仅等待其退出。
                }

                hasAwaitedGeneration = true;
                awaitedGeneration = generation;
            }

            // Complete 先于 Idle；不能在 ValueTask 完成后立刻 Drain，必须等 finalizer
            // 发布稳定 Idle，避免与 phase handoff 或下一代 publication 交叉。
            await Task.Yield();
        }
    }

    /// <summary>
    /// Close 后等待 OnDemandSendPump 的单消费者所有权释放。
    /// <para>
    /// Queued 尚未被 worker 获取时 CAS 回 Idle，使 ready queue 中的旧引用在未来 Pump CAS 时
    /// 直接 no-op；Running/Finalizing 时异步让出线程，直到 pump 完成 consumer re-check、
    /// 释放 Tracker 并回到 Idle。
    /// Close 后 TryScheduleSend 的双重 IsConnected 检查保证不会再发布新的所有权，
    /// 因而一旦观察到 Idle 即稳定，可由 Dispose 唯一执行关闭排空。
    /// </para>
    /// </summary>
    private async ValueTask WaitForOutboundPumpOwnershipReleaseAsync()
    {
        while (true)
        {
            var state = Volatile.Read(ref _sendState);
            if (state == SendStateIdle)
                return;

            if (state == SendStateQueued)
            {
                if (Interlocked.CompareExchange(
                        ref _sendState,
                        SendStateIdle,
                        SendStateQueued) == SendStateQueued)
                {
                    return;
                }

                continue;
            }

            // Running/Finalizing pump 持有 FIFO/mailbox 单消费者所有权。Close 已取消 socket send，
            // 正常只需极少数 continuation；不分配 per-session waiter/timer 常驻对象。
            await Task.Yield();
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
