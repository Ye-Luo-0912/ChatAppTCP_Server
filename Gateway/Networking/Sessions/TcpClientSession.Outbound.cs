using System.Net.Sockets;
using System.Threading.Channels;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 出站队列管理：Durable FIFO + Ephemeral keyed mailbox + 两种发送驱动模型。
/// <para>
/// 两种驱动模型：
/// <list type="bullet">
/// <item><see cref="SendLoopAsync"/> — PersistentSendLoop：每连接永久 Task 消费 _outbound Channel。</item>
/// <item><see cref="PumpOutboundAsync"/> — OnDemandSendPump：共享 worker 池按需调度，burst 上限 + 公平轮转。</item>
/// </list>
/// </para>
/// <para>
/// Ephemeral 帧通过 <see cref="EphemeralMailbox"/> 按 key 覆盖旧帧保留最新状态，
/// flush sentinel 写入 FIFO 唤醒发送循环排空 mailbox。sentinel 不占用预算。
/// </para>
/// </summary>
internal sealed partial class TcpClientSession
{
    public bool TryQueue(
        SharedOutboundFrame frame,
        SessionCloseReason? closeAfterSend = null)
    {
        if (!IsConnected)
        {
            return false;
        }

        var byteCount = frame.Length;
        if (!_outboundBudget.TryReserve(byteCount))
        {
            _metrics.OutboundRejected("byte-budget");
            Close(SessionCloseReason.OutboundQueueFull);
            return false;
        }

        // 全局出站字节预算检查。
        if (_globalOutboundBudget is not null &&
            !_globalOutboundBudget.TryReserve(byteCount))
        {
            _outboundBudget.Release(byteCount);
            _metrics.OutboundRejectedGlobalBudget();
            _metrics.OutboundRejected("global-byte-budget");
            Close(SessionCloseReason.OutboundQueueFull);
            return false;
        }

        _metrics.OutboundEnqueued(byteCount);

        if (!frame.TryRetain())
        {
            ReleaseQueuedWrite(byteCount);
            return false;
        }

        if (_outbound.Writer.TryWrite(
                new OutboundWrite(
                    frame,
                    byteCount,
                    closeAfterSend)))
        {
            TryScheduleSend();
            return true;
        }

        frame.Dispose();
        ReleaseQueuedWrite(byteCount);
        _metrics.OutboundRejected("item-capacity-or-closed");
        Close(SessionCloseReason.OutboundQueueFull);
        return false;
    }

    /// <summary>
    /// Ephemeral 等级帧入队（Typing/Presence 瞬态状态）。
    /// <para>
    /// 与 Critical/Durable 路径（<see cref="TryQueue"/>）的关键差异：
    /// <list type="bullet">
    /// <item>写入独立的 keyed latest-state mailbox，相同 <paramref name="key"/> 覆盖旧帧（dispose + 释放预算），</item>
    /// <item>不同 key 独立共存，确保各会话/用户的最新状态都能被送达。</item>
    /// <item>队列满或预算超限时仅丢弃帧，不关闭连接，避免慢消费者因瞬态帧被踢下线。</item>
    /// <item>通过单槽 flush sentinel 唤醒发送循环排空 mailbox，sentinel 不占用预算。</item>
    /// </list>
    /// 这修复了旧实现中 Ephemeral 与 Durable 共享 FIFO 导致的问题：
    /// 队列满时丢弃新帧、旧状态先发送、客户端可能收到陈旧的 Typing=true 而非最新 Typing=false。
    /// </para>
    /// </summary>
    public bool TryQueueEphemeral(SharedOutboundFrame frame, EphemeralKey key)
    {
        if (!IsConnected)
            return false;

        var byteCount = frame.Length;

        // 字节预算超限：丢弃帧，不断开连接。
        if (!_outboundBudget.TryReserve(byteCount))
        {
            _metrics.OutboundRejected("ephemeral-byte-budget");
            return false;
        }

        if (_globalOutboundBudget is not null &&
            !_globalOutboundBudget.TryReserve(byteCount))
        {
            _outboundBudget.Release(byteCount);
            _metrics.OutboundRejectedGlobalBudget();
            _metrics.OutboundRejected("ephemeral-global-byte-budget");
            return false;
        }

        if (!frame.TryRetain())
        {
            ReleaseQueuedWrite(byteCount);
            return false;
        }

        _metrics.OutboundEnqueued(byteCount);

        // 写入 mailbox：同 key 原子覆盖旧帧（dispose + 释放预算），不同 key 独立共存。
        // CAS 循环在 EphemeralMailbox.TryStore 内完成，避免与 drain 的 TryRemove 竞争。
        var newEntry = new EphemeralEntry(frame, byteCount);
        if (_ephemeralMailbox.TryStore(key, newEntry) is { } oldEntry)
        {
            // CAS 成功：drain 不会拿到 oldEntry，可安全 dispose 与释放预算。
            oldEntry.Frame.Dispose();
            ReleaseQueuedWrite(oldEntry.ByteCount);
        }

        // 唤醒发送循环：若 flush sentinel 未在队列中，写入一个。
        // sentinel 的 Frame=null 标识发送循环应排空 mailbox。
        // 单槽设计：多个 ephemeral 帧共享一个 sentinel，避免 sentinel 泛滥。
        if (!_ephemeralFlushPending)
        {
            _ephemeralFlushPending = true;
            if (!_outbound.Writer.TryWrite(new OutboundWrite(null, 0, null)))
            {
                // TryWrite 失败：队列已关闭（连接正在关闭）或队列满。
                // 重置 flag 允许后续 TryQueueEphemeral 重试 sentinel。
                // 不清理 mailbox 条目：连接关闭时 DrainOutboundOnClose 会统一清理；
                // 队列满时 PumpOutboundAsync 会在每个 durable write 后机会式排空 mailbox。
                _ephemeralFlushPending = false;
            }
        }

        // OnDemandSendPump：唤醒共享 worker 池处理 sentinel + mailbox。
        // PersistentSendLoop：_outboundPump=null，TryScheduleSend 是 no-op。
        TryScheduleSend();

        return true;
    }

    private async Task SendLoopAsync()
    {
        try
        {
            await foreach (var write in _outbound.Reader.ReadAllAsync(
                               _lifetime.Token).ConfigureAwait(false))
            {
                if (write.Frame is null)
                {
                    // Ephemeral flush sentinel：先重置 flag 再排空 mailbox。
                    // 先重置允许 drain 期间到达的新 TryQueueEphemeral 写入新 sentinel，
                    // 避免丢失唤醒（drain 中到达的新帧会被本次 drain 或下次 sentinel 处理）。
                    _ephemeralFlushPending = false;
                    await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                    continue;
                }

                ReleaseQueuedWrite(write.ByteCount);

                try
                {
                    await SendFrameAsync(
                            write.Frame.Memory,
                            _lifetime.Token)
                        .ConfigureAwait(false);
                    _metrics.FrameSent();

                    if (write.CloseAfterSend is { } closeReason)
                    {
                        Close(closeReason);
                        return;
                    }
                }
                finally
                {
                    write.Frame.Dispose();
                }

                // 机会式排空：处理 sentinel TryWrite 失败（队列满）导致的丢失唤醒。
                // 队列满时 sentinel 无法入队，但 mailbox 中有未发送帧；
                // 每个 durable write 后检查并排空，确保 ephemeral 不会因队列满而无限滞留。
                if (!_ephemeralMailbox.IsEmpty)
                {
                    _ephemeralFlushPending = false;
                    await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal close path.
        }
        catch (SocketException)
        {
            Close(SessionCloseReason.TransportError);
        }
        catch (ObjectDisposedException)
        {
            Close(SessionCloseReason.TransportError);
        }
        catch (Exception exception)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.SendLoop,
                ConnectionId,
                exception);
            Close(SessionCloseReason.TransportError);
        }
        finally
        {
            DrainOutboundOnClose();
        }
    }

    /// <summary>
    /// OnDemandSendPump/PerSessionDrain 模式：入队后唤醒发送驱动。
    /// <para>
    /// OnDemandSendPump（三态）：CAS Idle→Queued 成功后 TrySchedule 入 ready queue。
    /// PerSessionDrain（二态）：CAS Idle→Running 成功后启动自有 drain Task。
    /// PersistentSendLoop 模式下两者均为 null，本方法是 no-op。
    /// </para>
    /// <para>
    /// 若 <see cref="OutboundPumpCoordinator.TrySchedule"/> 失败（coordinator 停机），
    /// 回退 Queued→Idle 以避免状态泄漏；此时连接也将被 Close，残留帧由 <see cref="DrainOutboundOnClose"/> 清理。
    /// </para>
    /// </summary>
    private void TryScheduleSend()
    {
        // PerSessionDrain 模式：CAS Idle→Running 直接启动自有 drain（跳过 Queued）。
        if (_usePerSessionDrain)
        {
            if (Interlocked.CompareExchange(ref _sendState, SendStateRunning, SendStateIdle) != SendStateIdle)
                return;

            // 启动 Session 自有按需 drain Task。Socket.SendAsync 的 continuation 天然恢复同一 drain。
            // _perSessionDrainTask 仅用于 DisposeAsync 等待 drain 退出，无需取消（drain 通过 _lifetime 退出）。
            _perSessionDrainTask = RunPerSessionDrainAsync();
            return;
        }

        if (_outboundPump is null)
            return;

        // Idle → Queued：仅当当前空闲时才入队，保证同 session 同时只在 ready queue 中存在一份引用。
        if (Interlocked.CompareExchange(ref _sendState, SendStateQueued, SendStateIdle) != SendStateIdle)
            return;

        if (!_outboundPump.TrySchedule(this))
        {
            // ready channel 已关闭（停机）：回退 Queued→Idle，避免后续 TryScheduleSend 永远 CAS 失败。
            Interlocked.Exchange(ref _sendState, SendStateIdle);
        }
    }

    /// <summary>
    /// PerSessionDrain 模式的自有按需 drain：持续消费出站 Channel 直到队列空。
    /// <para>
    /// 与 <see cref="PumpOutboundAsync"/> 的区别：
    /// <list type="bullet">
    /// <item>无 burst 上限——drain 持续消费直到队列空（每连接独立，无需公平轮转）；</item>
    /// <item>无 ready queue 调度——CAS Idle→Running 直接启动，Socket continuation 恢复同一 drain；</item>
    /// <item>慢 Socket 不占用全局 Worker 名额——每连接独立 drain。</item>
    /// </list>
    /// </para>
    /// <para>
    /// 状态机：drain 退出时 CAS Running→Idle，然后重检防丢失唤醒
    /// （enqueuer 可能在 Running→Idle 转换前入队，其 CAS Idle→Running 会失败，
    /// 依赖此处 Idle 后的 re-check 来补发 drain）。
    /// </para>
    /// </summary>
    private async Task RunPerSessionDrainAsync()
    {
        try
        {
            while (IsConnected)
            {
                if (!_outbound.Reader.TryRead(out var write))
                {
                    // FIFO 空：检查 ephemeral mailbox（机会式排空，处理 sentinel TryWrite 失败的丢失唤醒）。
                    if (!_ephemeralMailbox.IsEmpty)
                    {
                        _ephemeralFlushPending = false;
                        await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                        continue;
                    }

                    // 队列空：CAS Running→Idle 退出 drain。
                    if (Interlocked.CompareExchange(ref _sendState, SendStateIdle, SendStateRunning) != SendStateRunning)
                        return; // 状态已变（停机回退等），让出 drain。

                    // 清除后重检：enqueuer 可能在 Running→Idle 转换前入队，
                    // 其 CAS Idle→Running 会失败（因为状态是 Running），
                    // 它依赖此处 Idle 后的 re-check 来补发 drain。
                    if (IsConnected && HasPendingWork())
                    {
                        if (Interlocked.CompareExchange(ref _sendState, SendStateRunning, SendStateIdle) == SendStateIdle)
                            continue; // 重新获得 drain 所有权，继续消费。
                    }
                    return; // 真正空闲，drain 退出。
                }

                if (write.Frame is null)
                {
                    // Ephemeral flush sentinel：先重置 flag 再排空 mailbox。
                    _ephemeralFlushPending = false;
                    await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                    continue; // sentinel 不计入 burst（PerSessionDrain 无 burst 限制）。
                }

                ReleaseQueuedWrite(write.ByteCount);

                try
                {
                    await SendFrameAsync(
                            write.Frame.Memory,
                            _lifetime.Token)
                        .ConfigureAwait(false);
                    _metrics.FrameSent();

                    if (write.CloseAfterSend is { } closeReason)
                    {
                        Close(closeReason);
                        return;
                    }
                }
                finally
                {
                    write.Frame.Dispose();
                }

                // 机会式排空：处理 sentinel TryWrite 失败（队列满）导致的丢失唤醒。
                if (!_ephemeralMailbox.IsEmpty)
                {
                    _ephemeralFlushPending = false;
                    await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                }
            }

            // 连接关闭期间 drain 退出：排空残留帧释放预算（与 DisposeAsync 幂等）。
            if (!IsConnected)
            {
                DrainOutboundOnClose();
            }
        }
        catch (OperationCanceledException)
            when (_lifetime.Token.IsCancellationRequested)
        {
            // 关闭或停机：Close 由 DisposeAsync/HandleClientAsync 调用。
            DrainOutboundOnClose();
        }
        catch (SocketException)
        {
            Close(SessionCloseReason.TransportError);
        }
        catch (ObjectDisposedException)
        {
            Close(SessionCloseReason.TransportError);
        }
        catch (Exception exception)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.SendLoop,
                ConnectionId,
                exception);
            Close(SessionCloseReason.TransportError);
        }
        finally
        {
            // 确保 drain 退出时状态归位（异常路径可能未 CAS Running→Idle）。
            Interlocked.CompareExchange(ref _sendState, SendStateIdle, SendStateRunning);
        }
    }

    /// <summary>
    /// OnDemandSendPump 模式：由 <see cref="OutboundPumpCoordinator"/> worker 调用，
    /// 处理最多 <paramref name="maxBurst"/> 个出站帧（durable FIFO + ephemeral mailbox）。
    /// <para>
    /// 三态状态机保证任意时刻一个 session 最多只有一个 worker 持有发送所有权：
    /// <list type="bullet">
    /// <item>worker 出队后 CAS Queued→Running 取得所有权；</item>
    /// <item>pump 结束若仍有 pending work：CAS Running→Queued 并重新入队（不经过 Idle）；</item>
    /// <item>pump 结束若无 pending work：CAS Running→Idle，然后重检防丢失唤醒。</item>
    /// </list>
    /// </para>
    /// </summary>
    public async ValueTask PumpOutboundAsync(int maxBurst, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxBurst, 0);

        // Queued → Running：取得发送所有权。若状态已变（停机回退等），让出 worker。
        if (Interlocked.CompareExchange(ref _sendState, SendStateRunning, SendStateQueued) != SendStateQueued)
            return;

        try
        {
            var processed = 0;
            while (processed < maxBurst && IsConnected)
            {
                if (!_outbound.Reader.TryRead(out var write))
                    break; // FIFO 空：让出 worker

                if (write.Frame is null)
                {
                    // Ephemeral flush sentinel：先重置 flag 再排空 mailbox。
                    _ephemeralFlushPending = false;
                    await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                    continue; // sentinel 不计入 burst
                }

                ReleaseQueuedWrite(write.ByteCount);

                try
                {
                    await SendFrameAsync(
                            write.Frame.Memory,
                            _lifetime.Token)
                        .ConfigureAwait(false);
                    _metrics.FrameSent();

                    if (write.CloseAfterSend is { } closeReason)
                    {
                        Close(closeReason);
                        return;
                    }
                }
                finally
                {
                    write.Frame.Dispose();
                }

                processed++;

                // 机会式排空：处理 sentinel TryWrite 失败（队列满）导致的丢失唤醒。
                if (!_ephemeralMailbox.IsEmpty)
                {
                    _ephemeralFlushPending = false;
                    await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                }
            }

            // Burst 上限达到且仍有待处理：Running→Queued 重新入队（公平轮转）。
            // 不经过 Idle：避免 enqueuer 在 Running→Idle→Queued 窗口内 CAS Idle→Queued 成功
            // 并重复 TrySchedule。Running→Queued 使 enqueuer 的 Idle→Queued CAS 失败（状态非 Idle）。
            if (processed >= maxBurst && IsConnected && HasPendingWork())
            {
                if (Interlocked.CompareExchange(ref _sendState, SendStateQueued, SendStateRunning) == SendStateRunning)
                {
                    if (!_outboundPump!.TrySchedule(this))
                    {
                        // coordinator 停机：回退 Queued→Idle，让后续路径（DisposeAsync）清理。
                        Interlocked.Exchange(ref _sendState, SendStateIdle);
                    }
                }
                return;
            }
        }
        catch (OperationCanceledException)
            when (_lifetime.Token.IsCancellationRequested ||
                  cancellationToken.IsCancellationRequested)
        {
            // 关闭或停机：Close 由 DisposeAsync/HandleClientAsync 调用。
        }
        catch (SocketException)
        {
            Close(SessionCloseReason.TransportError);
        }
        catch (ObjectDisposedException)
        {
            Close(SessionCloseReason.TransportError);
        }
        catch (Exception exception)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.SendLoop,
                ConnectionId,
                exception);
            Close(SessionCloseReason.TransportError);
        }
        finally
        {
            // pump 结束且无 pending work（或连接已关闭/异常）：Running→Idle。
            // 仅当状态仍为 Running 时清除。若 pump 已转 Queued（burst 重入队路径），则不处理。
            if (Interlocked.CompareExchange(ref _sendState, SendStateIdle, SendStateRunning) == SendStateRunning)
            {
                // 清除后重检：enqueuer 可能在 Running→Idle 转换前入队，
                // 其 CAS Idle→Queued 会失败（因为状态是 Running），
                // 它依赖此处 Idle 后的 re-check 来补发 ready signal。
                if (IsConnected && HasPendingWork())
                {
                    if (Interlocked.CompareExchange(ref _sendState, SendStateQueued, SendStateIdle) == SendStateIdle)
                    {
                        if (!_outboundPump!.TrySchedule(this))
                        {
                            Interlocked.Exchange(ref _sendState, SendStateIdle);
                        }
                    }
                    // else: enqueuer 已通过自己的 CAS Idle→Queued 完成 schedule。
                }
            }

            // 连接关闭期间 pump 退出：排空残留帧释放预算（与 DisposeAsync 幂等）。
            if (!IsConnected)
            {
                DrainOutboundOnClose();
            }
        }
    }

    /// <summary>
    /// 是否有待处理的出站帧（FIFO 或 ephemeral mailbox 非空）。
    /// 用于 pump 结束时的 re-check，判断是否需要重新调度。
    /// </summary>
    private bool HasPendingWork() =>
        _outbound.Reader.TryPeek(out _) || !_ephemeralMailbox.IsEmpty;

    /// <summary>
    /// 排空出站 FIFO 与 ephemeral mailbox 中残留的帧，释放预算与帧引用。
    /// 由 <see cref="SendLoopAsync"/> finally（PersistentSendLoop 模式）与
    /// <see cref="DisposeAsync"/>（OnDemandSendPump 模式）调用。
    /// 幂等：多次调用安全（TryRead/TryRemove 返回 false 后即退出）。
    /// </summary>
    private void DrainOutboundOnClose()
    {
        // 排空 FIFO 中残留的 writes（含 sentinel，Frame 可能为 null）。
        while (_outbound.Reader.TryRead(out var pending))
        {
            ReleaseQueuedWrite(pending.ByteCount);
            pending.Frame?.Dispose();
        }
        // 排空 mailbox 中残留的 ephemeral 帧，释放预算与帧引用。
        DrainEphemeralMailboxOnClose();
    }

    /// <summary>
    /// 排空 ephemeral mailbox：原子移除每个 key 的最新条目并发送。
    /// 调用前必须已重置 <see cref="_ephemeralFlushPending"/>，避免丢失唤醒。
    /// </summary>
    private async ValueTask DrainEphemeralMailboxAsync()
    {
        var toSend = _ephemeralMailbox.Drain();
        if (toSend is null)
            return;

        var index = 0;
        try
        {
            while (index < toSend.Count)
            {
                var entry = toSend[index];
                await SendFrameAsync(
                        entry.Frame.Memory,
                        _lifetime.Token)
                    .ConfigureAwait(false);
                _metrics.FrameSent();
                // 先递增 index 再 dispose：若 Dispose 抛异常，finally 不会重复 dispose 此条目。
                index++;
                entry.Frame.Dispose();
                ReleaseQueuedWrite(entry.ByteCount);
            }
        }
        finally
        {
            // 清理未发送的条目（发送失败或取消时）。index 指向第一个未成功完成的条目。
            while (index < toSend.Count)
            {
                toSend[index].Frame.Dispose();
                ReleaseQueuedWrite(toSend[index].ByteCount);
                index++;
            }
        }
    }

    /// <summary>
    /// 连接关闭时清理 mailbox：仅 dispose 帧并释放预算，不尝试发送。
    /// </summary>
    private void DrainEphemeralMailboxOnClose()
    {
        var toDispose = _ephemeralMailbox.Drain();
        if (toDispose is null)
            return;

        foreach (var entry in toDispose)
        {
            entry.Frame.Dispose();
            ReleaseQueuedWrite(entry.ByteCount);
        }
    }

    private void ReleaseQueuedWrite(int byteCount)
    {
        _outboundBudget.Release(byteCount);
        _globalOutboundBudget?.Release(byteCount);
        _metrics.OutboundDequeued(byteCount);
    }
}
