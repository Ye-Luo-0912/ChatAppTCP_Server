using System.Net.Sockets;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 出站队列管理：Durable FIFO + Ephemeral keyed mailbox + 三种发送驱动模型。
/// <para>
/// 三种驱动模型：
/// <list type="bullet">
/// <item><see cref="SendLoopAsync"/> — PersistentSendLoop：每连接永久 Task 消费 _outbound Channel。</item>
/// <item><see cref="PumpOutboundAsync"/> — OnDemandSendPump：共享 worker 池按需调度，burst 上限 + 公平轮转。</item>
/// <item><see cref="RunPerSessionDrainAsync"/> — PerSessionDrain：入队时按需启动连接自有 drain。</item>
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

        if (_outbound.TryWrite(
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
        // lock + 开放寻址在 EphemeralMailbox.TryStore 内完成，避免与 drain 的竞争。
        // 达 MaxEphemeralKeys 硬上限时新 key 被拒绝：dispose 新帧、释放预算，不计入队列。
        var mailbox = GetOrCreateEphemeralMailbox();
        var newEntry = new EphemeralEntry(frame, byteCount);
        if (mailbox.TryStore(
                key,
                newEntry,
                out var rejected,
                out var storedEntry) is { } oldEntry)
        {
            // CAS 成功：drain 不会拿到 oldEntry，可安全 dispose 与释放预算。
            oldEntry.Frame.Dispose();
            ReleaseQueuedWrite(oldEntry.ByteCount);
        }
        else if (rejected)
        {
            // distinct key 数量已达 MaxEphemeralKeys 上限：新 key 未存储。
            // dispose 新帧、释放预算与计数，记录拒绝指标，不唤醒发送循环（mailbox 无新增条目）。
            frame.Dispose();
            ReleaseQueuedWrite(byteCount);
            _metrics.OutboundRejected("ephemeral-key-limit");
            return false;
        }

        // Close 可发生在入口 IsConnected 检查之后、mailbox 存储之前。此时关闭路径
        // 可能已经完成 Drain；必须条件移除本次精确条目并归还引用/预算，否则它不会再被消费。
        if (!IsConnected)
        {
            ReleaseEphemeralEntryIfOwned(mailbox, key, storedEntry);
            return false;
        }

        // 唤醒发送循环：若 flush sentinel 未在队列中，写入一个。
        // sentinel 的 Frame=null 标识发送循环应排空 mailbox。
        // 单槽设计：多个 ephemeral 帧共享一个 sentinel，避免 sentinel 泛滥。
        if (!_ephemeralFlushPending)
        {
            _ephemeralFlushPending = true;
            if (!_outbound.TryWrite(new OutboundWrite(null, 0, null)))
            {
                // TryWrite 失败：队列已关闭（连接正在关闭）或队列满。
                // 重置 flag 允许后续 TryQueueEphemeral 重试 sentinel。
                // 队列满且连接仍活跃时保留条目，PumpOutboundAsync 会在每个 durable write 后
                // 机会式排空；连接已关闭时则在下方条件移除本次精确条目。
                _ephemeralFlushPending = false;

                // 关闭与 TryWrite 线性化后，Drain 可能已先于本次 Store 完成。
                // 只撤销本次仍在槽内的精确条目；若 Drain/覆盖已取得所有权则不触碰。
                if (!IsConnected)
                {
                    ReleaseEphemeralEntryIfOwned(mailbox, key, storedEntry);
                    return false;
                }
            }
        }

        // 覆盖已有 sentinel 时的关闭窗口：本次无需 TryWrite，但 Close 可能已在上一次
        // IsConnected 检查之后完成。条件移除不会触碰已由 drain/覆盖取得的条目。
        if (!IsConnected)
        {
            ReleaseEphemeralEntryIfOwned(mailbox, key, storedEntry);
            return false;
        }

        // OnDemandSendPump：唤醒共享 worker 池处理 sentinel + mailbox。
        // PersistentSendLoop：_outboundPump=null，TryScheduleSend 是 no-op。
        TryScheduleSend();

        return true;
    }

    private void ReleaseEphemeralEntryIfOwned(
        EphemeralMailbox mailbox,
        EphemeralKey key,
        EphemeralEntry expectedEntry)
    {
        if (!mailbox.TryRemove(key, expectedEntry, out var removedEntry))
            return;

        removedEntry.Frame.Dispose();
        ReleaseQueuedWrite(removedEntry.ByteCount);
    }

    /// <summary>
    /// 测试专用：模拟已在 Close 前通过连接检查、随后才到达 Finalizing 的 stale producer。
    /// 生产者实际路径使用相同的完整 phase+generation CAS。
    /// </summary>
    internal bool TryPromotePerSessionFinalizingToPendingForTest()
    {
        while (true)
        {
            var observed = Interlocked.Read(ref _drainStateGen);
            if ((observed & DrainStatePhaseMask) != DrainStateFinalizing)
                return false;

            var pending = DrainStateFinalizingPending | (uint)observed;
            if (Interlocked.CompareExchange(
                    ref _drainStateGen,
                    pending,
                    observed) == observed)
            {
                return true;
            }
        }
    }

    private async Task SendLoopAsync()
    {
        // P1-1：PersistentSendLoop 模式下仅在活跃发送期间注册 Tracker，
        // WaitToReadAsync 等待期间不注册，避免 10K 空闲连接每 100ms 被全量扫描。
        // 旧实现使用 ReadAllAsync 并在方法入口处常驻 Tracker，导致空闲连接也参与扫描。
        var trackerActive = false;
        var ownershipEpoch = 0;
        try
        {
            while (true)
            {
                // 等待有数据可读——此期间不在 Tracker 中（空闲连接不参与超时扫描）。
                // WaitToReadAsync 在 Channel 完成时返回 false，在取消时抛 OperationCanceledException。
                if (!await _outbound.WaitToReadAsync(
                        _lifetime.Token).ConfigureAwait(false))
                {
                    break; // Channel completed
                }

                // 有数据可读：获取发送所有权，注册 Tracker。
                // P0-1：捕获 Epoch，Release 时传回，防止旧所有权误删新所有权注册。
                if (!trackerActive)
                {
                    ownershipEpoch = _sendTimeoutTracker?.OnSendOwnershipAcquired(this) ?? 0;
                    trackerActive = true;
                }

                // 持续消费 burst：处理所有当前可读帧，直到 TryRead 返回 false（队列空）。
                while (_outbound.TryRead(out var write))
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
                    if (HasEphemeralEntries)
                    {
                        _ephemeralFlushPending = false;
                        await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                    }
                }

                // Burst 完成（队列空）：释放发送所有权，退出 Tracker。
                // 进入下一次 WaitToReadAsync 等待，空闲期间不参与超时扫描。
                if (trackerActive)
                {
                    if (ownershipEpoch != 0)
                        _sendTimeoutTracker?.OnSendOwnershipReleased(this, ownershipEpoch);
                    trackerActive = false;
                    ownershipEpoch = 0;
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
            // 确保 Tracker 已释放：异常路径可能在 burst 中间退出，此时 trackerActive 仍为 true。
            // Close 路径的 OnSessionClosed 为兜底。
            if (trackerActive && ownershipEpoch != 0)
                _sendTimeoutTracker?.OnSendOwnershipReleased(this, ownershipEpoch);
        }
    }

    /// <summary>
    /// OnDemandSendPump/PerSessionDrain 模式：入队后唤醒发送驱动。
    /// <para>
    /// OnDemandSendPump（含 Finalizing/Pending）：CAS Idle→Queued 成功后 TrySchedule 入 ready queue，
    /// pump 退出前保持 consumer ownership 完成 re-check。
    /// PerSessionDrain：CAS Idle→Publishing 声明代次，MRVTSC Reset 完成后再发布 Running。
    /// PersistentSendLoop 模式下两者均为 null，本方法是 no-op。
    /// </para>
    /// <para>
    /// PerSessionDrain 使用 packed phase+generation：Publishing 阶段隔离 CAS→Reset 窗口，
    /// Running 只在 MRVTSC Version 与 ActiveGeneration 全部发布后可见；Finalizing 在
    /// Complete 前阻止下一代 Reset，FinalizingPending 则记录 finalizer 检查期间到达的新工作。
    /// </para>
    /// <para>
    /// 若 <see cref="OutboundPumpCoordinator.TrySchedule"/> 失败（coordinator 停机），
    /// 回退 Queued→Idle 以避免状态泄漏；此时连接也将被 Close，残留帧由 <see cref="DrainOutboundOnClose"/> 清理。
    /// </para>
    /// </summary>
    private void TryScheduleSend()
    {
        // 入队可在 Close 线性化前成功、随后才来到调度点。Close 后禁止发布新的
        // pump/drain 所有权，否则 Dispose 可能已观察 Idle 并释放 lifetime。
        if (!IsConnected)
            return;

        // PerSessionDrain：零分配 packed phase+generation 状态机。
        if (_usePerSessionDrain)
        {
            while (true)
            {
                // Interlocked.Read 保证 32 位平台也不会撕裂 phase+generation。
                var observed = Interlocked.Read(ref _drainStateGen);
                var phase = observed & DrainStatePhaseMask;

                if (phase == DrainStateFinalizing)
                {
                    // finalizer 已做或即将做最后一次单消费者 TryPeek。生产者不在此处
                    // 访问 consumer cursor，只把状态提升为 Pending，避免检查后的丢失唤醒。
                    var pending = DrainStateFinalizingPending | (uint)observed;
                    if (Interlocked.CompareExchange(
                            ref _drainStateGen,
                            pending,
                            observed) == observed)
                    {
                        return;
                    }

                    continue;
                }

                if (phase != 0)
                    return; // Publishing/Running/Pending 已有 owner 负责本次工作。

                var newGeneration = (int)(observed & uint.MaxValue) + 1;
                var publishing = DrainStatePublishing | (uint)newGeneration;
                if (Interlocked.CompareExchange(
                        ref _drainStateGen,
                        publishing,
                        observed) != observed)
                {
                    continue;
                }

                // Close 赢在 publication 之前：没有 Reset、没有 drain，可精确撤销本代。
                if (!IsConnected)
                {
                    Interlocked.CompareExchange(
                        ref _drainStateGen,
                        (uint)newGeneration,
                        publishing);
                    return;
                }

                var op = _drainOp ?? LazyInitializer.EnsureInitialized(
                    ref _drainOp,
                    static () => new DrainOperation())!;

                // state 保持 Publishing，Dispose 不得读取 completion core。Reset 内部最后
                // 发布 ActiveGeneration，因此返回时 Version/Pending 已完整属于本代。
                op.Reset(newGeneration);

                // Reset 期间发生 Close：publisher 自己完成本代，不启动无意义 drain。
                if (!IsConnected)
                {
                    var finalizing = DrainStateFinalizing | (uint)newGeneration;
                    if (Interlocked.CompareExchange(
                            ref _drainStateGen,
                            finalizing,
                            publishing) == publishing)
                    {
                        op.Complete(newGeneration);
                        // 已在 Close 前通过 TryScheduleSend 入口检查的 producer，可能在
                        // Complete 窗口把 Finalizing 提升为 Pending。关闭路径不重调度，
                        // 但必须接受两种 phase 并循环发布稳定 Idle，否则 Dispose 永久等待。
                        while (true)
                        {
                            var observedAfterComplete = Interlocked.Read(
                                ref _drainStateGen);
                            var phaseAfterComplete =
                                observedAfterComplete & DrainStatePhaseMask;
                            if ((uint)observedAfterComplete != (uint)newGeneration ||
                                (phaseAfterComplete != DrainStateFinalizing &&
                                 phaseAfterComplete != DrainStateFinalizingPending))
                            {
                                break;
                            }

                            if (Interlocked.CompareExchange(
                                    ref _drainStateGen,
                                    (uint)newGeneration,
                                    observedAfterComplete) == observedAfterComplete)
                            {
                                break;
                            }
                        }
                    }

                    return;
                }

                var running = DrainStateRunning | (uint)newGeneration;
                if (Interlocked.CompareExchange(
                        ref _drainStateGen,
                        running,
                        publishing) != publishing)
                {
                    // Publishing 仅由本 publisher 改写；防御性完成，避免 Dispose 永久等待。
                    op.Complete(newGeneration);
                    Interlocked.CompareExchange(
                        ref _drainStateGen,
                        (uint)newGeneration,
                        publishing);
                    return;
                }

                _ = RunPerSessionDrainAsync(op, newGeneration);
                return;
            }
        }

        if (_outboundPump is null)
            return;

        while (true)
        {
            var observed = Volatile.Read(ref _sendState);
            if (observed == SendStateFinalizing)
            {
                // finalizer 的最后一次 TryPeek 仍在单消费者所有权内；生产者仅置 Pending，
                // 不读取/推进 consumer cursor。
                if (Interlocked.CompareExchange(
                        ref _sendState,
                        SendStateFinalizingPending,
                        SendStateFinalizing) == SendStateFinalizing)
                {
                    return;
                }

                continue;
            }

            if (observed != SendStateIdle)
                return; // Queued/Running/Pending 已有 owner 负责本次工作。

            if (Interlocked.CompareExchange(
                    ref _sendState,
                    SendStateQueued,
                    SendStateIdle) != SendStateIdle)
            {
                continue;
            }

            // Close 可发生在入口检查与 Idle→Queued 之间。只用精确 CAS 回滚，
            // 不能 Exchange 覆盖已经取得 Running 的 worker。
            if (!IsConnected)
            {
                Interlocked.CompareExchange(
                    ref _sendState,
                    SendStateIdle,
                    SendStateQueued);
                return;
            }

            if (!_outboundPump.TrySchedule(this))
            {
                // ready channel 已关闭：若尚未被 worker 取得，则精确回退 Queued→Idle。
                Interlocked.CompareExchange(
                    ref _sendState,
                    SendStateIdle,
                    SendStateQueued);
                return;
            }

            // TrySchedule 与 Close 的窗口再检查一次；ready queue 中的旧引用未来会在
            // Queued→Running CAS 失败后 no-op。若 worker 已 Running，Dispose 会等待 finalizing。
            if (!IsConnected)
            {
                Interlocked.CompareExchange(
                    ref _sendState,
                    SendStateIdle,
                    SendStateQueued);
            }

            return;
        }
    }

    /// <summary>
    /// PerSessionDrain 模式的自有按需 drain：持续消费出站 Channel 直到队列空。
    /// <para>
    /// 与 <see cref="PumpOutboundAsync"/> 的区别：
    /// <list type="bullet">
    /// <item>无 burst 上限——drain 持续消费直到队列空（每连接独立，无需公平轮转）；</item>
    /// <item>无 ready queue 调度——<c>Idle→Publishing→Running</c> 完整发布后直接启动，Socket continuation 恢复同一 drain；</item>
    /// <item>慢 Socket 不占用全局 Worker 名额——每连接独立 drain。</item>
    /// </list>
    /// </para>
    /// <para>
    /// 状态机：drain 持有 <paramref name="op"/> 作为 generation completion 令牌。
    /// 退出时 Running→Finalizing，在单消费者所有权内做最后一次 readable 检查；
    /// Complete 后才发布 Idle，随后仅依据已捕获的 Pending 信号重新调度，不再访问 consumer cursor。
    /// </para>
    /// </summary>
    private async Task RunPerSessionDrainAsync(DrainOperation op, int generation)
    {
        var ownershipEpoch = 0;
        try
        {
            // 发送所有权注册：drain 活跃期间驻留活跃集合，drain 退出时释放。
            // 放在 try 内，确保注册异常也会进入 finalizer 完成本代。
            ownershipEpoch = _sendTimeoutTracker?.OnSendOwnershipAcquired(this) ?? 0;

            while (IsConnected)
            {
                if (!_outbound.TryRead(out var write))
                {
                    // FIFO 空：检查 ephemeral mailbox（机会式排空，处理 sentinel TryWrite 失败的丢失唤醒）。
                    if (HasEphemeralEntries)
                    {
                        _ephemeralFlushPending = false;
                        await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                        continue;
                    }

                    return; // finalizer 在仍持有所有权时完成最后一次 re-check。
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
                if (HasEphemeralEntries)
                {
                    _ephemeralFlushPending = false;
                    await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                }
            }

        }
        catch (OperationCanceledException)
            when (_lifetime.Token.IsCancellationRequested)
        {
            // 正常关闭：DisposeAsync 等待本代 drain 释放所有权后唯一排空残留帧。
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
            FinalizePerSessionDrain(op, generation, ownershipEpoch);
        }
    }

    private void FinalizePerSessionDrain(
        DrainOperation op,
        int generation,
        int ownershipEpoch)
    {
        var running = DrainStateRunning | (uint)generation;
        var finalizing = DrainStateFinalizing | (uint)generation;
        var transitioned = Interlocked.CompareExchange(
            ref _drainStateGen,
            finalizing,
            running) == running;

        // Running 时入队的 producer 不会标 Pending，因此必须在 Finalizing 独占期
        // 做一次实际 readable 检查。检查后到 Idle 之间的新 producer 会 CAS Pending。
        var pendingWork = transitioned && IsConnected && HasPendingWork();

        // 发送所有权释放：从活跃集合移除（仅当 Epoch 匹配）。Close 路径的 OnSessionClosed 为兜底。
        if (ownershipEpoch != 0)
            _sendTimeoutTracker?.OnSendOwnershipReleased(this, ownershipEpoch);

        // Complete 必须发生在 Idle 之前；Idle 是允许下一代 Reset 的唯一入口。
        op.Complete(generation);

        if (!transitioned)
            return;

        var shouldSchedule = pendingWork;
        while (true)
        {
            var observed = Interlocked.Read(ref _drainStateGen);
            var phase = observed & DrainStatePhaseMask;
            if ((uint)observed != (uint)generation ||
                (phase != DrainStateFinalizing &&
                 phase != DrainStateFinalizingPending))
            {
                return;
            }

            shouldSchedule |= phase == DrainStateFinalizingPending;
            if (Interlocked.CompareExchange(
                    ref _drainStateGen,
                    (uint)generation,
                    observed) != observed)
            {
                continue;
            }

            // Idle 后禁止再碰 TryRead/TryPeek；这里只消费之前捕获的 producer-safe 信号。
            if (shouldSchedule && IsConnected)
                TryScheduleSend();
            return;
        }
    }

    /// <summary>
    /// OnDemandSendPump 模式：由 <see cref="OutboundPumpCoordinator"/> worker 调用，
    /// 处理最多 <paramref name="maxBurst"/> 个出站帧（durable FIFO + ephemeral mailbox）。
    /// <para>
    /// Finalizing 状态机保证任意时刻一个 session 最多只有一个 worker 持有发送所有权：
    /// <list type="bullet">
    /// <item>worker 出队后 CAS Queued→Running 取得所有权；</item>
    /// <item>pump 结束先 CAS Running→Finalizing，在所有权内完成最后一次 readable 检查；</item>
    /// <item>该窗口的新 producer 仅把状态提升为 FinalizingPending，不访问 consumer cursor；</item>
    /// <item>释放 Tracker 后，finalizer 再发布 Queued 或 Idle 的完整 handoff。</item>
    /// </list>
    /// </para>
    /// </summary>
    public async ValueTask PumpOutboundAsync(int maxBurst, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxBurst, 0);

        // Queued → Running：取得发送所有权。若状态已变（停机回退等），让出 worker。
        if (Interlocked.CompareExchange(ref _sendState, SendStateRunning, SendStateQueued) != SendStateQueued)
            return;

        var ownershipEpoch = 0;
        try
        {
            // 发送所有权注册：pump 活跃期间驻留活跃集合，pump 退出时释放。
            // 放在 try 内，确保注册异常也不会把 _sendState 永久卡在 Running。
            ownershipEpoch = _sendTimeoutTracker?.OnSendOwnershipAcquired(this) ?? 0;

            var processed = 0;
            while (processed < maxBurst && IsConnected)
            {
                if (!_outbound.TryRead(out var write))
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
                if (HasEphemeralEntries)
                {
                    _ephemeralFlushPending = false;
                    await DrainEphemeralMailboxAsync().ConfigureAwait(false);
                }
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
            FinalizeOnDemandPump(ownershipEpoch);
        }
    }

    private void FinalizeOnDemandPump(int ownershipEpoch)
    {
        // 延长单消费者所有权到最后一次 TryPeek 完成。producer 在 Finalizing 期间
        // 只把状态提升为 Pending，不会并发触碰 LazySegmented 的 consumer cursor。
        var transitioned = Interlocked.CompareExchange(
            ref _sendState,
            SendStateFinalizing,
            SendStateRunning) == SendStateRunning;
        var pendingWork = transitioned && IsConnected && HasPendingWork();

        // Queued/Idle 只在 Tracker release 之后发布，使 Dispose 观察到的 handoff 完整。
        if (ownershipEpoch != 0)
            _sendTimeoutTracker?.OnSendOwnershipReleased(this, ownershipEpoch);

        if (!transitioned)
            return;

        var shouldSchedule = pendingWork;
        while (true)
        {
            var observed = Volatile.Read(ref _sendState);
            if (observed != SendStateFinalizing &&
                observed != SendStateFinalizingPending)
            {
                return;
            }

            shouldSchedule |= observed == SendStateFinalizingPending;

            if (!IsConnected)
            {
                if (Interlocked.CompareExchange(
                        ref _sendState,
                        SendStateIdle,
                        observed) == observed)
                {
                    return;
                }

                continue;
            }

            if (!shouldSchedule)
            {
                // producer 若恰好在此窗口入队，会先把 Finalizing CAS 为 Pending，
                // 使本次 CAS 失败；producer 若在 Idle 后入队，则由它自己 schedule。
                if (Interlocked.CompareExchange(
                        ref _sendState,
                        SendStateIdle,
                        SendStateFinalizing) == SendStateFinalizing)
                {
                    return;
                }

                continue;
            }

            if (Interlocked.CompareExchange(
                    ref _sendState,
                    SendStateQueued,
                    observed) != observed)
            {
                continue;
            }

            // Queued 发布前 consumer re-check 与 Tracker release 已全部完成。
            // Close 若赢在发布后，精确回滚 Queued；若新 worker 已 Running，则由 Dispose 等待它。
            if (!IsConnected)
            {
                Interlocked.CompareExchange(
                    ref _sendState,
                    SendStateIdle,
                    SendStateQueued);
                return;
            }

            if (!_outboundPump!.TrySchedule(this))
            {
                Interlocked.CompareExchange(
                    ref _sendState,
                    SendStateIdle,
                    SendStateQueued);
                return;
            }

            if (!IsConnected)
            {
                Interlocked.CompareExchange(
                    ref _sendState,
                    SendStateIdle,
                    SendStateQueued);
            }
            return;
        }
    }

    /// <summary>
    /// 是否有待处理的出站帧（FIFO 或 ephemeral mailbox 非空）。
    /// 用于 pump 结束时的 re-check，判断是否需要重新调度。
    /// </summary>
    private bool HasPendingWork() =>
        _outbound.TryPeek(out _) || HasEphemeralEntries;

    /// <summary>
    /// 排空出站 FIFO 与 ephemeral mailbox 中残留的帧，释放预算与帧引用。
    /// 由 <see cref="SendLoopAsync"/> finally（PersistentSendLoop 模式）、等待活跃 drain 后的
    /// <see cref="DisposeAsync"/>（PerSessionDrain）或等待 pump 所有权释放后的 DisposeAsync
    /// （OnDemandSendPump）调用。调用方必须保证同一 session 没有并发消费者；
    /// EphemeralMailbox.Drain 复用内部 List，不支持并发 Drain。
    /// </summary>
    private void DrainOutboundOnClose()
    {
        // 排空 FIFO 中残留的 writes（含 sentinel，Frame 可能为 null）。
        while (_outbound.TryRead(out var pending))
        {
            // sentinel 不占预算、也没有 frame。ReleaseQueuedWrite 要求 byteCount > 0，
            // 因此只对真实 write 做预算与引用释放。
            if (pending.Frame is null)
                continue;

            ReleaseQueuedWrite(pending.ByteCount);
            pending.Frame.Dispose();
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
        // mailbox 可能为 null（Specialized 模式或连接从未收到 ephemeral 帧）。
        var toSend = _ephemeralMailbox?.Drain();
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
        // mailbox 可能为 null（Specialized 模式或连接从未收到 ephemeral 帧）。
        var toDispose = _ephemeralMailbox?.Drain();
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
