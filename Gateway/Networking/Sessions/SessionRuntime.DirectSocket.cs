using System.Buffers;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Networking.Executor;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal sealed partial class SessionRuntime
{
    /// <summary>
    /// 动态池化接收缓冲区上的增量解析循环：
    /// 小帧和粘包直接在接收缓冲区解析；只有跨缓冲 Payload 才单独租用数组，
    /// 并直接把后续 Socket 字节读入最终命令缓冲区，避免 Pipe segment 与二次复制。
    /// <para>
    /// 动态缓冲区策略：
    /// <list type="bullet">
    /// <item>初始 <see cref="TcpGatewayOptions.ReceiveBufferInitialSize"/>（默认 1 KiB）；</item>
    /// <item>帧无法容纳但可容纳在 <see cref="TcpGatewayOptions.ReceiveBufferMaxSize"/> 时自动升级；</item>
    /// <item>升级后按最近大帧时间保留；保留窗口到期且下一帧完成、缓存为空时才降级，
    ///   无 per-session timer，并防止 burst 大帧场景下 flapping；</item>
    /// <item>帧装配 deadline：<see cref="TcpGatewayOptions.HeaderAssemblyTimeout"/> 与
    ///   <see cref="TcpGatewayOptions.PayloadAssemblyTimeout"/> 防御慢速攻击。</item>
    /// </list>
    /// </para>
    /// </summary>
    private async Task RunDirectSocketAsync(
        TcpClientSession session,
        string remoteIp,
        SessionCommandRegistrationSet registrations,
        CancellationToken cancellationToken)
    {
        // 动态缓冲区：初始小，按需升级，大帧保留窗口到期后的安全点降级。
        var currentBufferSize = Math.Max(
            _options.ReceiveBufferInitialSize,
            PacketProtocol.HeaderSize);
        var maxBufferSize = Math.Max(
            _options.ReceiveBufferMaxSize,
            currentBufferSize);
        var receiveBuffer = ArrayPool<byte>.Shared.Rent(currentBufferSize);
        var start = 0;
        var end = 0;
        var bufferedReservedBytes = 0;

        // 动态接收缓冲区滞后策略（P1-2）：记录最近一帧大帧完成时间。
        // ReceiveBufferLargeFrameRetention 本身就是时间滞后窗口，避免再叠加固定帧数，
        // 否则保留窗口到期后仍需额外完成 N 个小帧才能释放大缓冲区。
        // 仅在 currentBufferSize > base 且帧已完整时读取；该状态必然先完成过大帧，
        // 因而 0 只是 definite-assignment 初值，不作为 TimeProvider 时间戳哨兵。
        var lastLargeFrameTimestamp = 0L;
        var baseBufferSize = Math.Max(
            _options.ReceiveBufferInitialSize,
            PacketProtocol.HeaderSize);

        // 帧装配 deadline 跟踪：记录第一个不完整字节到达的时间戳。
        // 0 = 当前无不完整帧（buffer 为空或刚完成一帧）。
        var partialFrameStartTimestamp = 0L;
        // P1-5：FrameAssemblyTimeoutTracker 替代 DeadlineWheel 管理帧装配超时。
        // 装配开始时 OnAssemblyStarted 注册（返回 FrameAssemblyState 引用），
        // 帧完成/阶段切换时 OnAssemblyCompleted 注销（引用相等性防 ABA）。
        // 扫描线程周期检查 GetElapsedTime(start) >= timeout，超时则 session.Close。
        FrameAssemblyState? assemblyState = null;
        // 当前注册对应的装配阶段（true=Header 装配，false=Payload 装配）。
        // 用于检测 header→payload 阶段切换并重新注册（Payload timeout 通常不同）。
        var assemblyRegForHeaderPhase = false;

        // 注销当前帧装配注册（如有）。
        // 在帧完成、进入 ReceivePayloadRemainderAsync、方法退出时调用。
        void CancelAssemblyDeadline()
        {
            if (assemblyState is { } state)
            {
                _frameAssemblyTracker?.OnAssemblyCompleted(session, state);
                assemblyState = null;
            }
        }

        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   session.IsConnected)
            {
                while (session.IsConnected &&
                       end - start >= PacketProtocol.HeaderSize)
                {
                    var headerStatus = PacketParser.TryParseHeader(
                        receiveBuffer.AsSpan(
                            start,
                            PacketProtocol.HeaderSize),
                        out var command,
                        out var payloadLength);
                    if (headerStatus != PacketParseStatus.Success)
                    {
                        RejectInvalidPacket(session);
                        return;
                    }

                    if (!ValidatePreAuthenticationCommand(
                            session,
                            command))
                    {
                        return;
                    }

                    if (!InboundPayloadEarlyValidator
                            .IsPayloadWithinLimit(
                                payloadLength,
                                _options.MaxInboundPayloadBytes))
                    {
                        _metrics.ProtocolError();
                        _rejectOversizedPayload(session, command);
                        return;
                    }

                    var available = end - start;
                    var frameLength =
                        PacketProtocol.HeaderSize + payloadLength;
                    if (available >= frameLength)
                    {
                        // 完整帧在缓冲区内：取消装配 deadline，标记帧装配完成。
                        CancelAssemblyDeadline();
                        partialFrameStartTimestamp = 0;

                        var frame = new PacketFrame(
                            command,
                            payloadLength == 0
                                ? ReadOnlySequence<byte>.Empty
                                : new ReadOnlySequence<byte>(
                                    receiveBuffer,
                                    start + PacketProtocol.HeaderSize,
                                    payloadLength));
                        var keepReading = await DispatchFrameAsync(
                                frame,
                                session,
                                remoteIp,
                                registrations,
                                cancellationToken)
                            .ConfigureAwait(false);

                        start += frameLength;
                        bufferedReservedBytes -= frameLength;
                        _globalInboundBudget.Release(frameLength);

                        // 大帧完成后从当前时刻开始计算保留窗口。
                        if (frameLength > baseBufferSize)
                            lastLargeFrameTimestamp = _timeProvider.GetTimestamp();

                        if (!keepReading)
                            return;

                        continue;
                    }

                    // 当前帧可完全容纳在固定接收缓冲区，先继续 Receive。
                    if (frameLength <= receiveBuffer.Length)
                    {
                        // Header 已完整但 Payload 不完整：进入 Payload 装配阶段。
                        // 若 partialFrameStartTimestamp 仍为 0（刚完成上一帧），
                        // 记录当前时间作为 Payload 装配起点。
                        if (partialFrameStartTimestamp == 0)
                            partialFrameStartTimestamp = _timeProvider.GetTimestamp();
                        break;
                    }

                    // 大帧跨越当前缓冲区：检查是否可升级到 maxBufferSize。
                    if (frameLength <= maxBufferSize &&
                        receiveBuffer.Length < maxBufferSize)
                    {
                        // 升级缓冲区：租更大 buffer，复制残留数据，归还旧 buffer。
                        // 升级后数据在新 buffer 的 0..remaining 位置，必须重置 start/end。
                        var remaining = end - start;
                        receiveBuffer = UpgradeReceiveBuffer(
                            receiveBuffer,
                            start,
                            end,
                            maxBufferSize);
                        currentBufferSize = maxBufferSize;
                        start = 0;
                        end = remaining;
                        // 不 break：重新进入 while 循环，frameLength <= receiveBuffer.Length 时走上面路径。
                        continue;
                    }

                    // 大帧仍超出 maxBufferSize：直接租最终 Payload 缓冲区。
                    // 进入 Payload 装配阶段（检查 deadline）。
                    if (partialFrameStartTimestamp == 0)
                        partialFrameStartTimestamp = _timeProvider.GetTimestamp();

                    if (!_globalInboundBudget.TryReserve(payloadLength))
                    {
                        session.Close(
                            SessionCloseReason.InboundBudgetExceeded);
                        return;
                    }

                    byte[]? payloadBuffer = null;
                    var dispatchOwnsPayload = false;
                    try
                    {
                        payloadBuffer =
                            ArrayPool<byte>.Shared.Rent(payloadLength);
                        var availablePayload =
                            available - PacketProtocol.HeaderSize;
                        if (availablePayload > 0)
                        {
                            Buffer.BlockCopy(
                                receiveBuffer,
                                start + PacketProtocol.HeaderSize,
                                payloadBuffer,
                                0,
                                availablePayload);
                        }

                        // receiveBuffer 内现有字节已转移/消费，释放其缓冲预算。
                        start = end;
                        bufferedReservedBytes -= available;
                        _globalInboundBudget.Release(available);

                        // 取消主循环的帧装配 deadline：ReceivePayloadRemainderAsync 内部
                        // 会注册自己的 payload 装配 deadline（基于 assemblyStartTimestamp 的剩余时间）。
                        // 避免两套 deadline 并存导致重复 Close。
                        CancelAssemblyDeadline();

                        // Payload 装配 deadline 检查：在 ReceivePayloadRemainderAsync 内单次注册。
                        var completed = await ReceivePayloadRemainderAsync(
                                session,
                                payloadBuffer,
                                availablePayload,
                                payloadLength,
                                partialFrameStartTimestamp,
                                cancellationToken)
                            .ConfigureAwait(false);
                        if (!completed)
                        {
                            _metrics.ProtocolError();
                            session.Close(
                                SessionCloseReason.RemoteClosed);
                            return;
                        }

                        // Payload 装配完成：取消装配 deadline（防御性，已在进入 Remainder 前取消）。
                        CancelAssemblyDeadline();
                        partialFrameStartTimestamp = 0;

                        var frame = new PacketFrame(
                            command,
                            new ReadOnlySequence<byte>(
                                payloadBuffer,
                                0,
                                payloadLength));

                        // DispatchFrameAsync 从调用开始即负责成功、拒绝和异常路径的资源归还。
                        dispatchOwnsPayload = true;
                        if (!await DispatchFrameAsync(
                                frame,
                                session,
                                remoteIp,
                                registrations,
                                cancellationToken,
                                payloadBuffer,
                                ownedPayloadBudgetReserved: true)
                            .ConfigureAwait(false))
                        {
                            return;
                        }

                        // 超过 maxBufferSize 的帧走独立 payload buffer，也属于大帧活动。
                        // 若当前连接此前已升级，必须刷新其保留窗口，避免刚完成大帧就降级。
                        if (frameLength > baseBufferSize)
                            lastLargeFrameTimestamp = _timeProvider.GetTimestamp();
                    }
                    finally
                    {
                        if (!dispatchOwnsPayload)
                        {
                            if (payloadBuffer is not null)
                                ArrayPool<byte>.Shared.Return(payloadBuffer);
                            _globalInboundBudget.Release(payloadLength);
                        }
                    }
                }
                if (start > 0)
                {
                    var remaining = end - start;
                    if (remaining > 0)
                    {
                        Buffer.BlockCopy(
                            receiveBuffer,
                            start,
                            receiveBuffer,
                            0,
                            remaining);
                    }

                    start = 0;
                    end = remaining;
                }

                // 帧装配 deadline 检查：若有不完整帧，检查是否超时。
                if (partialFrameStartTimestamp != 0)
                {
                    var elapsed = _timeProvider.GetElapsedTime(
                        partialFrameStartTimestamp);
                    // 判断当前是 Header 还是 Payload 装配阶段：
                    // end - start < HeaderSize → Header 装配
                    // end - start >= HeaderSize → Payload 装配
                    var isHeaderAssembly = end - start < PacketProtocol.HeaderSize;
                    var deadline = isHeaderAssembly
                        ? _options.HeaderAssemblyTimeout
                        : _options.PayloadAssemblyTimeout;
                    if (deadline > TimeSpan.Zero && elapsed >= deadline)
                    {
                        _metrics.ProtocolError();
                        session.Close(SessionCloseReason.SlowFrameAssembly);
                        return;
                    }
                }

                // 仅在 buffer 为空且没有半帧的安全点降级。无 per-session timer：
                // 网络静默时不唤醒连接；下一帧完成后若距最近大帧已超过保留窗口，立即释放大 buffer。
                if (ShouldDowngradeReceiveBuffer(
                        currentBufferSize,
                        baseBufferSize,
                        end - start,
                        partialFrameStartTimestamp,
                        lastLargeFrameTimestamp,
                        _options.ReceiveBufferLargeFrameRetention,
                        _timeProvider))
                {
                    // 先 Rent，再交换所有权，最后归还旧 buffer。若 Rent 抛异常，
                    // receiveBuffer 仍指向旧 owner，外层 finally 只会归还它一次。
                    ReplaceEmptyReceiveBuffer(
                        ref receiveBuffer,
                        baseBufferSize,
                        ArrayPool<byte>.Shared);
                    currentBufferSize = baseBufferSize;
                    start = 0;
                    end = 0;
                }
                // P1-5：帧装配超时注册——FrameAssemblyTimeoutTracker 替代 DeadlineWheel。
                // 单次绝对超时 per 帧装配：不每次 ReceiveAsync 后重注册，
                // 否则慢速客户端可通过分片字节无限延长超时（每次 Receive 重置全量 timeout）。
                // 仅在以下情况注册/重注册：
                // 1. 新帧装配开始（partialFrameStartTimestamp != 0 且无活跃注册）
                // 2. header→payload 阶段切换（timeout 值可能变化）
                if (partialFrameStartTimestamp != 0 && _frameAssemblyTracker is not null)
                {
                    var isHeaderAssembly = end - start < PacketProtocol.HeaderSize;
                    var deadline = isHeaderAssembly
                        ? _options.HeaderAssemblyTimeout
                        : _options.PayloadAssemblyTimeout;

                    if (deadline > TimeSpan.Zero)
                    {
                        // 检测阶段切换：当前注册阶段与实际阶段不符时重注册。
                        var phaseChanged = assemblyState is not null &&
                                           isHeaderAssembly != assemblyRegForHeaderPhase;

                        if (assemblyState is null || phaseChanged)
                        {
                            if (phaseChanged)
                            {
                                // 阶段切换：注销旧注册（header 阶段），用 payload timeout 重注册。
                                CancelAssemblyDeadline();
                            }

                            // 注册/重注册：OnAssemblyStarted 返回 FrameAssemblyState 引用。
                            // 扫描线程用 GetElapsedTime(start) >= timeout 判断超时，超时则 session.Close。
                            // 无需 epoch int[1] 容器——引用相等性天然防 ABA。
                            assemblyState = _frameAssemblyTracker.OnAssemblyStarted(session, deadline);
                            assemblyRegForHeaderPhase = isHeaderAssembly;
                        }
                    }
                    else if (assemblyState is not null)
                    {
                        // 当前阶段超时被禁用（<= 0）：注销活跃注册。
                        CancelAssemblyDeadline();
                    }
                }

                var bytesRead = await session
                    .ReceiveAsync(
                        receiveBuffer.AsMemory(
                            end,
                            receiveBuffer.Length - end),
                        cancellationToken)
                    .ConfigureAwait(false);

                // 注意：不在 ReceiveAsync 返回后取消 deadline——单次绝对 deadline 跨多次 Receive 保持活跃，
                // 直到帧完成或超时。这是修复 P0-3 的核心：避免每次 Receive 重置全量 timeout。
                if (bytesRead == 0)
                {
                    if (end != 0)
                        _metrics.ProtocolError();
                    session.Close(SessionCloseReason.RemoteClosed);
                    return;
                }

                if (!_globalInboundBudget.TryReserve(bytesRead))
                {
                    session.Close(
                        SessionCloseReason.InboundBudgetExceeded);
                    return;
                }

                // 若此前 buffer 为空（刚完成一帧），记录不完整帧起点。
                if (end - start == 0 && partialFrameStartTimestamp == 0)
                    partialFrameStartTimestamp = _timeProvider.GetTimestamp();

                bufferedReservedBytes += bytesRead;
                end += bytesRead;
            }
        }
        finally
        {
            // 确保取消残留的帧装配超时注册，避免回调在 buffer 已归还后触发。
            CancelAssemblyDeadline();

            if (bufferedReservedBytes > 0)
            {
                _globalInboundBudget.Release(
                    bufferedReservedBytes);
            }

            ArrayPool<byte>.Shared.Return(receiveBuffer);
        }
    }

    internal static bool ShouldDowngradeReceiveBuffer(
        int currentBufferSize,
        int baseBufferSize,
        int bufferedByteCount,
        long partialFrameStartTimestamp,
        long lastLargeFrameTimestamp,
        TimeSpan largeFrameRetention,
        TimeProvider timeProvider)
    {
        if (currentBufferSize <= baseBufferSize ||
            bufferedByteCount != 0 ||
            partialFrameStartTimestamp != 0 ||
            largeFrameRetention <= TimeSpan.Zero)
        {
            return false;
        }

        return timeProvider.GetElapsedTime(lastLargeFrameTimestamp) >= largeFrameRetention;
    }

    /// <summary>
    /// 在缓存为空的安全点替换接收 buffer。先成功租到 replacement，再交换 owner，
    /// 最后归还旧 buffer，保证 Rent 异常时调用方仍持有且只持有旧 owner。
    /// </summary>
    internal static void ReplaceEmptyReceiveBuffer(
        ref byte[] buffer,
        int newSize,
        ArrayPool<byte> pool)
    {
        var replacement = pool.Rent(newSize);
        var previous = buffer;
        buffer = replacement;
        pool.Return(previous);
    }

    /// <summary>
    /// 升级接收缓冲区：租更大 buffer，复制残留数据，归还旧 buffer。
    /// </summary>
    private static byte[] UpgradeReceiveBuffer(
        byte[] oldBuffer,
        int start,
        int end,
        int newSize)
    {
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        var remaining = end - start;
        if (remaining > 0)
        {
            Buffer.BlockCopy(
                oldBuffer,
                start,
                newBuffer,
                0,
                remaining);
        }
        ArrayPool<byte>.Shared.Return(oldBuffer);
        return newBuffer;
    }

    private async ValueTask<bool>
        ReceivePayloadRemainderAsync(
            TcpClientSession session,
            byte[] payloadBuffer,
            int received,
            int payloadLength,
            long assemblyStartTimestamp,
            CancellationToken cancellationToken)
    {
        var payloadDeadline = _options.PayloadAssemblyTimeout;

        // P1-5：FrameAssemblyTimeoutTracker 替代 DeadlineWheel。
        // 注册 payload 装配超时：扫描线程检查 GetElapsedTime(assemblyStartTimestamp) >= payloadDeadline。
        // 由于 assemblyStartTimestamp 是帧起点（header 接收时记录），单次注册即保证
        // 总时长不超 payloadDeadline，无需每块 ReceiveAsync 后重注册。
        FrameAssemblyState? payloadState = null;

        if (payloadDeadline > TimeSpan.Zero && _frameAssemblyTracker is not null)
        {
            var elapsed = _timeProvider.GetElapsedTime(assemblyStartTimestamp);
            if (elapsed >= payloadDeadline)
            {
                // 已超时。
                _metrics.ProtocolError();
                session.Close(SessionCloseReason.SlowFrameAssembly);
                return false;
            }

            // 注册：Tracker 用当前时间戳作为扫描起点。内联检查用 assemblyStartTimestamp 保证总时长。
            // 两者结合：扫描线程作为兜底（100ms 精度），内联检查作为快速路径（每 ReceiveAsync 前）。
            payloadState = _frameAssemblyTracker.OnAssemblyStarted(session, payloadDeadline);
        }
        try
        {
            while (received < payloadLength)
            {
                // Payload 装配超时内联检查（快速路径，不等 Tracker 扫描）。
                if (payloadDeadline > TimeSpan.Zero)
                {
                    var elapsed = _timeProvider.GetElapsedTime(
                        assemblyStartTimestamp);
                    if (elapsed >= payloadDeadline)
                    {
                        _metrics.ProtocolError();
                        session.Close(SessionCloseReason.SlowFrameAssembly);
                        return false;
                    }
                }

                var bytesRead = await session
                    .ReceiveAsync(
                        payloadBuffer.AsMemory(
                            received,
                            payloadLength - received),
                        cancellationToken)
                    .ConfigureAwait(false);

                if (bytesRead == 0)
                    return false;

                received += bytesRead;
            }

            return true;
        }
        finally
        {
            // P1-5：注销 payload 装配超时注册（引用相等性防 ABA）。
            if (payloadState is not null)
            {
                _frameAssemblyTracker?.OnAssemblyCompleted(session, payloadState);
            }
        }
    }
}
