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
    /// <item>空闲超过 <see cref="TcpGatewayOptions.ReceiveBufferDowngradeIdleTimeout"/> 后降级回初始大小；</item>
    /// <item>帧装配 deadline：<see cref="TcpGatewayOptions.HeaderAssemblyTimeout"/> 与
    ///   <see cref="TcpGatewayOptions.PayloadAssemblyTimeout"/> 防御慢速攻击。</item>
    /// </list>
    /// </para>
    /// </summary>
    private async Task RunDirectSocketAsync(
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 动态缓冲区：初始小，按需升级，空闲降级。
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

        // 帧装配 deadline 跟踪：记录第一个不完整字节到达的时间戳。
        // 0 = 当前无不完整帧（buffer 为空或刚完成一帧）。
        var partialFrameStartTimestamp = 0L;
        // DeadlineWheel 注册句柄：有部分帧时注册超时回调，ReceiveAsync 返回后取消。
        // 超时回调关闭 Socket，使挂起的 ReceiveAsync 被唤醒，避免永久阻塞。
        var assemblyDeadlineReg = default(DeadlineRegistration);

        // 上次 Receive 的时间戳，用于空闲降级判断。
        var lastReceiveTimestamp = _timeProvider.GetTimestamp();

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
                        // 完整帧在缓冲区内：标记帧装配完成。
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
                                cancellationToken)
                            .ConfigureAwait(false);

                        start += frameLength;
                        bufferedReservedBytes -= frameLength;
                        _globalInboundBudget.Release(frameLength);
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

                        // Payload 装配 deadline 检查：在 ReceivePayloadRemainderAsync 内逐块检查。
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

                        // Payload 装配完成。
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
                                cancellationToken,
                                payloadBuffer,
                                ownedPayloadBudgetReserved: true)
                            .ConfigureAwait(false))
                        {
                            return;
                        }
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

                // 空闲降级：完成一帧后（无残留数据且无进行中的帧装配），
                // 如果缓冲区大于初始大小，立即归还大缓冲区并租初始大小缓冲区。
                // 这比"等待空闲 60 秒后降级"更可靠：不依赖额外 Timer，且
                // 保证空闲 Receive 始终使用基础 buffer，大 buffer 仅在 burst 期间临时持有。
                if (end - start == 0 &&
                    partialFrameStartTimestamp == 0 &&
                    receiveBuffer.Length > Math.Max(
                        _options.ReceiveBufferInitialSize,
                        PacketProtocol.HeaderSize))
                {
                    ArrayPool<byte>.Shared.Return(receiveBuffer);
                    currentBufferSize = Math.Max(
                        _options.ReceiveBufferInitialSize,
                        PacketProtocol.HeaderSize);
                    receiveBuffer = ArrayPool<byte>.Shared.Rent(currentBufferSize);
                    start = 0;
                    end = 0;
                }

                // 帧装配 deadline 注册：若有不完整帧，注册超时回调。
                // 回调在 DeadlineWheel sweep 线程触发，关闭 Socket 使挂起的 ReceiveAsync 唤醒。
                // 不给每次 Receive 创建 CTS/Timer，复用全局 DeadlineWheel。
                if (partialFrameStartTimestamp != 0 && assemblyDeadlineReg.Id == 0)
                {
                    var isHeaderAssembly = end - start < PacketProtocol.HeaderSize;
                    var deadline = isHeaderAssembly
                        ? _options.HeaderAssemblyTimeout
                        : _options.PayloadAssemblyTimeout;
                    if (deadline > TimeSpan.Zero)
                    {
                        assemblyDeadlineReg = _deadlineWheel.Register(deadline, () =>
                        {
                            // 超时回调：关闭 Socket，使挂起的 ReceiveAsync 抛出异常被唤醒。
                            // session.Close 是幂等的，重复调用安全。
                            _metrics.ProtocolError();
                            session.Close(SessionCloseReason.SlowFrameAssembly);
                        });
                    }
                }

                var bytesRead = await session
                    .ReceiveAsync(
                        receiveBuffer.AsMemory(
                            end,
                            receiveBuffer.Length - end),
                        cancellationToken)
                    .ConfigureAwait(false);

                // ReceiveAsync 返回后取消装配超时注册（无论成功或异常）。
                if (assemblyDeadlineReg.Id != 0)
                {
                    _deadlineWheel.Cancel(assemblyDeadlineReg);
                    assemblyDeadlineReg = default;
                }
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
                lastReceiveTimestamp = _timeProvider.GetTimestamp();
            }
        }
        finally
        {
            // 确保取消残留的帧装配超时注册，避免回调在 buffer 已归还后触发。
            if (assemblyDeadlineReg.Id != 0)
            {
                _deadlineWheel.Cancel(assemblyDeadlineReg);
                assemblyDeadlineReg = default;
            }

            if (bufferedReservedBytes > 0)
            {
                _globalInboundBudget.Release(
                    bufferedReservedBytes);
            }

            ArrayPool<byte>.Shared.Return(receiveBuffer);
        }
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
        while (received < payloadLength)
        {
            // Payload 装配 deadline 检查。
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

            // 注册剩余装配时间的超时回调，防止 ReceiveAsync 永久挂起。
            // 超时后关闭 Socket，使 ReceiveAsync 被唤醒。
            DeadlineRegistration payloadReg = default;
            if (payloadDeadline > TimeSpan.Zero)
            {
                var elapsed = _timeProvider.GetElapsedTime(assemblyStartTimestamp);
                var remaining = payloadDeadline - elapsed;
                if (remaining > TimeSpan.Zero)
                {
                    payloadReg = _deadlineWheel.Register(remaining, () =>
                    {
                        _metrics.ProtocolError();
                        session.Close(SessionCloseReason.SlowFrameAssembly);
                    });
                }
            }

            int bytesRead;
            try
            {
                bytesRead = await session
                    .ReceiveAsync(
                        payloadBuffer.AsMemory(
                            received,
                            payloadLength - received),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                if (payloadReg.Id != 0)
                    _deadlineWheel.Cancel(payloadReg);
            }

            if (bytesRead == 0)
                return false;

            received += bytesRead;
        }

        return true;
    }
}
