using System.Buffers;
using ChatApp.TcpGateway.Core.Protocol;

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
                        receiveBuffer = UpgradeReceiveBuffer(
                            receiveBuffer,
                            start,
                            end,
                            maxBufferSize);
                        currentBufferSize = maxBufferSize;
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

                // 空闲降级检查：若超过降级空闲时间且缓冲区大于初始大小，降级。
                if (_options.ReceiveBufferDowngradeIdleTimeout > TimeSpan.Zero &&
                    receiveBuffer.Length > currentBufferSize &&
                    end - start == 0)
                {
                    var idleElapsed = _timeProvider.GetElapsedTime(
                        lastReceiveTimestamp);
                    if (idleElapsed >= _options.ReceiveBufferDowngradeIdleTimeout)
                    {
                        // 降级：归还大缓冲区，租初始大小缓冲区。
                        // end - start == 0 保证无数据丢失。
                        ArrayPool<byte>.Shared.Return(receiveBuffer);
                        currentBufferSize = Math.Max(
                            _options.ReceiveBufferInitialSize,
                            PacketProtocol.HeaderSize);
                        receiveBuffer = ArrayPool<byte>.Shared.Rent(currentBufferSize);
                    }
                }

                var bytesRead = await session
                    .ReceiveAsync(
                        receiveBuffer.AsMemory(
                            end,
                            receiveBuffer.Length - end),
                        cancellationToken)
                    .ConfigureAwait(false);
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
}
