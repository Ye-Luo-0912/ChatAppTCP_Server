using System.Buffers;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal sealed partial class SessionRuntime
{
    /// <summary>
    /// 固定池化接收缓冲区上的增量解析循环：
    /// 小帧和粘包直接在接收缓冲区解析；只有跨缓冲 Payload 才单独租用数组，
    /// 并直接把后续 Socket 字节读入最终命令缓冲区，避免 Pipe segment 与二次复制。
    /// </summary>
    private async Task RunDirectSocketAsync(
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        var receiveBuffer =
            ArrayPool<byte>.Shared.Rent(_options.ReceiveBufferSize);
        var start = 0;
        var end = 0;
        var bufferedReservedBytes = 0;

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
                        break;

                    // 大帧跨越固定接收缓冲区：直接租最终 Payload 缓冲区。
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

                        var completed = await ReceivePayloadRemainderAsync(
                                session,
                                payloadBuffer,
                                availablePayload,
                                payloadLength,
                                cancellationToken)
                            .ConfigureAwait(false);
                        if (!completed)
                        {
                            _metrics.ProtocolError();
                            session.Close(
                                SessionCloseReason.RemoteClosed);
                            return;
                        }

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

                bufferedReservedBytes += bytesRead;
                end += bytesRead;
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

    private static async ValueTask<bool>
        ReceivePayloadRemainderAsync(
            TcpClientSession session,
            byte[] payloadBuffer,
            int received,
            int payloadLength,
            CancellationToken cancellationToken)
    {
        while (received < payloadLength)
        {
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
