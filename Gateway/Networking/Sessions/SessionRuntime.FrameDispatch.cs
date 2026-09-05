using System.Buffers;
using System.Text.Json;
using ChatApp.TcpGateway.Core.Protocol;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal sealed partial class SessionRuntime
{
    /// <summary>
    /// 包头到达后立即执行状态相关校验，不等待完整 Payload。
    /// </summary>
    private bool ValidatePreAuthenticationCommand(
        TcpClientSession session,
        PacketCommand command)
    {
        if (session.IsAuthenticated)
            return true;

        if (_options.RequireClientHello &&
            command == PacketCommand.AuthenticationRequest &&
            !session.HasCompletedHandshake)
        {
            _metrics.ProtocolError();
            _sendProtocolError(
                session,
                ProtocolErrorCode.ProtocolViolation,
                "ClientHello required before authentication",
                true,
                null,
                (ushort)command);
            session.Close(SessionCloseReason.ProtocolViolation);
            return false;
        }

        if (PacketProtocol.IsAuthenticationCommand(command))
            return true;

        _metrics.ProtocolError();
        _sendProtocolError(
            session,
            ProtocolErrorCode.ProtocolViolation,
            "command not allowed before authentication",
            true,
            null,
            (ushort)command);
        session.Close(SessionCloseReason.ProtocolViolation);
        return false;
    }

    private void RejectInvalidPacket(TcpClientSession session)
    {
        _metrics.ProtocolError();
        _sendProtocolError(
            session,
            ProtocolErrorCode.ProtocolViolation,
            "invalid packet structure",
            true,
            null,
            null);
        session.Close(SessionCloseReason.ProtocolViolation);
    }

    /// <summary>
    /// Pipelines 与 DirectSocket 的统一帧处理入口。ownedPayloadBuffer 非空时，
    /// Payload 及其全局预算已由 DirectSocket 跨缓冲读取路径持有；成功入队后
    /// 所有权转移给 SessionCommand，其余路径在 finally 归还。
    /// <para>
    /// 标记为 internal 供 <see cref="SessionRuntimeTests"/> 直接驱动验证 lane 路由、
    /// 限流、能力门控、负载体积上限与资源所有权转移；其余不参与协议数据面的代码不调用此方法。
    /// </para>
    /// </summary>
    internal async ValueTask<bool> DispatchFrameAsync(
        PacketFrame frame,
        TcpClientSession session,
        string remoteIp,
        SessionCommandRegistrationSet registrations,
        CancellationToken cancellationToken,
        byte[]? ownedPayloadBuffer = null,
        bool ownedPayloadBudgetReserved = false)
    {
        var payloadLength = (int)frame.Payload.Length;
        var releaseOwnedPayload = ownedPayloadBuffer is not null;
        try
        {
            if (!InboundPayloadEarlyValidator.IsPayloadWithinLimit(
                    payloadLength,
                    _options.MaxInboundPayloadBytes))
            {
                _metrics.ProtocolError();
                _rejectOversizedPayload(session, frame.Command);
                return false;
            }

            // PacketParser / DirectSocket header parser 已校验 catalog；这里取一次描述符，
            // 后续速率成本、弃用状态、能力门控与 lane 共用，避免热路径重复 switch。
            var descriptorOrNull =
                CommandCatalog.TryGetDescriptor(frame.Command);
            if (!descriptorOrNull.HasValue)
            {
                RejectInvalidPacket(session);
                return false;
            }

            var descriptor = descriptorOrNull.GetValueOrDefault();
            var frameByteCount =
                PacketProtocol.HeaderSize + payloadLength;
            if (!session.RecordInboundTraffic(
                    _options.MaxPacketsPerSecond,
                    _options.MaxInboundBytesPerSecond,
                    frameByteCount,
                    descriptor.RateCost))
            {
                _metrics.ProtocolError();
                _sendProtocolError(
                    session,
                    ProtocolErrorCode.RateLimited,
                    "inbound rate limit exceeded",
                    false,
                    1000,
                    (ushort)frame.Command);
                return true;
            }

            _metrics.PacketReceived();
            if (descriptor.Deprecated)
            {
                _metrics.ProtocolError();
                _sendProtocolError(
                    session,
                    ProtocolErrorCode.UnsupportedCommand,
                    $"command {frame.Command} is deprecated",
                    false,
                    null,
                    (ushort)frame.Command);
                return true;
            }

            if (!CommandCatalog.IsFeatureAllowed(
                    in descriptor,
                    session.NegotiatedFeatureBits))
            {
                _metrics.ProtocolError();
                _sendProtocolError(
                    session,
                    ProtocolErrorCode.FeatureNotNegotiated,
                    $"command {frame.Command} requires feature " +
                    $"{descriptor.RequiredFeature}",
                    false,
                    null,
                    (ushort)frame.Command);
                return true;
            }

            var lane = descriptor.Lane;

            // Typing 领域 Actor 快路径：直接解析 payload 并路由到 LatestOnly Actor，
            // 不创建 SessionCommand、不复制 buffer、不预留 inbound budget。
            // 仅在 UseTypingActorPipeline=true 且 TypingNotify 命令时启用。
            // 独立捕获 JsonException：恶意/损坏 JSON 必须被归类为协议错误并关闭连接，
            // 不能一路抛到 SessionRuntime 外层被误记为 TransportError。
            if (_typingActorPipeline is not null &&
                frame.Command == PacketCommand.TypingNotify)
            {
                try
                {
                    _typingActorPipeline.TryHandleFrame(in frame, session);
                    return true;
                }
                catch (Exception ex) when (
                    ex is JsonException ||
                    ex is Serialization.BinaryPayloadDecodeException)
                {
                    _metrics.ProtocolError();
                    _sendProtocolError(
                        session,
                        ProtocolErrorCode.ProtocolViolation,
                        "invalid typing notify payload",
                        true,
                        null,
                        (ushort)frame.Command);
                    session.Close(SessionCloseReason.ProtocolViolation);
                    return false;
                }
            }

            if (lane == CommandLane.Inline)
            {
                try
                {
                    await _processPacketAsync(
                            frame,
                            session,
                            remoteIp,
                            cancellationToken)
                        .ConfigureAwait(false);
                    return true;
                }
                catch (Exception ex) when (
                    ex is JsonException ||
                    ex is Serialization.BinaryPayloadDecodeException)
                {
                    _metrics.ProtocolError();
                    session.Close(
                        SessionCloseReason.ProtocolViolation);
                    return false;
                }
            }

            byte[] commandBuffer;
            if (ownedPayloadBuffer is not null)
            {
                if (!ownedPayloadBudgetReserved)
                {
                    throw new InvalidOperationException(
                        "Owned DirectSocket payload has no inbound budget.");
                }

                commandBuffer = ownedPayloadBuffer;
            }
            else
            {
                if (!_globalInboundBudget.TryReserve(payloadLength))
                {
                    session.Close(
                        SessionCloseReason.InboundBudgetExceeded);
                    return false;
                }

                try
                {
                    commandBuffer = payloadLength > 0
                        ? ArrayPool<byte>.Shared.Rent(payloadLength)
                        : Array.Empty<byte>();
                    if (payloadLength > 0)
                        frame.Payload.CopyTo(commandBuffer);
                }
                catch
                {
                    _globalInboundBudget.Release(payloadLength);
                    throw;
                }
            }

            var command = new SessionCommand
            {
                Command = frame.Command,
                RentedBuffer = commandBuffer,
                PayloadLength = payloadLength,
                IsPooled = payloadLength > 0,
                ReservedInboundBytes = payloadLength,
                InboundBudget = _globalInboundBudget,
                Session = session,
                RemoteIp = remoteIp
            };

            bool enqueued;
            try
            {
                enqueued = registrations.TryEnqueue(lane, in command);
            }
            catch
            {
                SessionCommandResources.Release(in command);
                releaseOwnedPayload = false;
                throw;
            }

            if (enqueued)
            {
                releaseOwnedPayload = false;
                return true;
            }

            SessionCommandResources.Release(in command);
            releaseOwnedPayload = false;
            if (lane == CommandLane.Ephemeral)
                return true;

            session.Close(SessionCloseReason.OutboundQueueFull);
            return false;
        }
        finally
        {
            if (releaseOwnedPayload)
            {
                if (ownedPayloadBuffer!.Length > 0)
                    ArrayPool<byte>.Shared.Return(ownedPayloadBuffer);
                if (ownedPayloadBudgetReserved)
                    _globalInboundBudget.Release(payloadLength);
            }
        }
    }
}
