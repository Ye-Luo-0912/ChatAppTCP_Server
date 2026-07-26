using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Channels;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 每连接数据面运行时：封装 session 生命周期内 Pipe reader/writer +
/// <see cref="SessionCommandScheduler"/> 三件套（fill、read、scheduled-callback），
/// 从 <see cref="Networking.TcpGatewayService"/> 抽取以消除 God Service 中散落的 per-connection 数据路径。
/// <para>
/// 职责边界：
/// <list type="bullet">
/// <item>构造每会话 CTS（链接 session lifetime + host stopping）；</item>
/// <item>构造 <see cref="SessionCommandScheduler"/>（OrderedWrite/Query/Ephemeral 三 lane）；</item>
/// <item>构造 Pipe + <see cref="SessionInboundPipeLease"/>；</item>
/// <item>驱动 FillPipeAsync + ReadPipeAsync 并等待双方退出；</item>
/// <item>数据面清理：pipeLease 归还、session.Close、scheduler.DisposeAsync、session.DisposeAsync。</item>
/// </list>
/// 协议级内联命令处理与早投拒绝（SendProtocolError / RejectOversizedPayload / ProcessPacketAsync）
/// 通过构造函数注入的委托回调到 <see cref="Networking.TcpGatewayService"/>，
/// 避免将大量协议 codec/依赖注入本类型。
/// 服务级清理（Presence 下线、admission 释放、session 注册表移除）由调用方在 RunAsync 返回后处理。
/// </para>
/// <para>
/// 单例注册：所有连接共享同一实例，通过 <see cref="RunAsync"/> 的 session 参数区分连接。
/// </para>
/// </summary>
internal sealed class SessionRuntime
{
    private readonly TcpGatewayOptions _options;
    private readonly PipeOptions _pipeOptions;
    private readonly GlobalInboundBudget _globalInboundBudget;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;

    private readonly Func<PacketFrame, TcpClientSession, string, CancellationToken, ValueTask> _processPacketAsync;
    private readonly Action<TcpClientSession, ProtocolErrorCode, string?, bool, int?, ushort?> _sendProtocolError;
    private readonly Action<TcpClientSession, PacketCommand> _rejectOversizedPayload;

    public SessionRuntime(
        TcpGatewayOptions options,
        PipeOptions pipeOptions,
        GlobalInboundBudget globalInboundBudget,
        GatewayMetrics metrics,
        ILogger logger,
        Func<PacketFrame, TcpClientSession, string, CancellationToken, ValueTask> processPacketAsync,
        Action<TcpClientSession, ProtocolErrorCode, string?, bool, int?, ushort?> sendProtocolError,
        Action<TcpClientSession, PacketCommand> rejectOversizedPayload)
    {
        _options = options;
        _pipeOptions = pipeOptions;
        _globalInboundBudget = globalInboundBudget;
        _metrics = metrics;
        _logger = logger;
        _processPacketAsync = processPacketAsync;
        _sendProtocolError = sendProtocolError;
        _rejectOversizedPayload = rejectOversizedPayload;
    }

    /// <summary>
    /// 驱动单连接数据面：构建 scheduler + Pipe，启动 fill/read 双任务并等待退出。
    /// 返回时已完成数据面清理（pipeLease/scheduler/session）；服务级清理（Presence/admission/registry）由调用方处理。
    /// </summary>
    public async Task RunAsync(
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 链接 Session lifetime token 与宿主 stopping token。
        // 连接关闭时取消所有业务调用，避免后端资源继续被占用。
        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(
            session.LifetimeToken, cancellationToken);
        var sessionToken = sessionCts.Token;

        // 每会话命令调度器。将命令分发到 OrderedWrite/Query/Ephemeral 三条 lane，
        // 避免慢请求阻塞同连接的其他命令（队头阻塞）。
        // Control 命令（Auth/Heartbeat/PresenceUnwatch）由读循环内联处理。
        var scheduler = new SessionCommandScheduler(
            (command, token) => ProcessScheduledCommandAsync(
                command, session, remoteIp, token),
            _options.CommandSchedulerOrderedWriteCapacity,
            _options.CommandSchedulerQueryCapacity,
            _options.CommandSchedulerEphemeralCapacity,
            sessionToken,
            ex => _logger.TransportFailed(GatewayTransportOperation.ClientProcessing, session.ConnectionId, ex));

        var pipe = new Pipe(_pipeOptions);
        var pipeLease = new SessionInboundPipeLease(_globalInboundBudget);
        var fillTask = FillPipeAsync(
            session,
            pipe.Writer,
            pipeLease,
            sessionToken);
        var readTask = ReadPipeAsync(
            pipe.Reader,
            session,
            remoteIp,
            scheduler,
            pipeLease,
            sessionToken);

        try
        {
            await Task.WhenAll(fillTask, readTask).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested ||
                  !session.IsConnected)
        {
            // Expected shutdown path.
        }
        catch (SocketException)
        {
            session.Close(SessionCloseReason.TransportError);
        }
        catch (ObjectDisposedException)
        {
            session.Close(SessionCloseReason.TransportError);
        }
        catch (Exception exception)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                exception);
            session.Close(SessionCloseReason.TransportError);
        }
        finally
        {
            pipeLease.ReleaseAll();
            session.Close(
                cancellationToken.IsCancellationRequested
                    ? SessionCloseReason.ApplicationStopping
                    : SessionCloseReason.RemoteClosed);

            // 先停止命令调度器（等待 lane 消费者退出并归还租用缓冲区），
            // 再释放 Session。避免 Session 释放后调度器仍访问其字段。
            await scheduler.DisposeAsync().ConfigureAwait(false);

            await session.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task FillPipeAsync(
        TcpClientSession session,
        PipeWriter writer,
        SessionInboundPipeLease pipeLease,
        CancellationToken cancellationToken)
    {
        Exception? completionError = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   session.IsConnected)
            {
                var memory = writer.GetMemory(
                    _options.ReceiveBufferSize);
                var bytesRead = await session
                    .ReceiveAsync(memory, cancellationToken)
                    .ConfigureAwait(false);

                if (bytesRead == 0)
                {
                    session.Close(SessionCloseReason.RemoteClosed);
                    break;
                }

                if (!pipeLease.TryReserve(bytesRead))
                {
                    session.Close(SessionCloseReason.InboundBudgetExceeded);
                    break;
                }

                writer.Advance(bytesRead);
                var result = await writer
                    .FlushAsync(cancellationToken)
                    .ConfigureAwait(false);

                if (result.IsCanceled || result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception exception)
        {
            completionError = exception;
            throw;
        }
        finally
        {
            await writer.CompleteAsync(completionError)
                .ConfigureAwait(false);
        }
    }

    private async Task ReadPipeAsync(
        PipeReader reader,
        TcpClientSession session,
        string remoteIp,
        SessionCommandScheduler scheduler,
        SessionInboundPipeLease pipeLease,
        CancellationToken cancellationToken)
    {
        Exception? completionError = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   session.IsConnected)
            {
                var result = await reader
                    .ReadAsync(cancellationToken)
                    .ConfigureAwait(false);
                var readBuffer = result.Buffer;
                var buffer = readBuffer;

                // 跟踪已消费位置。Inline 命令处理后更新此位置；
                // 入队命令在复制 payload 后也更新此位置，使 Pipe 可立即回收内存。
                var consumed = buffer.Start;

                while (session.IsConnected)
                {
                    // 未认证状态下，在等待完整 Payload 前立即拒绝非认证命令。
                    // 攻击者可能声明 ChatMessage（上限 64 KiB）等命令并慢速发送，
                    // 旧实现在完整 Payload 到达后才由 ProcessPacketAsync 拒绝，浪费缓冲与连接。
                    if (!session.IsAuthenticated &&
                        PacketParser.TryPeekCommand(buffer, out var peekedCommand))
                    {
                        // RequireClientHello=true 时，认证前必须先完成 ClientHello 握手。
                        // ClientHello / AuthenticationRequest / Resume 均在 Inline lane 串行处理，
                        // 同一 TCP 段内多帧也不会乱序越过握手状态机。
                        if (_options.RequireClientHello &&
                            peekedCommand == PacketCommand.AuthenticationRequest &&
                            !session.HasCompletedHandshake)
                        {
                            _metrics.ProtocolError();
                            _sendProtocolError(
                                session,
                                ProtocolErrorCode.ProtocolViolation,
                                "ClientHello required before authentication",
                                true,
                                null,
                                (ushort)peekedCommand);
                            session.Close(SessionCloseReason.ProtocolViolation);
                            return;
                        }

                        if (!PacketProtocol.IsAuthenticationCommand(peekedCommand))
                        {
                            _metrics.ProtocolError();
                            _sendProtocolError(
                                session,
                                ProtocolErrorCode.ProtocolViolation,
                                "command not allowed before authentication",
                                true,
                                null,
                                (ushort)peekedCommand);
                            session.Close(SessionCloseReason.ProtocolViolation);
                            return;
                        }
                    }

                    var parseStatus = PacketParser.TryParse(
                        ref buffer,
                        out var frame);

                    if (parseStatus == PacketParseStatus.NeedMoreData)
                    {
                        break;
                    }

                    if (parseStatus == PacketParseStatus.InvalidPacket)
                    {
                        _metrics.ProtocolError();
                        _sendProtocolError(
                            session,
                            ProtocolErrorCode.ProtocolViolation,
                            "invalid packet structure",
                            true,
                            null,
                            null);
                        session.Close(
                            SessionCloseReason.ProtocolViolation);
                        return;
                    }

                    var payloadLength = (int)frame.Payload.Length;
                    if (!InboundPayloadEarlyValidator.IsPayloadWithinLimit(
                            payloadLength,
                            _options.MaxInboundPayloadBytes))
                    {
                        _metrics.ProtocolError();
                        _rejectOversizedPayload(session, frame.Command);
                        return;
                    }

                    var frameByteCount = PacketProtocol.HeaderSize +
                                         payloadLength;
                    var packetCost = PacketProtocol.GetCommandCost(frame.Command);
                    if (!session.RecordInboundTraffic(
                            _options.MaxPacketsPerSecond,
                            _options.MaxInboundBytesPerSecond,
                            frameByteCount,
                            packetCost))
                    {
                        // 限流为可重试错误：跳过当前帧，不关闭连接。
                        // 客户端收到 RateLimited + RetryAfter 后应退避重试。
                        _metrics.ProtocolError();
                        _sendProtocolError(
                            session,
                            ProtocolErrorCode.RateLimited,
                            "inbound rate limit exceeded",
                            false,
                            1000,
                            (ushort)frame.Command);
                        consumed = buffer.Start;
                        continue;
                    }

                    _metrics.PacketReceived();

                    // 按 lane 分类调度。委托 CommandCatalog（单一事实源）。
                    var lane = CommandCatalog.GetLane(frame.Command);

                    if (lane == CommandLane.Inline)
                    {
                        // Control 命令内联处理：ClientHello/Auth/Heartbeat/PresenceUnwatch。
                        // 握手、认证、恢复必须在同一读循环内严格串行，禁止入 OrderedWrite。
                        try
                        {
                            await _processPacketAsync(
                                    frame,
                                    session,
                                    remoteIp,
                                    cancellationToken)
                                .ConfigureAwait(false);
                        }
                        catch (JsonException)
                        {
                            _metrics.ProtocolError();
                            session.Close(
                                SessionCloseReason.ProtocolViolation);
                            return;
                        }
                    }
                    else if (lane == CommandLane.Ephemeral)
                    {
                        // 复制出 Pipe 前预留全局入站预算（所有权从 Pipe 转到 lane 缓冲）。
                        if (!_globalInboundBudget.TryReserve(payloadLength))
                        {
                            session.Close(SessionCloseReason.InboundBudgetExceeded);
                            return;
                        }

                        // Ephemeral 命令（Typing）使用普通分配 + DropOldest。
                        var buffer2 = payloadLength > 0
                            ? new byte[payloadLength]
                            : Array.Empty<byte>();

                        if (payloadLength > 0)
                            frame.Payload.CopyTo(buffer2);

                        var command = new SessionCommand
                        {
                            Command = frame.Command,
                            RentedBuffer = buffer2,
                            PayloadLength = payloadLength,
                            IsPooled = false,
                            ReservedInboundBytes = payloadLength,
                            InboundBudget = _globalInboundBudget
                        };

                        // TryEnqueueEphemeral 非阻塞：返回 false 仅在调度器已关闭时。
                        if (!scheduler.TryEnqueueEphemeral(command))
                        {
                            _globalInboundBudget.Release(payloadLength);
                            return;
                        }
                    }
                    else
                    {
                        if (!_globalInboundBudget.TryReserve(payloadLength))
                        {
                            session.Close(SessionCloseReason.InboundBudgetExceeded);
                            return;
                        }

                        // 复制 payload 到 ArrayPool 租用缓冲区，立即释放 Pipe。
                        var rented = payloadLength > 0
                            ? ArrayPool<byte>.Shared.Rent(payloadLength)
                            : Array.Empty<byte>();

                        if (payloadLength > 0)
                            frame.Payload.CopyTo(rented);

                        var command = new SessionCommand
                        {
                            Command = frame.Command,
                            RentedBuffer = rented,
                            PayloadLength = payloadLength,
                            IsPooled = true,
                            ReservedInboundBytes = payloadLength,
                            InboundBudget = _globalInboundBudget
                        };

                        try
                        {
                            if (lane == CommandLane.Query)
                            {
                                await scheduler.EnqueueQueryAsync(
                                        command, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                            else
                            {
                                await scheduler.EnqueueOrderedAsync(
                                        command, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            // 会话关闭中，归还缓冲区与入站预算并退出。
                            if (rented.Length > 0)
                                ArrayPool<byte>.Shared.Return(rented);
                            _globalInboundBudget.Release(payloadLength);
                            throw;
                        }
                        catch (ChannelClosedException)
                        {
                            // 调度器已关闭，归还缓冲区与入站预算并退出。
                            if (rented.Length > 0)
                                ArrayPool<byte>.Shared.Return(rented);
                            _globalInboundBudget.Release(payloadLength);
                            return;
                        }
                    }

                    // 标记此帧已消费（Pipe 可回收对应内存）。
                    consumed = buffer.Start;
                }

                var consumedBytes = (int)readBuffer
                    .Slice(readBuffer.Start, consumed)
                    .Length;
                pipeLease.Release(consumedBytes);
                reader.AdvanceTo(consumed, buffer.End);

                if (result.IsCanceled)
                {
                    break;
                }

                if (result.IsCompleted)
                {
                    if (!buffer.IsEmpty)
                    {
                        _metrics.ProtocolError();
                    }

                    break;
                }
            }
        }
        catch (Exception exception)
        {
            completionError = exception;
            throw;
        }
        finally
        {
            await reader.CompleteAsync(completionError)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 调度器消费者回调。从租用缓冲区构造 PacketFrame，
    /// 调用 <see cref="_processPacketAsync"/> 回调，并捕获异常关闭会话。
    /// </summary>
    private async ValueTask ProcessScheduledCommandAsync(
        SessionCommand command,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        try
        {
            var frame = new PacketFrame(
                command.Command,
                command.AsPayloadSequence());
            await _processPacketAsync(
                    frame,
                    session,
                    remoteIp,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (JsonException)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
        }
        catch (SocketException)
        {
            session.Close(SessionCloseReason.TransportError);
        }
        catch (Exception ex)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                ex);
            session.Close(SessionCloseReason.TransportError);
        }
    }
}
