using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text.Json;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 每连接数据面运行时：封装 session 生命周期内 Pipe reader/writer +
/// 全局 <see cref="SessionCommandExecutor"/>（OrderedWrite/Query）调度，
/// 从 <see cref="Networking.TcpGatewayService"/> 抽取以消除 God Service 中散落的 per-connection 数据路径。
/// <para>
/// 职责边界：
/// <list type="bullet">
/// <item>构造每会话 CTS（链接 session lifetime + host stopping）；</item>
/// <item>注册连接到全局 <see cref="SessionCommandExecutor"/>（OrderedWrite/Query 各一份）；</item>
/// <item>构造 Pipe + <see cref="SessionInboundPipeLease"/>；</item>
/// <item>驱动 FillPipeAsync + ReadPipeAsync 并等待双方退出；</item>
/// <item>Inline 与 Ephemeral 命令在读循环内同步处理（不创建每连接 Consumer Task）；</item>
/// <item>数据面清理：pipeLease 归还、session.Close、executor 注销、session.DisposeAsync。</item>
/// </list>
/// 协议级内联命令处理与早投拒绝（SendProtocolError / RejectOversizedPayload / ProcessPacketAsync）
/// 通过构造函数注入的委托回调到 <see cref="Networking.TcpGatewayService"/>，
/// 避免将大量协议 codec/依赖注入本类型。
/// 服务级清理（Presence 下线、admission 释放、session 注册表移除）由调用方在 RunAsync 返回后处理。
/// </para>
/// <para>
/// 单例注册：所有连接共享同一实例，通过 <see cref="RunAsync"/> 的 session 参数区分连接。
/// </para>
/// <para>
/// V2 重构：消除每连接 OrderedWrite/Query/Ephemeral 三个 Channel + 三个 Consumer Task。
/// OrderedWrite/Query 转发到全局 <see cref="SessionCommandExecutor"/>（共享 worker 池）。
/// Ephemeral 命令（TypingNotify）在读循环内同步处理，直接走 ProcessPacketAsync →
/// CommandDispatcher → TypingCommandHandler → TypingFanoutCoordinator.TryAccept（非阻塞）。
/// </para>
/// </summary>
internal sealed class SessionRuntime
{
    private readonly TcpGatewayOptions _options;
    private readonly PipeOptions _pipeOptions;
    private readonly GlobalInboundBudget _globalInboundBudget;
    private readonly SessionCommandExecutor _orderedWriteExecutor;
    private readonly SessionCommandExecutor _queryExecutor;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;

    private readonly Func<PacketFrame, TcpClientSession, string, CancellationToken, ValueTask> _processPacketAsync;
    private readonly Action<TcpClientSession, ProtocolErrorCode, string?, bool, int?, ushort?> _sendProtocolError;
    private readonly Action<TcpClientSession, PacketCommand> _rejectOversizedPayload;

    public SessionRuntime(
        TcpGatewayOptions options,
        PipeOptions pipeOptions,
        GlobalInboundBudget globalInboundBudget,
        SessionCommandExecutor orderedWriteExecutor,
        SessionCommandExecutor queryExecutor,
        GatewayMetrics metrics,
        ILogger logger,
        Func<PacketFrame, TcpClientSession, string, CancellationToken, ValueTask> processPacketAsync,
        Action<TcpClientSession, ProtocolErrorCode, string?, bool, int?, ushort?> sendProtocolError,
        Action<TcpClientSession, PacketCommand> rejectOversizedPayload)
    {
        _options = options;
        _pipeOptions = pipeOptions;
        _globalInboundBudget = globalInboundBudget;
        _orderedWriteExecutor = orderedWriteExecutor;
        _queryExecutor = queryExecutor;
        _metrics = metrics;
        _logger = logger;
        _processPacketAsync = processPacketAsync;
        _sendProtocolError = sendProtocolError;
        _rejectOversizedPayload = rejectOversizedPayload;
    }

    /// <summary>
    /// 驱动单连接数据面：注册到全局 executor + 构建 Pipe，启动 fill/read 双任务并等待退出。
    /// 返回时已完成数据面清理（pipeLease/executor 注销/session）；服务级清理由调用方处理。
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

        // 连接注册到全局执行器由 TcpGatewayService.OnConnectionAccepted 完成
        // （在 session 暴露到 _sessions 字典前注册，避免命令到达时未注册）。
        // 注销由 HandleClientAsync finally 块在 SessionRuntime 返回后统一处理。

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

            // 执行器注销由 TcpGatewayService.HandleClientAsync finally 统一处理，
            // 避免此处与 service 级清理的顺序耦合。

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

                    // 弃用命令检查：标记为 Deprecated 的命令仍登记在 catalog 中以保持向后兼容，
                    // 但服务端不执行任何业务逻辑，直接返回 UnsupportedCommand 错误帧引导客户端迁移。
                    // 非致命：连接保持，客户端可继续发送其他命令。
                    if (PacketProtocol.IsDeprecated(frame.Command))
                    {
                        _metrics.ProtocolError();
                        _sendProtocolError(
                            session,
                            ProtocolErrorCode.UnsupportedCommand,
                            $"command {frame.Command} is deprecated",
                            false,
                            null,
                            (ushort)frame.Command);
                        consumed = buffer.Start;
                        continue;
                    }

                    // 按 lane 分类调度。委托 CommandCatalog（单一事实源）。
                    var lane = CommandCatalog.GetLane(frame.Command);

                    if (lane == CommandLane.Inline || lane == CommandLane.Ephemeral)
                    {
                        // Control 命令（ClientHello/Auth/Heartbeat/PresenceUnwatch）内联处理。
                        // Ephemeral 命令（TypingNotify）也内联处理：走 ProcessPacketAsync →
                        // CommandDispatcher → TypingCommandHandler → TypingFanoutCoordinator.TryAccept（非阻塞）。
                        // V2：消除每连接 Ephemeral Channel + Consumer Task。
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
                            InboundBudget = _globalInboundBudget,
                            // V2：全局执行器回调通过 Session/RemoteIp 恢复 per-connection 上下文。
                            Session = session,
                            RemoteIp = remoteIp
                        };

                        var executor = lane == CommandLane.Query
                            ? _queryExecutor
                            : _orderedWriteExecutor;

                        if (!executor.TryEnqueue(session.ConnectionId, command))
                        {
                            // 执行器未注册连接或队列满：归还缓冲区与入站预算。
                            if (rented.Length > 0)
                                ArrayPool<byte>.Shared.Return(rented);
                            _globalInboundBudget.Release(payloadLength);
                            // 队列满视为背压：关闭连接（与旧 Channel FullMode=Wait 行为不同，
                            // 但旧行为的 Wait 也会因 lifetime 取消最终抛出 ChannelClosedException）。
                            session.Close(SessionCloseReason.OutboundQueueFull);
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
}
