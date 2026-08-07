using ChatApp.Shared.Protocol.Tcp;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Tracing;
using Microsoft.Extensions.Hosting;

namespace ChatApp.TcpGateway.Gateway.Networking;

/// <summary>
/// <see cref="TcpGatewayService"/> 的协议帧处理分部。
/// <para>
/// 包含数据面三入口：
/// <list type="bullet">
/// <item><see cref="ProcessPacketAsync"/>：SessionRuntime 委托回调，鉴权前置守卫 + 命令分派；</item>
/// <item><see cref="SendProtocolError"/>：SessionRuntime 委托回调 + ProcessPacketAsync 守卫使用；</item>
/// <item><see cref="RejectOversizedPayload"/>：SessionRuntime 委托回调，早投拒绝；</item>
/// <item><see cref="ProcessScheduledCommandAsync"/>：全局 SessionCommandExecutor worker 池回调。</item>
/// </list>
/// 与主文件分离以使主文件聚焦在 BackgroundService 生命周期与连接级编排
/// （StartAsync/ExecuteAsync/Dispose/OnConnectionAccepted/HandleClientAsync）。
/// </para>
/// </summary>
internal sealed partial class TcpGatewayService
{
    private async ValueTask ProcessPacketAsync(
        PacketFrame frame,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 鉴权前置：未认证会话只能处理 PreAuthentication/PreHandshake 命令。
        // 委托 CommandCatalog，避免字面量枚举比较遗漏新增握手命令。
        if (!session.IsAuthenticated &&
            !CommandCatalog.IsPreAuthentication(frame.Command))
        {
            _metrics.ProtocolError();
            SendProtocolError(session, ProtocolErrorCode.AuthRequired, "authentication required");
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        using var activity = GatewayTelemetry.StartCommand(frame.Command);

        // 已迁移到 CommandDispatcher 的命令（全部业务命令）优先走 handler 路径。
        // 测试路径下 dispatcher 为 null，落回下方逻辑。
        if (_commandDispatcher is { } dispatcher)
        {
            var context = new CommandContext(session, remoteIp);
            if (await dispatcher
                    .TryDispatchAsync(frame, context, cancellationToken)
                    .ConfigureAwait(false))
            {
                return;
            }
        }

        // 连接状态机命令（Auth/ClientHello）委托 SessionControlHandler。
        // 不走 dispatcher：依赖 _listenerHost 准入回调与 _lifecycleCoordinator（内部创建）。
        var wasAuthenticated = session.IsAuthenticated;
        if (await _sessionControlHandler
                .TryHandleAsync(
                    frame.Command,
                    frame.Payload,
                    session,
                    remoteIp,
                    cancellationToken)
                .ConfigureAwait(false))
        {
            // 认证转换钩子：未认证 → 已认证时注册用户桶（按 userId 分桶用于 presence 刷新）。
            // 覆盖 AuthenticationRequest 与 ResumeRequest 两条认证路径。
            if (!wasAuthenticated && session.IsAuthenticated && session.UserId > 0)
                _heartbeatBuckets.RegisterUser(session.UserId);
            return;
        }

        switch (frame.Command)
        {
            case PacketCommand.Heartbeat:
                // 使用静态 pinned Heartbeat ACK 帧，避免每次重复分配。
                // TryQueue 内部 TryRetain 增加 ref count，SendLoop 发送后 Dispose 减少 ref count。
                session.TryQueue(OutboundFrameFactory.GetHeartbeatAck());
                break;

            default:
                _metrics.ProtocolError();
                session.Close(SessionCloseReason.ProtocolViolation);
                break;
        }
    }

    /// <summary>
    /// 发送协议级 Error 帧（PacketCommand.Error = 500）。
    /// 依赖未注入（测试场景）时静默跳过，仅记录指标。
    /// 保留在本 service：SessionRuntime 通过委托回调使用，ProcessPacketAsync 鉴权前置守卫也使用。
    /// </summary>
    private void SendProtocolError(
        TcpClientSession session,
        ProtocolErrorCode code,
        string? message = null,
        bool fatal = false,
        int? retryAfterMs = null,
        ushort? originCommand = null)
    {
        // 测试场景下 _protocolErrorFrameCodec 可能为 null，跳过 Error 帧发送。
        if (_protocolErrorFrameCodec is null)
        {
            return;
        }

        var error = new ProtocolErrorFrame
        {
            Code = code,
            Fatal = fatal || code.IsFatal(),
            RetryAfterMs = retryAfterMs,
            Message = message,
            OriginCommand = originCommand
        };

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.Error,
            _protocolErrorFrameCodec,
            error);
        // Critical 等级：使用 TryQueue 保证发送（满时关闭连接）。
        session.TryQueue(frame, closeAfterSend: fatal ? SessionCloseReason.ProtocolViolation : null);
    }

    private void RejectOversizedPayload(
        TcpClientSession session,
        PacketCommand command)
    {
        if (command == PacketCommand.ChatMessage && session.IsAuthenticated)
        {
            // 内联构造 MessageAcknowledgement 并发送。SendMessageAcknowledgement 辅助方法
            // 已随 ChatMessage 命令迁移至 MessagingCommandHandler 删除，此处保留 _messageAcknowledgementCodec
            // 仅供本路径使用（早投拒绝，发生在 dispatcher 接管命令之前）。
            var acknowledgement = new MessageAcknowledgement
            {
                ClientMessageId = string.Empty,
                CommandId = string.Empty,
                Accepted = false,
                ErrorCode = InboundPayloadEarlyValidator.PayloadTooLargeCode,
                ErrorMessage = $"消息体超过上限 {_options.MaxInboundPayloadBytes} 字节。",
                AcknowledgedUtc = _timeProvider.GetUtcNow().UtcDateTime
            };

            using var outboundFrame = OutboundFrameFactory.Create(
                PacketCommand.MessageAcknowledgement,
                _messageAcknowledgementCodec,
                acknowledgement);
            if (!session.TryQueue(outboundFrame, SessionCloseReason.ProtocolViolation))
            {
                session.Close(SessionCloseReason.ProtocolViolation);
            }
            return;
        }

        session.Close(SessionCloseReason.ProtocolViolation);
    }

    /// <summary>
    /// 全局 <see cref="SessionCommandExecutor"/> 的命令处理回调。
    /// <para>
    /// 从 <see cref="SessionCommand"/> 恢复 per-connection 上下文（Session/RemoteIp），
    /// 构造 <see cref="PacketFrame"/> 后委托 <see cref="ProcessPacketAsync"/>。
    /// 资源释放（RentedBuffer 归还、InboundBudget 释放）由执行器在 finally 中统一完成。
    /// </para>
    /// <para>
    /// 使用 session.LifetimeToken 而非执行器 token：连接关闭时取消业务调用，
    /// 避免后端资源继续被占用。执行器 token 仅用于 worker 池停机。
    /// </para>
    /// <para>
    /// Opt-2：不再为每条命令创建 LinkedCTS(session.LifetimeToken, executorToken)。
    /// 停机流程保证先关闭所有 Session（取消 LifetimeToken），再停止 Executor，
    /// 因此 executorToken 对命令处理是冗余的——LifetimeToken 已覆盖连接关闭与宿主停机两个场景。
    /// 这消除了每条 Chat/Receipt/Edit/History 命令的 CTS 分配与 Token Registration 开销。
    /// </para>
    /// </summary>
    private async ValueTask ProcessScheduledCommandAsync(
        SessionCommand command,
        CancellationToken cancellationToken)
    {
        // 执行器可能在连接关闭后仍处理队列中残留命令：检查 session 状态。
        if (!command.Session.IsConnected)
            return;

        var frame = new PacketFrame(
            command.Command,
            command.AsPayloadSequence());

        // 直接使用 session.LifetimeToken：覆盖连接关闭与宿主停机（停机先 Close Session）。
        // cancellationToken（executor token）不再链接——它只在 executor 停止时取消，
        // 而那时 Session 已被 Close，LifetimeToken 已取消。
        await ProcessPacketAsync(
                frame,
                command.Session,
                command.RemoteIp,
                command.Session.LifetimeToken)
            .ConfigureAwait(false);
    }
}
