using System.Buffers;
using System.Net.Sockets;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Core.Server;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Transport;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 会话控制命令处理器：处理连接状态机命令 <see cref="PacketCommand.AuthenticationRequest"/>
/// 与 <see cref="PacketCommand.ClientHello"/>。
/// <para>
/// 从 <see cref="Networking.TcpGatewayService"/> 抽取以消除 God Service 中散落的握手与鉴权逻辑。
/// 与 <see cref="SessionRuntime"/> / <see cref="HeartbeatCoordinator"/> 抽取模式一致：单例，
/// 由 <see cref="Networking.TcpGatewayService"/> 内部构造并传入已注入依赖。
/// </para>
/// <para>
/// 为什么不走 <see cref="Dispatching.CommandDispatcher"/>：本处理器依赖
/// <see cref="TcpListenerHost"/>（连接准入回调）与 <see cref="SessionLifecycleCoordinator"/>
/// （内部创建的会话生命周期协调器），二者均为 <see cref="Networking.TcpGatewayService"/>
/// 内部字段而非 DI 单例，无法通过 <c>AddSingleton</c> 注入到 dispatcher 的 handler 图中。
/// Auth/ClientHello 属连接状态机命令，与业务命令的依赖图不同，保留直接调用更清晰。
/// </para>
/// <para>
/// 测试场景下（codec/identity 未注入）<see cref="HandleClientHelloAsync"/> 静默跳过握手，
/// 回退到旧 v1 行为，与抽取前一致。
/// </para>
/// </summary>
internal sealed class SessionControlHandler
{
    private readonly TcpGatewayOptions _options;
    private readonly IRealtimeAuthenticator _authenticator;
    private readonly IPayloadCodec<AuthenticationRequest> _authenticationRequestCodec;
    private readonly IPayloadCodec<AuthenticationResponse> _authenticationResponseCodec;
    private readonly IPayloadCodec<ClientHello>? _clientHelloCodec;
    private readonly IPayloadCodec<ServerHello>? _serverHelloCodec;
    private readonly IPayloadCodec<ResumeResponse>? _resumeResponseCodec;
    private readonly IPayloadCodec<ProtocolErrorFrame>? _protocolErrorFrameCodec;
    private readonly IServerIdentity? _serverIdentity;
    private readonly TcpListenerHost _listenerHost;
    private readonly SessionLifecycleCoordinator _lifecycleCoordinator;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;

    public SessionControlHandler(
        TcpGatewayOptions options,
        IRealtimeAuthenticator authenticator,
        IPayloadCodec<AuthenticationRequest> authenticationRequestCodec,
        IPayloadCodec<AuthenticationResponse> authenticationResponseCodec,
        IPayloadCodec<ClientHello>? clientHelloCodec,
        IPayloadCodec<ServerHello>? serverHelloCodec,
        IPayloadCodec<ResumeResponse>? resumeResponseCodec,
        IPayloadCodec<ProtocolErrorFrame>? protocolErrorFrameCodec,
        IServerIdentity? serverIdentity,
        TcpListenerHost listenerHost,
        SessionLifecycleCoordinator lifecycleCoordinator,
        GatewayMetrics metrics,
        ILogger logger)
    {
        _options = options;
        _authenticator = authenticator;
        _authenticationRequestCodec = authenticationRequestCodec;
        _authenticationResponseCodec = authenticationResponseCodec;
        _clientHelloCodec = clientHelloCodec;
        _serverHelloCodec = serverHelloCodec;
        _resumeResponseCodec = resumeResponseCodec;
        _protocolErrorFrameCodec = protocolErrorFrameCodec;
        _serverIdentity = serverIdentity;
        _listenerHost = listenerHost;
        _lifecycleCoordinator = lifecycleCoordinator;
        _metrics = metrics;
        _logger = logger;
    }

    /// <summary>
    /// 分发连接状态机命令。仅处理 AuthenticationRequest 与 ClientHello，
    /// 其他命令返回 false 由调用方继续处理。
    /// </summary>
    public async ValueTask<bool> TryHandleAsync(
        PacketCommand command,
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        switch (command)
        {
            case PacketCommand.AuthenticationRequest:
                await HandleAuthenticationAsync(
                        payload, session, remoteIp, cancellationToken)
                    .ConfigureAwait(false);
                return true;

            case PacketCommand.ClientHello:
                await HandleClientHelloAsync(
                        payload, session, remoteIp, cancellationToken)
                    .ConfigureAwait(false);
                return true;

            default:
                return false;
        }
    }

    private async ValueTask HandleAuthenticationAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        if (session.IsAuthenticated)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        // 连接状态机：RequireClientHello 时必须先完成握手（含 Resume 路径），再接受认证。
        if (_options.RequireClientHello && !session.HasCompletedHandshake)
        {
            _metrics.ProtocolError();
            SendProtocolError(
                session,
                ProtocolErrorCode.ProtocolViolation,
                "ClientHello required before authentication",
                fatal: true,
                originCommand: (ushort)PacketCommand.AuthenticationRequest);
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var request = _authenticationRequestCodec.Deserialize(payload);
        if (request is null ||
            string.IsNullOrWhiteSpace(request.AccessToken))
        {
            _listenerHost.RecordAuthenticationFailure(remoteIp);
            SendAuthenticationFailure(
                session,
                "AccessToken 为空",
                AuthenticationFailureKind.InvalidCredentials);
            return;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken);
        timeout.CancelAfter(_options.AuthenticationTimeout);

        RealtimeAuthenticationResult result;
        try
        {
            result = await _authenticator
                .AuthenticateAsync(
                    request.AccessToken,
                    request.DeviceIdHash,
                    timeout.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (!cancellationToken.IsCancellationRequested)
        {
            session.Close(SessionCloseReason.AuthenticationTimedOut);
            return;
        }

        if (!result.Succeeded)
        {
            _listenerHost.RecordAuthenticationFailure(remoteIp);
            SendAuthenticationFailure(
                session,
                result.ErrorMessage ?? "Token 无效或已过期",
                result.FailureKind);
            return;
        }

        // 认证成功，递减未认证计数，释放槽位给新连接。
        _listenerHost.MarkAuthenticated();
        _metrics.UnauthenticatedConnectionClosed();

        // Session 生命周期（注册、Presence 上线、同设备替换、ResumeToken 颁发）委托协调器。
        var resumeToken = await _lifecycleCoordinator.OnAuthenticatedAsync(
                session, result, cancellationToken)
            .ConfigureAwait(false);

        var response = new AuthenticationResponse
        {
            Success = true,
            UserId = result.UserId,
            SessionId = session.SessionId,
            DeviceIdHash = result.DeviceIdHash,
            DeviceId = result.DeviceId,
            ResumeToken = resumeToken
        };

        using var responseFrame = OutboundFrameFactory.Create(
            PacketCommand.AuthenticationResponse,
            _authenticationResponseCodec,
            response);
        session.TryQueue(responseFrame);
    }

    private async ValueTask HandleClientHelloAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 重复 ClientHello 视为协议违例：已认证或已完成握手的会话不应再次发起握手。
        if (session.IsAuthenticated || session.HasCompletedHandshake)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        // 依赖未注入（测试场景）时静默跳过握手，回退到旧 v1 行为。
        if (_clientHelloCodec is null || _serverHelloCodec is null || _serverIdentity is null)
        {
            return;
        }

        var hello = _clientHelloCodec.Deserialize(payload);
        if (hello is null)
        {
            SendProtocolError(session, ProtocolErrorCode.InvalidPayload, "invalid ClientHello");
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        // 协议版本协商：客户端版本须在 [MinProtocolVersion, CurrentProtocolVersion] 区间内。
        // 高于 CurrentProtocolVersion：服务端不支持，致命错误。
        // 低于 MinProtocolVersion：客户端过旧，需升级，致命错误。
        if (hello.ProtocolVersion > PacketProtocol.CurrentProtocolVersion)
        {
            SendProtocolError(
                session,
                ProtocolErrorCode.UnsupportedVersion,
                $"unsupported protocol version {hello.ProtocolVersion}",
                fatal: true);
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        if (hello.ProtocolVersion < PacketProtocol.MinProtocolVersion)
        {
            SendProtocolError(
                session,
                ProtocolErrorCode.UnsupportedVersion,
                $"protocol version {hello.ProtocolVersion} below minimum supported {PacketProtocol.MinProtocolVersion}",
                fatal: true);
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        // 断线重连：客户端携带 ResumeToken 时尝试恢复。
        if (_options.EnableResume && !string.IsNullOrWhiteSpace(hello.ResumeToken))
        {
            // _resumeResponseCodec 未注入（测试场景）时跳过 Resume 尝试，回退到完整认证。
            ResumeLifecycleResult? resumeResult = null;
            if (_resumeResponseCodec is not null)
            {
                resumeResult = await _lifecycleCoordinator
                    .TryResumeAsync(hello.ResumeToken!, session, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (resumeResult is not null)
            {
                // 恢复成功：admission 跟踪 + 发送 ResumeResponse。
                _listenerHost.MarkAuthenticated();
                _metrics.UnauthenticatedConnectionClosed();

                var resumeResponse = new ResumeResponse
                {
                    Success = true,
                    ResumeToken = resumeResult.ResumeToken,
                    UserId = resumeResult.UserId,
                    SessionId = resumeResult.SessionId,
                    DeviceId = resumeResult.DeviceId,
                    // 来自 SyncBootstrap 查询的 ServerTimeMs；查询失败或超时为 null，
                    // 客户端应回退到“始终 SyncBootstrap”策略。
                    LastConversationSequence = resumeResult.LastConversationSequence
                };
                using var resumeFrame = OutboundFrameFactory.Create(
                    PacketCommand.ResumeResponse,
                    _resumeResponseCodec!,
                    resumeResponse);
                session.TryQueue(resumeFrame);
                return; // 恢复成功，ResumeResponse 已发送。
            }

            // 恢复失败：记录准入失败用于限流统计，再发送 Error 帧，客户端应走完整认证流程。
            _listenerHost.RecordAuthenticationFailure(remoteIp);
            SendProtocolError(
                session,
                ProtocolErrorCode.ResumeFailed,
                "resume token invalid or expired");
            // 继续发送 ServerHello，客户端可选择重新认证。
        }

        // 发送 ServerHello 握手响应。
        // FeatureBits 协商：取客户端声明位与服务端支持位的交集（按位与）。
        // 当前服务端无强制能力位，协商结果 = 客户端声明位 ∩ 服务端支持位；
        // 未来引入命令级能力协商时，解析器将根据协商结果决定是否接受特定命令。
        var negotiatedFeatureBits = hello.FeatureBits & _serverIdentity.FeatureBits;
        var serverHello = new ServerHello
        {
            ProtocolVersion = PacketProtocol.CurrentProtocolVersion,
            FeatureBits = negotiatedFeatureBits,
            ServerDeviceId = _serverIdentity.ServerDeviceId,
            ServerTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            HeartbeatIntervalMs = (int)_options.IdleTimeout.TotalMilliseconds / 2,
            MaxPayloadBytes = _options.MaxInboundPayloadBytes,
            ResumeSupported = _options.EnableResume,
            PayloadFormat = ProtocolPayloadFormat.Json
        };

        using var helloFrame = OutboundFrameFactory.Create(
            PacketCommand.ServerHello,
            _serverHelloCodec,
            serverHello);
        session.TryQueue(helloFrame);
        session.MarkHandshakeCompleted();
    }

    private void SendAuthenticationFailure(
        TcpClientSession session,
        string message,
        AuthenticationFailureKind failureKind)
    {
        _metrics.AuthenticationFailed(failureKind);

        var response = new AuthenticationResponse
        {
            Success = false,
            ErrorMessage = message
        };

        using var responseFrame = OutboundFrameFactory.Create(
            PacketCommand.AuthenticationResponse,
            _authenticationResponseCodec,
            response);

        if (!session.TryQueue(
                responseFrame,
                SessionCloseReason.AuthenticationRejected))
        {
            session.Close(
                SessionCloseReason.AuthenticationRejected);
        }
    }

    /// <summary>
    /// 发送协议级 Error 帧（PacketCommand.Error = 500）。
    /// 依赖未注入（测试场景）时静默跳过，仅记录指标。
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
}
