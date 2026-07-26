using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using System.Buffers;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Push;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Commands.Push;

/// <summary>
/// 离线推送令牌相关命令处理器（RegisterPushTokenRequest / UnregisterPushTokenRequest）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec 与 <see cref="IPushTokenStore"/> 端口，
/// 不再依赖 service 的私有字段。Push codec 统一通过 DI 注入为 <see cref="IPayloadCodec{T}"/>，
/// 消除原 service 构造函数内 <c>new JsonPayloadCodec&lt;T&gt;</c> 的不一致。
/// </para>
/// <para>
/// 行为与原内联 handler 完全等价（包括校验顺序、错误码、metric 与日志事件）。
/// </para>
/// </summary>
internal sealed class PushTokenCommandHandler : ICommandHandler
{
    private readonly IPushTokenStore? _pushTokenStore;
    private readonly IPayloadCodec<RegisterPushTokenRequest> _registerRequestCodec;
    private readonly IPayloadCodec<RegisterPushTokenResponse> _registerResponseCodec;
    private readonly IPayloadCodec<UnregisterPushTokenRequest> _unregisterRequestCodec;
    private readonly IPayloadCodec<UnregisterPushTokenResponse> _unregisterResponseCodec;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<PushTokenCommandHandler> _logger;

    public PushTokenCommandHandler(
        IPayloadCodec<RegisterPushTokenRequest> registerRequestCodec,
        IPayloadCodec<RegisterPushTokenResponse> registerResponseCodec,
        IPayloadCodec<UnregisterPushTokenRequest> unregisterRequestCodec,
        IPayloadCodec<UnregisterPushTokenResponse> unregisterResponseCodec,
        GatewayMetrics metrics,
        ILogger<PushTokenCommandHandler> logger,
        IPushTokenStore? pushTokenStore = null)
    {
        _registerRequestCodec = registerRequestCodec;
        _registerResponseCodec = registerResponseCodec;
        _unregisterRequestCodec = unregisterRequestCodec;
        _unregisterResponseCodec = unregisterResponseCodec;
        _metrics = metrics;
        _logger = logger;
        _pushTokenStore = pushTokenStore;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        ICommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.RegisterPushTokenRequest => HandleRegisterAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.UnregisterPushTokenRequest => HandleUnregisterAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    /// <summary>
    /// 注册设备推送令牌。按 (userId, deviceIdHash) 幂等覆盖；超出每用户上限时按最旧淘汰。
    /// deviceIdHash 取自认证会话，忽略客户端传入；token 字符串长度上限由 <see cref="PushTokenLimits"/> 限制。
    /// </summary>
    private async ValueTask HandleRegisterAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (_pushTokenStore is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var request = _registerRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > PushTokenLimits.MaxRequestIdLength
            || !Enum.IsDefined(request.Platform)
            || request.Platform == 0
            || string.IsNullOrWhiteSpace(request.Token)
            || request.Token.Length > PushTokenLimits.MaxTokenLength
            || (request.AppDeviceLabel is { Length: > PushTokenLimits.MaxAppDeviceLabelLength })
            || session.DeviceIdHash is null or 0)
        {
            SendRegisterResponse(
                session,
                new RegisterPushTokenResponse
                {
                    RequestId = requestId.Length <= PushTokenLimits.MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_push_token_request",
                    ErrorMessage = "推送令牌注册请求参数无效。"
                });
            return;
        }

        try
        {
            var activeCount = await _pushTokenStore
                .RegisterAsync(
                    session.UserId,
                    session.DeviceIdHash!.Value,
                    request.Platform,
                    request.Token,
                    string.IsNullOrWhiteSpace(request.AppDeviceLabel)
                        ? null
                        : request.AppDeviceLabel,
                    cancellationToken)
                .ConfigureAwait(false);

            SendRegisterResponse(
                session,
                new RegisterPushTokenResponse
                {
                    RequestId = requestId,
                    Succeeded = true,
                    ActiveTokenCount = activeCount
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.RegisterPushTokenRequest);
            _logger.CommandFailed(
                PacketCommand.RegisterPushTokenRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendRegisterResponse(
                session,
                new RegisterPushTokenResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "push_token_store_unavailable",
                    ErrorMessage = "推送令牌存储暂不可用。"
                });
        }
    }

    /// <summary>
    /// 注销推送令牌。未传 Token 时按当前连接 deviceIdHash 注销该设备全部令牌；
    /// 传 Token 时按字符串精确注销（可跨设备，适合平台令牌失效场景）。
    /// </summary>
    private async ValueTask HandleUnregisterAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (_pushTokenStore is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var request = _unregisterRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;
        if (requestId.Length > PushTokenLimits.MaxRequestIdLength
            || (request.Token is { Length: > PushTokenLimits.MaxTokenLength })
            || (string.IsNullOrWhiteSpace(request.Token) && session.DeviceIdHash is null or 0))
        {
            SendUnregisterResponse(
                session,
                new UnregisterPushTokenResponse
                {
                    RequestId = requestId.Length <= PushTokenLimits.MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_push_token_request",
                    ErrorMessage = "推送令牌注销请求参数无效。"
                });
            return;
        }

        try
        {
            int activeCount;
            if (!string.IsNullOrWhiteSpace(request.Token))
            {
                activeCount = await _pushTokenStore
                    .UnregisterByTokenAsync(
                        session.UserId,
                        request.Token,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                activeCount = await _pushTokenStore
                    .UnregisterByDeviceAsync(
                        session.UserId,
                        session.DeviceIdHash!.Value,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            SendUnregisterResponse(
                session,
                new UnregisterPushTokenResponse
                {
                    RequestId = requestId,
                    Succeeded = true,
                    ActiveTokenCount = activeCount
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.UnregisterPushTokenRequest);
            _logger.CommandFailed(
                PacketCommand.UnregisterPushTokenRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendUnregisterResponse(
                session,
                new UnregisterPushTokenResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "push_token_store_unavailable",
                    ErrorMessage = "推送令牌存储暂不可用。"
                });
        }
    }

    private void SendRegisterResponse(
        TcpClientSession session,
        RegisterPushTokenResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.RegisterPushTokenResponse,
            _registerResponseCodec,
            response);
        session.TryQueue(frame);
    }

    private void SendUnregisterResponse(
        TcpClientSession session,
        UnregisterPushTokenResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.UnregisterPushTokenResponse,
            _unregisterResponseCodec,
            response);
        session.TryQueue(frame);
    }
}
