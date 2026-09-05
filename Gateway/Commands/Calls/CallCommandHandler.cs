using System.Buffers;
using System.Text;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using ChatApp.TcpGateway.Gateway.Serialization;

namespace ChatApp.TcpGateway.Gateway.Commands.Calls;

/// <summary>
/// 通话信令控制面命令处理器（CallCommandRequest）。
/// <para>
/// 统一命令格式通过 <see cref="TcpCallCommandType"/> 区分命令类型。Gateway 仅做廉价结构校验
/// （command id / call id / SDP 预算 / 类型合法）与转发；授权（grant 校验）、状态机迁移、
/// 幂等与乱序判定由 Realtime 侧（<see cref="ICallBackend"/>）负责。
/// </para>
/// <para>
/// 成功且 Realtime 返回需转发的对端信号时，处理器把 <see cref="TcpCallSignal"/> push 到
/// <see cref="TcpCallSignal.ToUserId"/> 的在线会话（<see cref="PacketCommand.CallSignal"/>）。
/// 媒体/SDP 只走临时信令路径，不进入持久化存储。
/// </para>
/// <para>
/// <b>群组路径（GROUP-CALL-1，Mesh ≤4 人）</b>：携带群组 grant（<see cref="TcpCallKind.Group"/>）
/// 的命令不经 Realtime 1:1 状态机，由 <see cref="GroupCallSignalRelay"/> 无状态中继——校验
/// grant（HMAC 覆盖全部参与者）通过后按参与者名单扇出到其余在线成员（invite），成员离开
/// （End）扇出 <c>participant-left</c> 事件；房间状态由客户端按 revision/成员列表自洽，
/// Gateway 不做服务端房间状态。
/// </para>
/// <para>
/// 校验顺序、错误码与 metric 事件遵循 <see cref="RelationshipCommandHandler"/> 既有约定。
/// </para>
/// </summary>
internal sealed class CallCommandHandler : ICommandHandler
{
    private const int MaxRequestIdLength = 64;

    private readonly ICallBackend _backend;
    private readonly GroupCallSignalRelay _groupRelay;
    private readonly IPayloadCodec<TcpCallCommandRequest> _requestCodec;
    private readonly IPayloadCodec<TcpCallCommandResponse> _responseCodec;
    private readonly IPayloadCodec<TcpCallSignal> _signalCodec;
    private readonly UserSessionRegistry _userSessions;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<CallCommandHandler> _logger;

    public CallCommandHandler(
        ICallBackend backend,
        GroupCallSignalRelay groupRelay,
        IPayloadCodec<TcpCallCommandRequest> requestCodec,
        IPayloadCodec<TcpCallCommandResponse> responseCodec,
        IPayloadCodec<TcpCallSignal> signalCodec,
        UserSessionRegistry userSessions,
        GatewayMetrics metrics,
        ILogger<CallCommandHandler> logger)
    {
        _backend = backend;
        _groupRelay = groupRelay;
        _requestCodec = requestCodec;
        _responseCodec = responseCodec;
        _signalCodec = signalCodec;
        _userSessions = userSessions;
        _metrics = metrics;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.CallCommandRequest => HandleAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    /// <summary>处理一条通话信令命令。</summary>
    private async ValueTask HandleAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = SessionPayload.Deserialize(
            session,
            PacketCommand.CallCommandRequest,
            _requestCodec,
            payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;

        // 廉价结构校验：RequestId / CommandId / CallId 长度、命令类型合法性、SDP 预算。
        var commandId = (request.CommandId ?? string.Empty).Trim();
        var callId = (request.CallId ?? string.Empty).Trim();
        var invalid =
            requestId.Length > MaxRequestIdLength
            || commandId.Length == 0
            || commandId.Length > TcpCallConstants.MaxCommandIdBytes
            || callId.Length == 0
            || callId.Length > TcpCallConstants.MaxCallIdBytes
            || !Enum.IsDefined(request.Type)
            || request.Type == 0
            || (request.Sdp is { } sdp
                && Encoding.UTF8.GetByteCount(sdp) > TcpCallConstants.MaxSdpBytes);

        if (invalid)
        {
            SendResponse(
                session,
                new TcpCallCommandResponse
                {
                    RequestId = requestId.Length <= MaxRequestIdLength ? requestId : string.Empty,
                    CallId = callId.Length <= TcpCallConstants.MaxCallIdBytes ? callId : string.Empty,
                    Succeeded = false,
                    ErrorCode = TcpCallErrorCode.BadRequest,
                    ErrorMessage = "通话命令请求参数无效。"
                });
            return;
        }

        try
        {
            // 群组路径（GROUP-CALL-1）：群组 grant 不进入 Realtime 1:1 状态机，无状态中继扇出。
            if (request.Grant is not null && GroupCallSignalRelay.IsGroupGrant(request.Grant))
            {
                await HandleGroupAsync(request, requestId, callId, session, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            var result = await _backend
                .SendCommandAsync(
                    requestId,
                    session.UserId,
                    session.SessionId ?? $"tcp-{session.ConnectionId}",
                    commandId,
                    callId,
                    request.Type,
                    request.Revision,
                    request.Grant,
                    request.Sdp,
                    request.ClientOccurredAtMs,
                    cancellationToken)
                .ConfigureAwait(false);

            SendResponse(
                session,
                new TcpCallCommandResponse
                {
                    RequestId = result.RequestId,
                    CallId = result.CallId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    State = result.State,
                    EndReason = result.EndReason,
                    Revision = result.Revision,
                    Replayed = result.Replayed,
                    SignalToForward = result.SignalToForward
                });

            if (result.Succeeded && result.SignalToForward is { } signal)
                PushSignal(signal);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.CallCommandRequest);
            _logger.CommandFailed(
                PacketCommand.CallCommandRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendResponse(
                session,
                new TcpCallCommandResponse
                {
                    RequestId = requestId,
                    CallId = callId,
                    Succeeded = false,
                    ErrorCode = TcpCallErrorCode.StateStoreUnavailable,
                    ErrorMessage = "通话服务暂时不可用。"
                });
        }
    }

    /// <summary>
    /// 群组信令无状态中继：grant 校验通过后按参与者名单扇出到其余在线成员（排除发起者）。
    /// </summary>
    private ValueTask HandleGroupAsync(
        TcpCallCommandRequest request,
        string requestId,
        string callId,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var verdict = _groupRelay.Evaluate(requestId, request, request.Grant!, session.UserId);
        if (!verdict.Succeeded)
        {
            SendResponse(
                session,
                new TcpCallCommandResponse
                {
                    RequestId = requestId,
                    CallId = callId,
                    Succeeded = false,
                    ErrorCode = verdict.ErrorCode,
                    ErrorMessage = verdict.ErrorMessage
                });
            return ValueTask.CompletedTask;
        }

        foreach (var signal in verdict.Signals)
        {
            PushSignal(signal);
        }

        SendResponse(
            session,
            new TcpCallCommandResponse
            {
                RequestId = requestId,
                CallId = callId,
                Succeeded = true,
                State = verdict.RelayState,
                EndReason = verdict.RelayEndReason,
                Revision = request.Revision
            });
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 把对端需收到的信令 push 到目标用户的本机会话快照（临时信令路径，不持久化）。
    /// </summary>
    private void PushSignal(TcpCallSignal signal)
    {
        var targets = _userSessions.GetSnapshot(signal.ToUserId);
        if (targets.Length == 0)
            return;

        using var frames = new FormatGroupedFrame<TcpCallSignal>(
            PacketCommand.CallSignal,
            _signalCodec,
            signal);
        var queued = 0;
        foreach (var target in targets)
        {
            if (target.TryQueue(frames.GetFrame(target)))
                queued++;
        }

        _metrics.RealtimeEventHandled(queued);
    }

    private void SendResponse(
        TcpClientSession session,
        TcpCallCommandResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.CallCommandResponse,
            _responseCodec,
            session,
            response);
        session.TryQueue(frame);
    }
}