using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using RealtimeCallCommand = ChatApp.Realtime.Abstractions.Calls.CallCommand;
using RealtimeCallGrant = ChatApp.Realtime.Abstractions.Calls.CallGrant;
using RealtimeCallSignalEnvelope =
    ChatApp.Realtime.Abstractions.Calls.CallSignalEnvelope;

namespace ChatApp.TcpGateway.Gateway.Commands.Calls;

/// <summary>
/// 通话信令后端端口：把 TCP wire 命令映射为 Realtime 侧 <see cref="RealtimeCallCommand"/>
/// 并转发，返回状态机处理结果（终态/错误码/需转发的对端信号）。
/// <para>
/// Realtime 侧持有 <c>CallStateMachine</c>、临时状态存储、grant 校验与 signal 预算；
/// Gateway 作为 producer 只做廉价结构校验与映射，不重复实现状态机或授权判定。
/// </para>
/// </summary>
internal interface ICallBackend
{
    /// <summary>
    /// 执行一条通话信令命令（invite/ringing/accept/reject/cancel/end/reconnect）。
    /// </summary>
    Task<CallCommandBackendResult> SendCommandAsync(
        string requestId,
        long actorUserId,
        string actorSessionId,
        string commandId,
        string callId,
        TcpCallCommandType type,
        long revision,
        TcpCallGrant? grant,
        string? sdp,
        long clientOccurredAtMs,
        CancellationToken cancellationToken = default);
}

/// <summary>通话信令命令后端结果。</summary>
internal sealed record CallCommandBackendResult(
    string RequestId,
    string CallId,
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    TcpCallState State,
    TcpCallEndReason EndReason,
    long Revision,
    bool Replayed,
    TcpCallSignal? SignalToForward)
{
    public static CallCommandBackendResult Failed(
        string requestId,
        string callId,
        string errorCode,
        string errorMessage) =>
        new(requestId, callId, false, errorCode, errorMessage, default, default, 0, false, null);
}

/// <summary>
/// 生产适配实现：经 <see cref="IRealtimeMessageBus.SendCallCommandAsync"/> 将通话命令转发到
/// RealtimeServices，由其校验 grant 并驱动状态机。
/// <para>
/// 总线异常（NATS 超时等）不做吞咽，直接向 <see cref="CallCommandHandler"/> 抛出，由其 catch-all
/// 统一映射为 <c>call_state_store_unavailable</c> 响应。这与其它 Realtime 命令处理器的异常约定一致。
/// </para>
/// <para>
/// <c>TcpCallCommandType</c> / <c>TcpCallState</c> / <c>TcpCallEndReason</c> 与 Realtime
/// <c>CallCommandType</c> / <c>CallState</c> / <c>CallEndReason</c> 数值一致，直接数值映射；
/// 错误码经 <see cref="CallErrorCodeExtensions.ToStableCode"/> 转为稳定 wire 字符串。
/// </para>
/// </summary>
internal sealed class RealtimeCallBackend : ICallBackend
{
    private readonly IRealtimeMessageBus _messageBus;

    public RealtimeCallBackend(IRealtimeMessageBus messageBus)
    {
        _messageBus = messageBus;
    }

    public async Task<CallCommandBackendResult> SendCommandAsync(
        string requestId,
        long actorUserId,
        string actorSessionId,
        string commandId,
        string callId,
        TcpCallCommandType type,
        long revision,
        TcpCallGrant? grant,
        string? sdp,
        long clientOccurredAtMs,
        CancellationToken cancellationToken = default)
    {
        // grant 是 Realtime 状态机的必需授权输入；缺失时 fail-closed，不猜测。
        if (grant is null)
        {
            return CallCommandBackendResult.Failed(
                requestId,
                callId,
                TcpCallErrorCode.GrantInvalid,
                "call grant 缺失。");
        }

        var command = new RealtimeCallCommand
        {
            CommandId = commandId,
            CallId = callId,
            Type = (CallCommandType)type,
            ActorUserId = actorUserId,
            ActorSessionId = actorSessionId,
            Grant = MapGrant(grant),
            Revision = revision,
            Sdp = sdp,
            ClientOccurredAtMs = clientOccurredAtMs
        };

        var result = await _messageBus
            .SendCallCommandAsync(command, cancellationToken)
            .ConfigureAwait(false);

        return new CallCommandBackendResult(
            requestId,
            callId,
            result.Succeeded,
            result.ErrorCode?.ToStableCode(),
            result.ErrorMessage,
            (TcpCallState)result.State,
            (TcpCallEndReason)result.EndReason,
            result.Revision,
            result.Replayed,
            result.SignalToForward is { } signal ? MapSignal(signal) : null);
    }

    private static RealtimeCallGrant MapGrant(TcpCallGrant grant) => new()
    {
        CallId = grant.CallId,
        CallerUserId = grant.CallerUserId,
        CalleeUserId = grant.CalleeUserId,
        ExpiresAtMs = grant.ExpiresAtMs,
        Nonce = grant.Nonce,
        Signature = grant.Signature
    };

    private static TcpCallSignal MapSignal(RealtimeCallSignalEnvelope signal) => new()
    {
        SignalId = signal.SignalId,
        CallId = signal.CallId,
        FromUserId = signal.FromUserId,
        ToUserId = signal.ToUserId,
        Kind = (TcpCallCommandType)signal.Kind,
        Sdp = signal.Sdp,
        Revision = signal.Revision,
        OccurredAtMs = signal.OccurredAtMs
    };
}

/// <summary>
/// 占位实现：RealtimeServices 侧通话后端尚未接入。
/// 返回 <c>call_service_unavailable</c>，客户端可稍后重试。
/// </summary>
internal sealed class StubCallBackend : ICallBackend
{
    private readonly ILogger<StubCallBackend> _logger;

    public StubCallBackend(ILogger<StubCallBackend> logger)
    {
        _logger = logger;
    }

    public Task<CallCommandBackendResult> SendCommandAsync(
        string requestId,
        long actorUserId,
        string actorSessionId,
        string commandId,
        string callId,
        TcpCallCommandType type,
        long revision,
        TcpCallGrant? grant,
        string? sdp,
        long clientOccurredAtMs,
        CancellationToken cancellationToken = default)
    {
        _logger.CallBackendUnavailable(requestId, callId, actorUserId);
        return Task.FromResult(CallCommandBackendResult.Failed(
            requestId,
            callId,
            TcpCallErrorCode.StateStoreUnavailable,
            "通话服务暂未配置。"));
    }
}