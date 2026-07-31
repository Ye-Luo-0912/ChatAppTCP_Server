using System.Buffers;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Commands.Relationships;

/// <summary>
/// 关系相关命令处理器（RelationshipCommandRequest / RelationshipListRequest）。
/// <para>
/// 主线四：统一命令格式通过 <see cref="RelationshipOperation"/> 区分操作类型。
/// Realtime 侧（<see cref="IRelationshipBackend"/>）负责权限校验与业务规则
/// （不可自加好友、不可重复拉黑等）。Gateway 仅做廉价结构校验与转发。
/// </para>
/// <para>
/// 校验顺序、错误码与 metric 事件遵循 <see cref="PushTokenCommandHandler"/> 既有约定。
/// </para>
/// </summary>
internal sealed class RelationshipCommandHandler : ICommandHandler
{
    private const int MaxRequestIdLength = 64;
    private const int MaxMessageLength = 512; // 好友请求附言上限
    private const int MaxRequestIdToRespondLength = 64;
    private const int MaxCursorLength = 256;
    private const int DefaultPageSize = 50;
    private const int MaxPageSize = 200;

    private readonly IRelationshipBackend _backend;
    private readonly IPayloadCodec<RelationshipCommandRequest> _commandRequestCodec;
    private readonly IPayloadCodec<RelationshipCommandResponse> _commandResponseCodec;
    private readonly IPayloadCodec<RelationshipListRequest> _listRequestCodec;
    private readonly IPayloadCodec<RelationshipListResponse> _listResponseCodec;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<RelationshipCommandHandler> _logger;

    public RelationshipCommandHandler(
        IRelationshipBackend backend,
        IPayloadCodec<RelationshipCommandRequest> commandRequestCodec,
        IPayloadCodec<RelationshipCommandResponse> commandResponseCodec,
        IPayloadCodec<RelationshipListRequest> listRequestCodec,
        IPayloadCodec<RelationshipListResponse> listResponseCodec,
        GatewayMetrics metrics,
        ILogger<RelationshipCommandHandler> logger)
    {
        _backend = backend;
        _commandRequestCodec = commandRequestCodec;
        _commandResponseCodec = commandResponseCodec;
        _listRequestCodec = listRequestCodec;
        _listResponseCodec = listResponseCodec;
        _metrics = metrics;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.RelationshipCommandRequest => HandleCommandAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.RelationshipListRequest => HandleListAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    /// <summary>
    /// 处理关系变更命令（发送/接受/拒绝好友请求、删除好友、拉黑/取消拉黑）。
    /// </summary>
    private async ValueTask HandleCommandAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _commandRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;

        // 廉价结构校验：RequestId / Message / RequestIdToRespond 长度、Operation 合法性。
        if (requestId.Length > MaxRequestIdLength
            || !Enum.IsDefined(request.Operation)
            || request.Operation == 0
            || (request.Message is { Length: > MaxMessageLength })
            || (request.RequestIdToRespond is { Length: > MaxRequestIdToRespondLength }))
        {
            SendCommandResponse(
                session,
                new RelationshipCommandResponse
                {
                    RequestId = requestId.Length <= MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_relationship_request",
                    ErrorMessage = "关系命令请求参数无效。"
                });
            return;
        }

        // 操作语义校验：
        // - SendFriendRequest / RemoveFriend / BlockUser / UnblockUser 需要 TargetUserId
        // - AcceptFriendRequest / DeclineFriendRequest 需要 RequestIdToRespond
        var op = request.Operation;
        var needsTarget = op is RelationshipOperation.SendFriendRequest
            or RelationshipOperation.RemoveFriend
            or RelationshipOperation.BlockUser
            or RelationshipOperation.UnblockUser;
        var needsRequestIdToRespond = op is RelationshipOperation.AcceptFriendRequest
            or RelationshipOperation.DeclineFriendRequest;

        if ((needsTarget && request.TargetUserId is null or <= 0)
            || (needsRequestIdToRespond && string.IsNullOrWhiteSpace(request.RequestIdToRespond)))
        {
            SendCommandResponse(
                session,
                new RelationshipCommandResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "invalid_relationship_request",
                    ErrorMessage = "关系命令请求参数无效。"
                });
            return;
        }

        try
        {
            var result = await _backend
                .MutateAsync(
                    requestId,
                    session.UserId,
                    request.Operation,
                    request.TargetUserId,
                    request.Message,
                    request.RequestIdToRespond,
                    session.SessionId,
                    cancellationToken)
                .ConfigureAwait(false);

            SendCommandResponse(
                session,
                new RelationshipCommandResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    Operation = result.Operation,
                    TargetUserId = result.TargetUserId,
                    ResourceId = result.ResourceId
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.RelationshipCommandRequest);
            _logger.CommandFailed(
                PacketCommand.RelationshipCommandRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendCommandResponse(
                session,
                new RelationshipCommandResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "relationship_service_unavailable",
                    ErrorMessage = "关系服务暂时不可用。"
                });
        }
    }

    /// <summary>
    /// 处理关系列表查询（好友 / 好友请求 / 黑名单），支持分页。
    /// </summary>
    private async ValueTask HandleListAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _listRequestCodec.Deserialize(payload);
        if (request is null)
        {
            _metrics.ProtocolError();
            session.Close(SessionCloseReason.ProtocolViolation);
            return;
        }

        var requestId = string.IsNullOrWhiteSpace(request.RequestId)
            ? Guid.CreateVersion7().ToString("N")
            : request.RequestId;

        // 廉价结构校验：RequestId / Cursor 长度、ListType 合法性。
        if (requestId.Length > MaxRequestIdLength
            || !Enum.IsDefined(request.ListType)
            || request.ListType == 0
            || (request.Cursor is { Length: > MaxCursorLength }))
        {
            SendListResponse(
                session,
                new RelationshipListResponse
                {
                    RequestId = requestId.Length <= MaxRequestIdLength
                        ? requestId
                        : string.Empty,
                    Succeeded = false,
                    ErrorCode = "invalid_relationship_request",
                    ErrorMessage = "关系列表查询请求参数无效。"
                });
            return;
        }

        try
        {
            var result = await _backend
                .QueryListAsync(
                    requestId,
                    session.UserId,
                    request.ListType,
                    request.PageSize,
                    request.Cursor,
                    cancellationToken)
                .ConfigureAwait(false);

            SendListResponse(
                session,
                new RelationshipListResponse
                {
                    RequestId = result.RequestId,
                    Succeeded = result.Succeeded,
                    ErrorCode = result.ErrorCode,
                    ErrorMessage = result.ErrorMessage,
                    Items = result.Items,
                    NextCursor = result.NextCursor,
                    HasMore = result.HasMore
                });
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _metrics.CommandFailed(PacketCommand.RelationshipListRequest);
            _logger.CommandFailed(
                PacketCommand.RelationshipListRequest,
                session.ConnectionId,
                requestId,
                exception);
            SendListResponse(
                session,
                new RelationshipListResponse
                {
                    RequestId = requestId,
                    Succeeded = false,
                    ErrorCode = "relationship_service_unavailable",
                    ErrorMessage = "关系服务暂时不可用。"
                });
        }
    }

    private void SendCommandResponse(
        TcpClientSession session,
        RelationshipCommandResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.RelationshipCommandResponse,
            _commandResponseCodec,
            response);
        session.TryQueue(frame);
    }

    private void SendListResponse(
        TcpClientSession session,
        RelationshipListResponse response)
    {
        using var frame = OutboundFrameFactory.Create(
            PacketCommand.RelationshipListResponse,
            _listResponseCodec,
            response);
        session.TryQueue(frame);
    }
}
