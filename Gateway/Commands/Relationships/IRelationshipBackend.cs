using ChatApp.Realtime.Integration;
using ChatApp.Shared.Protocol.Tcp;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using RealtimeRelationshipCommand =
    ChatApp.Realtime.Abstractions.Relationships.RelationshipCommand;
using RealtimeRelationshipListQuery =
    ChatApp.Realtime.Abstractions.Relationships.RelationshipListQuery;

namespace ChatApp.TcpGateway.Gateway.Commands.Relationships;

/// <summary>
/// 关系后端端口：抽象 RealtimeServices 侧关系域操作（好友请求/接受/拒绝、拉黑等）。
/// <para>
/// 主线四：当前为 stub 实现（<see cref="StubRelationshipBackend"/>），
/// 返回 <c>relationship_service_unavailable</c>。待 sibling 仓库
/// <c>IRealtimeMessageBus</c> 新增 <c>MutateRelationshipAsync</c> /
/// <c>QueryRelationshipListAsync</c> 后，替换为调用 bus 的适配实现即可。
/// </para>
/// </summary>
internal interface IRelationshipBackend
{
    /// <summary>
    /// 执行关系变更命令（发送/接受/拒绝好友请求、删除好友、拉黑/取消拉黑）。
    /// </summary>
    Task<RelationshipCommandBackendResult> MutateAsync(
        string requestId,
        long actorUserId,
        RelationshipOperation operation,
        long? targetUserId,
        string? message,
        string? requestIdToRespond,
        string? actorSessionId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 查询关系列表（好友 / 好友请求 / 黑名单），支持分页。
    /// </summary>
    Task<RelationshipListBackendResult> QueryListAsync(
        string requestId,
        long actorUserId,
        RelationshipListType listType,
        int? pageSize,
        string? cursor,
        CancellationToken cancellationToken = default);
}

/// <summary>关系变更命令后端结果。</summary>
internal sealed record RelationshipCommandBackendResult(
    string RequestId,
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    RelationshipOperation? Operation,
    long? TargetUserId,
    string? ResourceId)
{
    public static RelationshipCommandBackendResult Success(
        string requestId,
        RelationshipOperation operation,
        long? targetUserId,
        string? resourceId) =>
        new(requestId, true, null, null, operation, targetUserId, resourceId);

    public static RelationshipCommandBackendResult Failed(
        string requestId, string errorCode, string errorMessage) =>
        new(requestId, false, errorCode, errorMessage, null, null, null);
}

/// <summary>关系列表查询后端结果。</summary>
internal sealed record RelationshipListBackendResult(
    string RequestId,
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    IReadOnlyList<RelationshipItem>? Items,
    string? NextCursor,
    bool HasMore)
{
    public static RelationshipListBackendResult Success(
        string requestId,
        IReadOnlyList<RelationshipItem> items,
        string? nextCursor,
        bool hasMore) =>
        new(requestId, true, null, null, items, nextCursor, hasMore);

    public static RelationshipListBackendResult Failed(
        string requestId, string errorCode, string errorMessage) =>
        new(requestId, false, errorCode, errorMessage, null, null, false);
}

/// <summary>
/// 占位实现：RealtimeServices 侧关系域后端尚未接入。
/// 返回 <c>relationship_service_unavailable</c>，客户端可稍后重试。
/// </summary>
internal sealed class StubRelationshipBackend : IRelationshipBackend
{
    private readonly ILogger<StubRelationshipBackend> _logger;

    public StubRelationshipBackend(ILogger<StubRelationshipBackend> logger)
    {
        _logger = logger;
    }

    public Task<RelationshipCommandBackendResult> MutateAsync(
        string requestId,
        long actorUserId,
        RelationshipOperation operation,
        long? targetUserId,
        string? message,
        string? requestIdToRespond,
        string? actorSessionId,
        CancellationToken cancellationToken = default)
    {
        _logger.RelationshipMutateBackendUnavailable(requestId, (int)operation, actorUserId);
        return Task.FromResult(RelationshipCommandBackendResult.Failed(
            requestId,
            "relationship_service_unavailable",
            "关系服务暂未配置。"));
    }

    public Task<RelationshipListBackendResult> QueryListAsync(
        string requestId,
        long actorUserId,
        RelationshipListType listType,
        int? pageSize,
        string? cursor,
        CancellationToken cancellationToken = default)
    {
        _logger.RelationshipListBackendUnavailable(requestId, (int)listType, actorUserId);
        return Task.FromResult(RelationshipListBackendResult.Failed(
            requestId,
            TcpRelationshipListErrorCode.ProjectionUnavailable,
            "关系服务暂未配置。"));
    }
}

/// <summary>
/// 生产适配实现：经 <see cref="IRealtimeMessageBus.MutateRelationshipAsync"/> /
/// <see cref="IRealtimeMessageBus.QueryRelationshipListAsync"/> 将关系域命令转发到
/// RealtimeServices，由其执行权限校验与业务规则。
/// <para>
/// 总线异常（含 <see cref="RealtimeServerBusyException"/>、NATS 超时等）不做吞咽，
/// 直接向 <see cref="RelationshipCommandHandler"/> 抛出，由其 catch-all 统一映射为
/// <c>relationship_service_unavailable</c> 响应。这与其他 Realtime 命令处理器
/// （<see cref="RealtimeAttachmentBackend"/> / <c>GroupCommandHandler</c>）的异常约定一致。
/// </para>
/// <para>
/// <c>RelationshipOperation</c> / <c>RelationshipListType</c> 直接使用
/// ChatApp.Realtime.Contracts 的共享枚举，不再维护本地副本或数值转换。
/// </para>
/// </summary>
internal sealed class RealtimeRelationshipBackend : IRelationshipBackend
{
    private readonly IRealtimeMessageBus _messageBus;

    public RealtimeRelationshipBackend(IRealtimeMessageBus messageBus)
    {
        _messageBus = messageBus;
    }

    public async Task<RelationshipCommandBackendResult> MutateAsync(
        string requestId,
        long actorUserId,
        RelationshipOperation operation,
        long? targetUserId,
        string? message,
        string? requestIdToRespond,
        string? actorSessionId,
        CancellationToken cancellationToken = default)
    {
        var command = new RealtimeRelationshipCommand
        {
            RequestId = requestId,
            ActorUserId = actorUserId,
            Operation = operation,
            TargetUserId = targetUserId,
            Message = message,
            RequestIdToRespond = requestIdToRespond,
            ActorSessionId = actorSessionId
        };

        var result = await _messageBus
            .MutateRelationshipAsync(command, cancellationToken)
            .ConfigureAwait(false);

        return new RelationshipCommandBackendResult(
            result.RequestId,
            result.Succeeded,
            result.ErrorCode,
            result.ErrorMessage,
            result.Operation,
            result.TargetUserId,
            result.ResourceId);
    }

    public async Task<RelationshipListBackendResult> QueryListAsync(
        string requestId,
        long actorUserId,
        RelationshipListType listType,
        int? pageSize,
        string? cursor,
        CancellationToken cancellationToken = default)
    {
        var query = new RealtimeRelationshipListQuery
        {
            RequestId = requestId,
            ActorUserId = actorUserId,
            ListType = listType,
            PageSize = pageSize,
            Cursor = cursor
        };

        var result = await _messageBus
            .QueryRelationshipListAsync(query, cancellationToken)
            .ConfigureAwait(false);

        return new RelationshipListBackendResult(
            result.RequestId,
            result.Succeeded,
            result.ErrorCode,
            result.ErrorMessage,
            result.Items,
            result.NextCursor,
            result.HasMore);
    }
}
