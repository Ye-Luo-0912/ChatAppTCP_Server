using System.Buffers;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Commands.Presence;

/// <summary>
/// 在线状态相关命令处理器（PresenceQuery / PresenceUnwatch）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec、<see cref="PresenceWatcherRegistry"/>、
/// <see cref="IWatcherGatewayDirectory"/>、<see cref="IGlobalPresenceStore"/>、
/// <see cref="IRealtimeMessageBus"/> 等端口，不再依赖 service 私有字段。行为与原内联
/// handler 完全等价（校验顺序、错误码、metric 与日志事件）。
/// </para>
/// <para>
/// PresenceQuery 直接用 <see cref="OutboundFrameFactory.Create"/> 构造响应帧（不走 Send 包装），
/// 与原代码一致；PresenceUnwatch 无响应帧。
/// </para>
/// </summary>
internal sealed class PresenceCommandHandler : ICommandHandler
{
    private readonly TcpGatewayOptions _options;
    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeIntegrationOptions _integrationOptions;
    private readonly IGlobalPresenceStore _globalPresence;
    private readonly UserSessionRegistry _userSessions;
    private readonly PresenceWatcherRegistry _presenceWatchers;
    private readonly IWatcherGatewayDirectory _watcherDirectory;
    private readonly IPayloadCodec<PresenceQueryRequest> _presenceQueryRequestCodec;
    private readonly IPayloadCodec<PresenceUnwatchRequest> _presenceUnwatchRequestCodec;
    private readonly IPayloadCodec<PresenceSnapshotResponse> _presenceSnapshotResponseCodec;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<PresenceCommandHandler> _logger;

    public PresenceCommandHandler(
        IOptions<TcpGatewayOptions> options,
        IRealtimeMessageBus messageBus,
        RealtimeIntegrationOptions integrationOptions,
        IGlobalPresenceStore globalPresence,
        UserSessionRegistry userSessions,
        PresenceWatcherRegistry presenceWatchers,
        IWatcherGatewayDirectory watcherDirectory,
        IPayloadCodec<PresenceQueryRequest> presenceQueryRequestCodec,
        IPayloadCodec<PresenceUnwatchRequest> presenceUnwatchRequestCodec,
        IPayloadCodec<PresenceSnapshotResponse> presenceSnapshotResponseCodec,
        GatewayMetrics metrics,
        ILogger<PresenceCommandHandler> logger)
    {
        _options = options.Value;
        _messageBus = messageBus;
        _integrationOptions = integrationOptions;
        _globalPresence = globalPresence;
        _userSessions = userSessions;
        _presenceWatchers = presenceWatchers;
        _watcherDirectory = watcherDirectory;
        _presenceQueryRequestCodec = presenceQueryRequestCodec;
        _presenceUnwatchRequestCodec = presenceUnwatchRequestCodec;
        _presenceSnapshotResponseCodec = presenceSnapshotResponseCodec;
        _metrics = metrics;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.PresenceQuery => HandlePresenceQueryAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.PresenceUnwatch => HandlePresenceUnwatchAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };

    /// <summary>
    /// 查询指定用户集合的在线状态并订阅其变化。授权结果与请求集合做交集后，
    /// 登记本机 watcher 与全局 watcher 目录，返回当前在线快照。
    /// <see cref="TcpGatewayOptions.EnableEphemeralPresenceAndTyping"/> 关闭时直接返回空快照。
    /// </summary>
    private async ValueTask HandlePresenceQueryAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var request = _presenceQueryRequestCodec.Deserialize(payload);
        if (request is null || string.IsNullOrWhiteSpace(request.RequestId))
            return;

        if (!_options.EnableEphemeralPresenceAndTyping)
        {
            using var disabled = OutboundFrameFactory.Create(
                PacketCommand.PresenceSnapshot,
                _presenceSnapshotResponseCodec,
                new PresenceSnapshotResponse
                {
                    RequestId = request.RequestId.Trim(),
                    Items = []
                });
            session.TryQueue(disabled);
            return;
        }

        var requested = (request.UserIds ?? Array.Empty<long>())
            .Where(id => id > 0 && id != session.UserId)
            .Distinct()
            .Take(100)
            .ToArray();

        long[] allowedIds;
        try
        {
            var auth = await _messageBus
                .AuthorizePresenceAsync(
                    new PresenceAuthorizeQuery
                    {
                        WatcherUserId = session.UserId,
                        TargetUserIds = requested
                    },
                    cancellationToken)
                .ConfigureAwait(false);

            // 授权结果与原始请求集合做交集。
            // 授权服务若返回请求范围外的用户（实现 bug 或协议变更），Gateway 不得订阅或返回其在线状态。
            // 此处使用 HashSet 做 O(1) 交集，保留 requested 顺序以便客户端映射。
            if (auth.AllowedUserIds is null || auth.AllowedUserIds.Count == 0)
            {
                allowedIds = [];
            }
            else if (requested.Length == 0)
            {
                allowedIds = [];
            }
            else
            {
                var requestedSet = requested.Length <= 64
                    ? null
                    : new HashSet<long>(requested);
                var result = new List<long>(Math.Min(requested.Length, auth.AllowedUserIds.Count));
                foreach (var id in auth.AllowedUserIds)
                {
                    if (id <= 0)
                        continue;
                    // 交集：id 必须在 requested 中。
                    if (requestedSet is not null)
                    {
                        if (requestedSet.Contains(id) && !result.Contains(id))
                            result.Add(id);
                    }
                    else
                    {
                        // 小集合线性扫描避免 HashSet 分配。
                        var found = false;
                        foreach (var rid in requested)
                        {
                            if (rid == id)
                            {
                                found = true;
                                break;
                            }
                        }
                        if (found && !result.Contains(id))
                            result.Add(id);
                    }
                }
                allowedIds = result.ToArray();
            }
        }
        catch (Exception ex)
        {
            _metrics.PresenceQueryFailed();
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PresenceAuthorize,
                ex);
            allowedIds = [];
        }

        _presenceWatchers.WatchMany(allowedIds, session.UserId);

        // 分片路由：将被观察用户与本实例的对应关系登记到全局 watcher 目录，
        // 供 Presence 事件发布方定向投递。失败不阻断查询响应。
        // 目录以 watcherUserId + gatewayInstanceId 为幂等成员；同一关系重复注册不会产生重复路由。
        if (allowedIds.Length > 0)
        {
            try
            {
                await _watcherDirectory
                    .RegisterWatchersAsync(
                        session.UserId,
                        allowedIds,
                        _integrationOptions.InstanceId,
                        cancellationToken)
                    .ConfigureAwait(false);
                _metrics.WatcherDirectoryOp("register", success: true);
            }
            catch (Exception ex)
            {
                _metrics.WatcherDirectoryOp("register", success: false);
                _logger.DependencyOperationFailed(
                    GatewayDependency.Redis,
                    GatewayDependencyOperation.WatcherDirectoryQuery,
                    ex);
            }
        }

        IReadOnlyDictionary<long, bool> onlineMap;
        try
        {
            onlineMap = await _globalPresence
                .GetOnlineManyAsync(allowedIds, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            onlineMap = new Dictionary<long, bool>();
        }

        var items = new PresenceSnapshotItem[allowedIds.Length];
        for (var i = 0; i < allowedIds.Length; i++)
        {
            var userId = allowedIds[i];
            var localOnline = _userSessions.GetSnapshot(userId).Length > 0;
            var globalOnline = onlineMap.TryGetValue(userId, out var on) && on;
            items[i] = new PresenceSnapshotItem
            {
                UserId = userId,
                IsOnline = localOnline || globalOnline
            };
        }

        var response = new PresenceSnapshotResponse
        {
            RequestId = request.RequestId.Trim(),
            Items = items
        };

        using var outbound = OutboundFrameFactory.Create(
            PacketCommand.PresenceSnapshot,
            _presenceSnapshotResponseCodec,
            response);
        session.TryQueue(outbound);
    }

    /// <summary>
    /// 取消对指定用户集合的在线状态订阅，并从全局 watcher 目录注销对应关系。
    /// <see cref="TcpGatewayOptions.EnableEphemeralPresenceAndTyping"/> 关闭时直接返回。
    /// </summary>
    private async ValueTask HandlePresenceUnwatchAsync(
        ReadOnlySequence<byte> payload,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (!_options.EnableEphemeralPresenceAndTyping)
            return;

        var request = _presenceUnwatchRequestCodec.Deserialize(payload);
        if (request?.UserIds is null || request.UserIds.Count == 0)
            return;

        var selfUserId = session.UserId;
        var userIds = request.UserIds
            .Where(id => id > 0 && id != selfUserId)
            .Distinct()
            .Take(100)
            .ToArray();
        _presenceWatchers.UnwatchMany(userIds, selfUserId);

        // 分片路由：从全局 watcher 目录注销对应关系。失败不阻断客户端请求。
        if (userIds.Length > 0)
        {
            try
            {
                await _watcherDirectory
                    .UnregisterWatchersAsync(
                        selfUserId,
                        userIds,
                        _integrationOptions.InstanceId,
                        cancellationToken)
                    .ConfigureAwait(false);
                _metrics.WatcherDirectoryOp("unregister", success: true);
            }
            catch (Exception ex)
            {
                _metrics.WatcherDirectoryOp("unregister", success: false);
                _logger.DependencyOperationFailed(
                    GatewayDependency.Redis,
                    GatewayDependencyOperation.WatcherDirectoryQuery,
                    ex);
            }
        }
    }
}
