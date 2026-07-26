using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 集中协调 TCP 会话生命周期：登录注册、同设备替换、Resume 恢复、
/// 设备租约管理、Presence 上下线广播与连接销毁清理。
/// <para>
/// 从 <see cref="Networking.TcpGatewayService"/> 抽取，消除 God Service 中
/// 散落在 7 个方法中的生命周期逻辑。协议级关注点（握手编解码、admission 跟踪、
/// 帧发送）仍由 <see cref="Networking.TcpGatewayService"/> 负责；本类型只处理
/// 会话状态、Presence 与设备租约。
/// </para>
/// <para>
/// 由 <see cref="Networking.TcpGatewayService"/> 在构造时内部创建并传入已注入的依赖，
/// 因此 service 构造函数签名不变，既有测试无需修改。
/// </para>
/// </summary>
internal sealed class SessionLifecycleCoordinator
{
    private readonly IDeviceSessionLeaseStore _deviceSessionLeaseStore;
    private readonly IGlobalPresenceStore _globalPresence;
    private readonly IResumeTokenStore? _resumeTokenStore;
    private readonly UserSessionRegistry _userSessions;
    private readonly PresenceWatcherRegistry _presenceWatchers;
    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeIntegrationOptions _integrationOptions;
    private readonly TcpGatewayOptions _options;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly IPayloadCodec<PresenceChanged> _presenceChangedCodec;

    public SessionLifecycleCoordinator(
        IDeviceSessionLeaseStore deviceSessionLeaseStore,
        IGlobalPresenceStore globalPresence,
        IResumeTokenStore? resumeTokenStore,
        UserSessionRegistry userSessions,
        PresenceWatcherRegistry presenceWatchers,
        IRealtimeMessageBus messageBus,
        RealtimeIntegrationOptions integrationOptions,
        TcpGatewayOptions options,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger logger,
        IPayloadCodec<PresenceChanged> presenceChangedCodec)
    {
        _deviceSessionLeaseStore = deviceSessionLeaseStore;
        _globalPresence = globalPresence;
        _resumeTokenStore = resumeTokenStore;
        _userSessions = userSessions;
        _presenceWatchers = presenceWatchers;
        _messageBus = messageBus;
        _integrationOptions = integrationOptions;
        _options = options;
        _metrics = metrics;
        _timeProvider = timeProvider;
        _logger = logger;
        _presenceChangedCodec = presenceChangedCodec;
    }

    /// <summary>
    /// 认证成功后注册会话、广播 Presence 上线、执行同设备替换并颁发 ResumeToken。
    /// <para>
    /// 调用方（<see cref="Networking.TcpGatewayService"/>）需在调用本方法前完成
    /// admission 跟踪（<c>MarkAuthenticated</c>/<c>UnauthenticatedConnectionClosed</c>）。
    /// 本方法返回颁发的 ResumeToken（若启用 Resume 且 store 已注入），调用方据此构造
    /// <see cref="AuthenticationResponse"/>。
    /// </para>
    /// </summary>
    /// <returns>颁发的 ResumeToken；未启用 Resume 或颁发失败时返回 null。</returns>
    public async Task<string?> OnAuthenticatedAsync(
        TcpClientSession session,
        RealtimeAuthenticationResult result,
        CancellationToken cancellationToken)
    {
        session.Authenticate(
            result.UserId,
            result.SessionId,
            result.DeviceIdHash,
            result.DeviceId);

        var becameOnline = _userSessions.Add(session);
        if (becameOnline && _options.EnableEphemeralPresenceAndTyping)
            await PublishPresenceChangedAsync(result.UserId, isOnline: true, cancellationToken)
                .ConfigureAwait(false);

        if (_options.ReplaceSameDeviceSession)
        {
            await ReplaceSameDeviceSessionsAsync(session, cancellationToken)
                .ConfigureAwait(false);
        }

        if (!_options.EnableResume || _resumeTokenStore is null)
            return null;

        try
        {
            return await _resumeTokenStore.IssueAsync(
                new ResumeContext
                {
                    UserId = result.UserId,
                    SessionId = session.SessionId ?? $"tcp-{session.ConnectionId}",
                    ConnectionLeaseId = session.ConnectionLeaseId,
                    DeviceId = result.DeviceId,
                    DeviceIdHash = result.DeviceIdHash
                },
                _options.ResumeTokenTtl,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                ex);
            return null;
        }
    }

    /// <summary>
    /// 尝试凭 ResumeToken 恢复会话：校验 Token、复用原身份认证、注册会话、
    /// 广播 Presence 上线、接管设备租约并颁发新 ResumeToken。
    /// <para>
    /// 调用方在返回 <c>null</c>（校验失败）时应发送 ProtocolError(ResumeFailed)。
    /// 返回结果时调用方负责 admission 跟踪（<c>MarkAuthenticated</c>/
    /// <c>UnauthenticatedConnectionClosed</c>）并构造 <see cref="ResumeResponse"/> 发送。
    /// </para>
    /// </summary>
    /// <returns>恢复成功返回结果；Token 无效或过期返回 null。</returns>
    public async Task<ResumeLifecycleResult?> TryResumeAsync(
        string resumeToken,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (_resumeTokenStore is null)
            return null;

        ResumeContext? context;
        try
        {
            context = await _resumeTokenStore
                .TryValidateAsync(resumeToken, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.ResumeTokenLookup,
                ex);
            return null;
        }

        if (context is null)
            return null;

        // 恢复会话：复用原 UserId/SessionId/DeviceId。
        session.Authenticate(
            context.UserId,
            context.SessionId,
            context.DeviceIdHash,
            context.DeviceId);
        session.MarkHandshakeCompleted();

        if (_userSessions.Add(session) && _options.EnableEphemeralPresenceAndTyping)
        {
            await PublishPresenceChangedAsync(context.UserId, isOnline: true, cancellationToken)
                .ConfigureAwait(false);
        }

        // 设备租约接管：原 ConnectionLeaseId 已随旧连接释放，这里用新 Session 的 ConnectionLeaseId 重新获取。
        // 仅当原会话携带 DeviceIdHash 时才接管。缺少 DeviceIdHash 的会话不持有设备租约，
        // 不应使用伪设备 0 接管——否则所有无 DeviceIdHash 的会话会落入同一零值设备键，相互覆盖租约。
        if (context.DeviceIdHash is { } deviceHash)
        {
            try
            {
                await _deviceSessionLeaseStore.TakeOverAsync(
                    context.UserId,
                    deviceHash,
                    context.SessionId,
                    session.ConnectionLeaseId,
                    _options.IdleTimeout + TimeSpan.FromMinutes(5),
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.TransportFailed(
                    GatewayTransportOperation.ClientProcessing,
                    session.ConnectionId,
                    ex);
            }
        }

        // 颁发新的 ResumeToken（旧 Token 已被消费）。
        string? newToken = null;
        if (_options.EnableResume)
        {
            try
            {
                newToken = await _resumeTokenStore.IssueAsync(
                    new ResumeContext
                    {
                        UserId = context.UserId,
                        SessionId = context.SessionId,
                        ConnectionLeaseId = session.ConnectionLeaseId,
                        DeviceId = context.DeviceId,
                        DeviceIdHash = context.DeviceIdHash
                    },
                    _options.ResumeTokenTtl,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.TransportFailed(
                    GatewayTransportOperation.ClientProcessing,
                    session.ConnectionId,
                    ex);
            }
        }

        return new ResumeLifecycleResult
        {
            ResumeToken = newToken,
            UserId = context.UserId,
            SessionId = context.SessionId,
            DeviceId = context.DeviceId
        };
    }

    /// <summary>
    /// 连接关闭后的清理：从用户会话注册表移除、广播 Presence 下线、
    /// 移除 Presence watcher 订阅、释放设备租约（仅当仍持有所有权时）。
    /// <para>
    /// 由 <see cref="Networking.TcpGatewayService.HandleClientAsync"/> 的 finally 块调用。
    /// 使用 <see cref="CancellationToken.None"/> 避免 host stopping token 取消时跳过清理。
    /// </para>
    /// </summary>
    public async Task OnDisconnectedAsync(
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var wentOffline = _userSessions.Remove(session);
        if (wentOffline)
        {
            if (_options.EnableEphemeralPresenceAndTyping)
                await PublishPresenceChangedAsync(session.UserId, isOnline: false, cancellationToken)
                    .ConfigureAwait(false);
            _presenceWatchers.RemoveWatcher(session.UserId);
        }

        if (session is { UserId: > 0, DeviceIdHash: { } deviceHash }
            && !string.IsNullOrWhiteSpace(session.SessionId))
        {
            try
            {
                // 使用 ConnectionLeaseId 作为所有权令牌释放租约。
                await _deviceSessionLeaseStore
                    .ReleaseIfOwnerAsync(
                        session.UserId,
                        deviceHash,
                        session.ConnectionLeaseId,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _logger.SessionRevocationFailed(
                    session.ConnectionId,
                    session.SessionId,
                    exception);
            }
        }
    }

    /// <summary>
    /// 心跳扫描中刷新设备租约 TTL（仅在仍持有所有权时）。
    /// 限制并发 Redis 往返以避免 10k 连接串行扫描。
    /// </summary>
    public async Task RefreshLeaseAsync(
        SemaphoreSlim gate,
        long userId,
        ulong deviceHash,
        string leaseId,
        TimeSpan leaseTtl,
        CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _deviceSessionLeaseStore
                .RefreshIfOwnerAsync(
                    userId,
                    deviceHash,
                    leaseId,
                    leaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseRefresh,
                exception);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// 心跳扫描中刷新 Redis 全局在线状态 score（防止 TTL 过期误判下线）。
    /// 限制并发 Redis 往返以避免 10k 连接串行扫描。
    /// </summary>
    public async Task RefreshPresenceAsync(
        SemaphoreSlim gate,
        long userId,
        CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _globalPresence
                .RefreshOnlineAsync(
                    userId,
                    _integrationOptions.InstanceId,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.PresenceRefresh,
                exception);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// 只在全局状态转换（0-&gt;1 或 1-&gt;0）时广播与发布跨网关 Presence 事件。
    /// 旧实现每实例本地首连/断开都无条件广播，导致多实例登录时互相覆盖、误报下线。
    /// </summary>
    private async Task PublishPresenceChangedAsync(
        long userId,
        bool isOnline,
        CancellationToken cancellationToken)
    {
        PresenceTransition transition;
        if (isOnline)
            transition = await _globalPresence
                .SetOnlineAsync(userId, _integrationOptions.InstanceId, cancellationToken)
                .ConfigureAwait(false);
        else
            transition = await _globalPresence
                .SetOfflineAsync(userId, _integrationOptions.InstanceId, cancellationToken)
                .ConfigureAwait(false);

        if (transition == PresenceTransition.None)
        {
            _metrics.PresenceTransition("none");
            return;
        }

        var globalIsOnline = transition == PresenceTransition.WentOnline;
        _metrics.PresenceTransition(globalIsOnline ? "online" : "offline");
        BroadcastPresenceChangedLocal(userId, globalIsOnline);

        try
        {
            await _messageBus
                .PublishEphemeralPresenceAsync(
                    new EphemeralPresenceEvent
                    {
                        OriginInstanceId = _integrationOptions.InstanceId,
                        UserId = userId,
                        IsOnline = globalIsOnline
                    },
                    cancellationToken)
                .ConfigureAwait(false);
            _metrics.PresenceEphemeralPublished();
        }
        catch (Exception ex)
        {
            _metrics.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralPresencePublish);
            _logger.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.EphemeralPresencePublish,
                ex);
        }
    }

    private void BroadcastPresenceChangedLocal(long userId, bool isOnline)
    {
        var watchers = _presenceWatchers.GetWatchers(userId);
        if (watchers.Length == 0)
        {
            _metrics.PresenceFanoutSkipped();
            return;
        }

        var update = new PresenceChanged
        {
            UserId = userId,
            IsOnline = isOnline
        };

        using var frame = OutboundFrameFactory.Create(
            PacketCommand.PresenceChanged,
            _presenceChangedCodec,
            update);
        var recipientCount = 0;
        foreach (var watcherId in watchers)
        {
            foreach (var watcherSession in _userSessions.GetSnapshot(watcherId))
            {
                watcherSession.TryQueueEphemeral(frame);
                recipientCount++;
            }
        }
        _metrics.PresenceFanoutDelivered(recipientCount);
    }

    private async ValueTask ReplaceSameDeviceSessionsAsync(
        TcpClientSession incoming,
        CancellationToken cancellationToken)
    {
        // 1) 本机旧连接立即踢下线。
        var localVictims = _userSessions.TakeOverSameDevice(incoming);
        var occurredAtMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        foreach (var victim in localVictims)
            await RevokeSessionAsync(victim, occurredAtMs, cancellationToken).ConfigureAwait(false);

        // 2) Redis/Garnet 设备租约：发现跨 Gateway 的旧 SessionId 并广播 SessionRevoked。
        if (incoming.DeviceIdHash is not { } deviceHash
            || string.IsNullOrWhiteSpace(incoming.SessionId)
            || incoming.UserId <= 0)
        {
            return;
        }

        // TTL 略长于空闲超时，避免正常心跳间隙丢租约；断开时 ReleaseIfOwner。
        var leaseTtl = _options.IdleTimeout + TimeSpan.FromMinutes(5);
        string? previousSessionId;
        try
        {
            // 传入 ConnectionLeaseId 作为所有权令牌。
            previousSessionId = await _deviceSessionLeaseStore
                .TakeOverAsync(
                    incoming.UserId,
                    deviceHash,
                    incoming.SessionId,
                    incoming.ConnectionLeaseId,
                    leaseTtl,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.SessionRevocationFailed(
                incoming.ConnectionId,
                incoming.SessionId,
                exception);
            return;
        }

        if (string.IsNullOrWhiteSpace(previousSessionId)
            || string.Equals(previousSessionId, incoming.SessionId, StringComparison.Ordinal))
        {
            return;
        }

        // 本机已踢过的 SessionId 不必再发；跨实例依赖此事件。
        var alreadyLocal = localVictims.Any(v =>
            string.Equals(v.SessionId, previousSessionId, StringComparison.Ordinal));
        if (alreadyLocal)
            return;

        try
        {
            await _messageBus
                .PublishEventAsync(
                    new RealtimeEvent
                    {
                        EventId = RealtimeEventContracts.CreateSessionRevokedEventId(
                            incoming.UserId,
                            previousSessionId,
                            occurredAtMs),
                        Type = RealtimeEventType.SessionRevoked,
                        TargetUserId = incoming.UserId,
                        SessionId = previousSessionId,
                        OccurredAtMs = occurredAtMs
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.SessionRevocationFailed(
                incoming.ConnectionId,
                previousSessionId,
                exception);
        }
    }

    private async ValueTask RevokeSessionAsync(
        TcpClientSession victim,
        long occurredAtMs,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(victim.SessionId))
        {
            victim.Close(SessionCloseReason.SessionRevoked);
            return;
        }

        try
        {
            await _messageBus
                .PublishEventAsync(
                    new RealtimeEvent
                    {
                        EventId = RealtimeEventContracts.CreateSessionRevokedEventId(
                            victim.UserId,
                            victim.SessionId,
                            occurredAtMs),
                        Type = RealtimeEventType.SessionRevoked,
                        TargetUserId = victim.UserId,
                        SessionId = victim.SessionId,
                        OccurredAtMs = occurredAtMs
                    },
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.SessionRevocationFailed(
                victim.ConnectionId,
                victim.SessionId,
                exception);
        }

        // 本机立即断开；跨 Gateway 实例依赖 SessionRevoked 事件。
        victim.Close(SessionCloseReason.SessionRevoked);
    }
}

/// <summary>
/// <see cref="SessionLifecycleCoordinator.TryResumeAsync"/> 成功时的返回结果。
/// 调用方据此构造 <see cref="ResumeResponse"/> 帧发送给客户端。
/// </summary>
internal sealed class ResumeLifecycleResult
{
    /// <summary>新颁发的 ResumeToken（旧 Token 已失效）；颁发失败时为 null。</summary>
    public string? ResumeToken { get; init; }

    public long UserId { get; init; }

    public string SessionId { get; init; } = string.Empty;

    public string? DeviceId { get; init; }
}
