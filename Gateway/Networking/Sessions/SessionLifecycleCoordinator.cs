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
using ChatApp.TcpGateway.Infrastructure.Caching;
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
/// <para>
/// 实现拆分至 partial 文件：
/// <list type="bullet">
/// <item><see cref="SessionLifecycleCoordinator.Presence"/> — Presence 广播与发布</item>
/// <item><see cref="SessionLifecycleCoordinator.DeviceSession"/> — 同设备替换与 ResumeToken 撤销</item>
/// </list>
/// </para>
/// </summary>
internal sealed partial class SessionLifecycleCoordinator
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
    // 可选 Redis 熔断器：仅用于 TryResumeAsync 入口快速失败判定与 CircuitOpen 失败归因。
    // 实际 Redis 调用由 IResumeTokenStore / IDeviceSessionLeaseStore 内部再次检查。
    private readonly IRedisCircuitBreaker? _circuitBreaker;

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
        IPayloadCodec<PresenceChanged> presenceChangedCodec,
        IRedisCircuitBreaker? circuitBreaker = null)
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
        _circuitBreaker = circuitBreaker;
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
            var token = await _resumeTokenStore.IssueAsync(
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
            session.CurrentResumeToken = token;
            return token;
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

        // Resume 路径可观测性：每次尝试 +1，用于计算 Resume 命中率与失败分布。
        _metrics.ResumeAttempted();

        // 熔断器开路：直接快速失败，不发起 Redis 调用，避免重连风暴串行排队。
        // 此处单独计数 CircuitOpen 失败原因，与 store 内部静默返回 null 区分。
        if (_circuitBreaker is { IsAvailable: false })
        {
            _metrics.RedisCircuitBreakerOpen();
            _metrics.ResumeFailed(ResumeFailureReason.CircuitOpen);
            return null;
        }

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
            _metrics.ResumeFailed(ResumeFailureReason.RedisFailure);
            return null;
        }

        if (context is null)
        {
            _metrics.ResumeFailed(ResumeFailureReason.InvalidToken);
            return null;
        }

        // 代次校验：若待恢复会话携带 DeviceIdHash，查询当前设备租约持有者。
        // 若租约已被另一个 SessionId 接管（同设备重新登录/管理员踢下线），
        // 说明此 ResumeContext 来自已被替换的旧会话，必须拒绝恢复。
        // 这与同设备替换时调用 RevokeAsync 撤销旧 Token 形成双重防线：
        //   1) RevokeAsync 删除 Redis 中的旧 Token（阻断 Token 复活）
        //   2) 此处代次校验拦截在 Token 被 Revoke 前的 TTL 窗口内已消费的恢复请求
        if (context.DeviceIdHash is { } resumeDeviceHash
            && !string.IsNullOrWhiteSpace(context.SessionId))
        {
            try
            {
                var currentLeaseSessionId = await _deviceSessionLeaseStore
                    .GetCurrentSessionIdAsync(
                        context.UserId,
                        resumeDeviceHash,
                        cancellationToken)
                    .ConfigureAwait(false);

                if (!string.IsNullOrWhiteSpace(currentLeaseSessionId)
                    && !string.Equals(
                        currentLeaseSessionId,
                        context.SessionId,
                        StringComparison.Ordinal))
                {
                    // 设备租约已归属另一个更新的会话，拒绝恢复旧会话。
                    _logger.TransportFailed(
                        GatewayTransportOperation.ClientProcessing,
                        session.ConnectionId,
                        new InvalidOperationException(
                            "Resume rejected: device lease owned by a newer session."));
                    _metrics.ResumeFailed(ResumeFailureReason.LeaseMismatch);
                    return null;
                }
            }
            catch (Exception ex)
            {
                _logger.DependencyOperationFailed(
                    GatewayDependency.Redis,
                    GatewayDependencyOperation.DeviceLeaseQuery,
                    ex);
                // 查询失败时不阻断恢复（退化为旧行为），由 RevokeAsync 兜底。
            }
        }

        // 恢复会话：复用原 UserId/SessionId/DeviceId。
        session.Authenticate(
            context.UserId,
            context.SessionId,
            context.DeviceIdHash,
            context.DeviceId);

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
            string? previousSessionId = null;
            try
            {
                previousSessionId = await _deviceSessionLeaseStore.TakeOverAsync(
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

            // 接管发现跨 Gateway 旧 SessionId：广播 SessionRevoked 让目标 Gateway 关闭旧连接。
            // 与 ReplaceSameDeviceSessionsAsync 行为一致；Resume 之前未广播会导致旧 Gateway
            // 在 SessionRevoked 事件到达前继续向已恢复 session 发送出站帧（虽本机会话已被新连接接管）。
            if (!string.IsNullOrWhiteSpace(previousSessionId)
                && !string.Equals(
                    previousSessionId,
                    context.SessionId,
                    StringComparison.Ordinal))
            {
                var occurredAtMs = _timeProvider
                    .GetUtcNow()
                    .ToUnixTimeMilliseconds();
                await PublishSessionRevokedEventAsync(
                    context.UserId,
                    previousSessionId,
                    occurredAtMs,
                    session.ConnectionId,
                    cancellationToken).ConfigureAwait(false);
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
                session.CurrentResumeToken = newToken;
            }
            catch (Exception ex)
            {
                _logger.TransportFailed(
                    GatewayTransportOperation.ClientProcessing,
                    session.ConnectionId,
                    ex);
            }
        }

        // 查询服务端同步水位：调用 SyncBootstrap（最小 limit）获取 ServerTimeMs 作为水位。
        // 短超时 + best-effort：失败或超时返回 null，不影响 Resume 成功路径。
        var lastConversationSequence = await QueryResumeWatermarkAsync(
                context.UserId,
                context.DeviceIdHash,
                cancellationToken)
            .ConfigureAwait(false);

        _metrics.ResumeSucceeded();

        return new ResumeLifecycleResult
        {
            ResumeToken = newToken,
            UserId = context.UserId,
            SessionId = context.SessionId,
            DeviceId = context.DeviceId,
            LastConversationSequence = lastConversationSequence
        };
    }

    /// <summary>
    /// 查询服务端同步水位（Unix 毫秒），用于填充 ResumeResponse.LastConversationSequence。
    /// <para>
    /// 调用 <see cref="IRealtimeMessageBus.QuerySyncBootstrapAsync"/> 携带最小 limit
    /// （ListLimit=0, HistoryLimitPerConversation=0, MaxConversationsWithHistory=0, Watermarks=null）
    /// 获取服务端 <see cref="ChatApp.Realtime.Abstractions.Sync.SyncBootstrapPage.ServerTimeMs"/>。
    /// </para>
    /// <para>
    /// 短超时（500ms）+ best-effort：任何异常（NATS 超时、取消、协议错误）均吞掉并返回 null。
    /// 调用方据此决定是否填充 LastConversationSequence 字段；返回 null 时客户端应回退到
    /// “始终 SyncBootstrap”策略（与未填充字段等价）。
    /// </para>
    /// </summary>
    private async Task<long?> QueryResumeWatermarkAsync(
        long userId,
        ulong? deviceIdHash,
        CancellationToken cancellationToken)
    {
        if (userId <= 0)
            return null;

        // 短超时：Resume 是热路径，水位查询不应阻塞重连超过 500ms。
        // 超时取消与外部 cancellationToken 解耦：外部取消仍立即生效，超时只限制查询本身。
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(TimeSpan.FromMilliseconds(500));

        var query = new ChatApp.Realtime.Abstractions.Sync.SyncBootstrapQuery
        {
            RequestId = Guid.CreateVersion7().ToString("N"),
            UserId = userId,
            DeviceIdHash = deviceIdHash,
            // 最小 limit：只要 ServerTimeMs，不要会话列表与历史。
            ListLimit = 0,
            HistoryLimitPerConversation = 0,
            MaxConversationsWithHistory = 0,
            Watermarks = null
        };

        try
        {
            var page = await _messageBus
                .QuerySyncBootstrapAsync(query, timeoutCts.Token)
                .ConfigureAwait(false);

            if (!page.Succeeded)
                return null;

            return page.ServerTimeMs > 0 ? page.ServerTimeMs : null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // 外部取消：传播（不应在此吞掉）。
            throw;
        }
        catch (Exception ex)
        {
            // 超时或 NATS 故障：吞掉，返回 null。Resume 主路径不受影响。
            _logger.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.ResumeWatermarkQuery,
                ex);
            return null;
        }
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
    /// <returns><c>true</c> 刷新成功；<c>false</c> Redis 异常或非所有者（已吞异常并记录日志）。</returns>
    public async Task<bool> RefreshLeaseAsync(
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
            return true;
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
            return false;
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
    /// <returns><c>true</c> 刷新成功；<c>false</c> Redis 异常（已吞异常并记录日志）。</returns>
    public async Task<bool> RefreshPresenceAsync(
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
            return true;
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
            return false;
        }
        finally
        {
            gate.Release();
        }
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

    /// <summary>
    /// 服务端最后已知同步水位（Unix 毫秒），由 SyncBootstrap 查询返回的 ServerTimeMs 充当。
    /// 客户端可据此判断是否需要触发增量 SyncBootstrap：若客户端记录的最后 ServerTimeMs
    /// 小于本值，则发起 SyncBootstrap 拉取缺失数据；否则可跳过。
    /// <para>
    /// 查询失败或超时返回 null，客户端应回退到“始终 SyncBootstrap”策略。
    /// </para>
    /// </summary>
    public long? LastConversationSequence { get; init; }
}
