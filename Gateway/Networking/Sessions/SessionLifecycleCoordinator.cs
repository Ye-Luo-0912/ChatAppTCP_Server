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
    // 三-3：冻结用户缓存。fail-open + 后台刷新，供认证/Resume 路径快速拒绝冻结用户。
    private readonly IFrozenUserCache? _frozenUserCache;

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
        IRedisCircuitBreaker? circuitBreaker = null,
        IFrozenUserCache? frozenUserCache = null)
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
        _frozenUserCache = frozenUserCache;
    }

    /// <summary>
    /// 认证成功后注册会话、广播 Presence 上线、执行同设备替换并颁发 ResumeToken。
    /// <para>
    /// 调用方（<see cref="Networking.TcpGatewayService"/>）需在调用本方法前完成
    /// admission 跟踪（<c>MarkAuthenticated</c>/<c>UnauthenticatedConnectionClosed</c>）。
    /// </para>
    /// <para>
    /// P1-C：返回 <see cref="AuthLifecycleResult"/>，区分成功与依赖不可用失败。
    /// 调用方据 <see cref="AuthLifecycleResult.Success"/> 决定发送
    /// <see cref="AuthenticationResponse"/>(Success=true) 或
    /// <see cref="AuthenticationResponse"/>(Success=false, DependencyUnavailable) 并关闭连接。
    /// </para>
    /// <para>
    /// 失败场景仅 <see cref="AuthFailureKind.DependencyUnavailable"/>：
    /// <see cref="TcpGatewayOptions.AuthRedisFailMode"/>=<see cref="RedisFailMode.FailClosed"/>
    /// 时 TakeOver 依赖不可用 → 回滚本地状态、拒绝认证、关闭连接。
    /// <see cref="RedisFailMode.FailOpen"/> 时 TakeOver 失败仅记录日志，继续完成认证（旧行为）。
    /// </para>
    /// </summary>
    public async Task<AuthLifecycleResult> OnAuthenticatedAsync(
        TcpClientSession session,
        RealtimeAuthenticationResult result,
        CancellationToken cancellationToken)
    {
        // 三-3：冻结用户拒绝认证。fail-open 缓存未命中时放行——
        // 认证路径权威性由 AccessTokenStore 保证（冻结时 Server 撤销 access token），
        // 此处为快速拦截已预热缓存的冻结用户，避免无谓的 Registry/Presence/TakeOver 副作用。
        // 失败 metric 由 SessionControlHandler.SendAuthenticationFailure 记录，此处不重复计数。
        if (_frozenUserCache is not null && _frozenUserCache.IsFrozen(result.UserId))
        {
            return AuthLifecycleResult.Failed(AuthFailureKind.UserFrozen);
        }

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
            // P1-C：AuthRedisFailMode 控制 TakeOver 依赖不可用时的行为。
            // 返回 false 表示 fail-closed 拒绝认证；true 表示继续（fail-open 或 TakeOver 成功）。
            var continueAuth = await ReplaceSameDeviceSessionsAsync(
                    session, cancellationToken)
                .ConfigureAwait(false);

            if (!continueAuth)
            {
                // FailClosed：回滚已完成的本地状态变更（Registry + Presence），
                // 调用方据此发送 AuthenticationResponse(Success=false) 并关闭连接。
                // P1-D：复用 AbortLocalStateAsync（与 Resume Commit 共用回滚逻辑）。
                await AbortLocalStateAsync(session, result.UserId, cancellationToken)
                    .ConfigureAwait(false);
                return AuthLifecycleResult.Failed(
                    AuthFailureKind.DependencyUnavailable,
                    RetryAfterMsForDependency);
            }
        }

        if (!_options.EnableResume || _resumeTokenStore is null)
            return AuthLifecycleResult.Succeeded(resumeToken: null);

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
            return AuthLifecycleResult.Succeeded(token);
        }
        catch (Exception ex)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                ex);
            // Token 颁发失败不阻断认证：会话已注册，客户端下次重连走完整认证。
            return AuthLifecycleResult.Succeeded(resumeToken: null);
        }
    }

    /// <summary>
    /// 尝试凭 ResumeToken 恢复会话：校验 Token、复用原身份认证、注册会话、
    /// 广播 Presence 上线、接管设备租约并颁发新 ResumeToken。
    /// <para>
    /// P1-B：返回 <see cref="ResumeAttemptResult"/>，区分成功与失败种类。
    /// 调用方据 <see cref="ResumeAttemptResult.FailureKind"/> 决定发送
    /// <see cref="ProtocolErrorCode.ResumeFailed"/>（不可恢复）或
    /// <see cref="ProtocolErrorCode.DependencyUnavailable"/>（可重试）。
    /// </para>
    /// <para>
    /// 返回 null 表示未尝试 Resume（store 未注入或未启用）；
    /// 非 null 时调用方据 <see cref="ResumeAttemptResult.Success"/> 分支处理。
    /// 成功时调用方负责 admission 跟踪（<c>MarkAuthenticated</c>/
    /// <c>UnauthenticatedConnectionClosed</c>）并构造 <see cref="ResumeResponse"/> 发送。
    /// </para>
    /// <para>
    /// P1-D：内部拆分为两阶段提交：
    /// <list type="number">
    /// <item><term>Prepare</term><description>只读校验（熔断器、Token、代次），见 <see cref="PrepareResumeAsync"/>。</description></item>
    /// <item><term>Commit</term><description>状态变更（注册、Presence、TakeOver、Token），见 <see cref="CommitResumeAsync"/>。</description></item>
    /// <item><term>Abort</term><description>Commit 失败时回滚本地状态，见 <see cref="AbortLocalStateAsync"/>。</description></item>
    /// </list>
    /// Prepare 失败直接返回（无可观测副作用，无需 Abort）；
    /// Commit 失败时由其内部调用 Abort 回滚本地状态后返回失败。
    /// </para>
    /// </summary>
    /// <returns>未尝试返回 null；尝试后返回成功或失败结果。</returns>
    public async Task<ResumeAttemptResult?> TryResumeAsync(
        string resumeToken,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        // Resume 路径可观测性：每次尝试 +1，用于计算 Resume 命中率与失败分布。
        _metrics.ResumeAttempted();

        // P1-D：Prepare 阶段——只读校验，无副作用。
        var prepareResult = await PrepareResumeAsync(
                resumeToken, session, cancellationToken)
            .ConfigureAwait(false);

        // store 未注入：未尝试 Resume。
        if (prepareResult is null)
            return null;

        // Prepare 失败：直接返回失败（无可观测副作用，无需 Abort）。
        if (!prepareResult.Success)
            return prepareResult.ToAttemptResult();

        // P1-D：Commit 阶段——执行状态变更。
        // Commit 内部在 TakeOver 失败时自动调用 Abort 回滚本地状态。
        return await CommitResumeAsync(
                prepareResult.Prepared!, session, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// P1-B：依赖不可用时建议客户端的重试退避（毫秒）。
    /// 客户端应在此时间后重试 Resume，或回退到完整认证。
    /// </summary>
    private const int RetryAfterMsForDependency = 1000;

    // P1-D：原 RollbackResumeLocalStateAsync 已重命名为 AbortResumeAsync，
    // 移至 SessionLifecycleCoordinator.ResumeCommit.cs（两阶段提交的 Abort 阶段）。

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
                // P1-A2：使用私有 LeaseOwnerToken 做 CAS 释放，遵循最小权限原则。
                // 旧实现用公开 ConnectionLeaseId（TransportId）做 CAS——若 TransportId 泄漏到
                // 日志/事件，攻击者可构造 Release 请求擦除他人租约。
                await _deviceSessionLeaseStore
                    .ReleaseIfOwnerAsync(
                        session.UserId,
                        deviceHash,
                        session.LeaseOwnerToken,
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
    /// </summary>
    /// <param name="gate">并发限制信号量；为 null 时由调用方（如固定 Worker 池）保证并发上限。</param>
    /// <param name="leaseOwnerToken">P1-A2：私有所有权凭证（非公开 TransportId），用于 Redis CAS。</param>
    /// <returns><c>true</c> 刷新成功；<c>false</c> Redis 异常或非所有者（已吞异常并记录日志）。</returns>
    public async Task<bool> RefreshLeaseAsync(
        SemaphoreSlim? gate,
        long userId,
        ulong deviceHash,
        string leaseOwnerToken,
        TimeSpan leaseTtl,
        CancellationToken cancellationToken)
    {
        if (gate is not null)
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _deviceSessionLeaseStore
                .RefreshIfOwnerAsync(
                    userId,
                    deviceHash,
                    leaseOwnerToken,
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
            if (gate is not null)
                gate.Release();
        }
    }

    /// <summary>
    /// 心跳扫描中刷新 Redis 全局在线状态 score（防止 TTL 过期误判下线）。
    /// </summary>
    /// <param name="gate">并发限制信号量；为 null 时由调用方（如固定 Worker 池）保证并发上限。</param>
    /// <returns><c>true</c> 刷新成功；<c>false</c> Redis 异常（已吞异常并记录日志）。</returns>
    public async Task<bool> RefreshPresenceAsync(
        SemaphoreSlim? gate,
        long userId,
        CancellationToken cancellationToken)
    {
        if (gate is not null)
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
            if (gate is not null)
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

/// <summary>
/// P1-B：<see cref="SessionLifecycleCoordinator.TryResumeAsync"/> 的统一返回类型。
/// <para>
/// 成功时 <see cref="Success"/>=true 且 <see cref="Result"/> 非空；
/// 失败时 <see cref="Success"/>=false 且 <see cref="FailureKind"/> 指示失败种类，
/// 调用方据此发送 <see cref="Core.Protocol.ProtocolErrorCode.ResumeFailed"/>
/// 或 <see cref="Core.Protocol.ProtocolErrorCode.DependencyUnavailable"/>。
/// </para>
/// </summary>
internal sealed class ResumeAttemptResult
{
    public bool Success { get; init; }

    /// <summary>成功时的恢复结果；失败时为 null。</summary>
    public ResumeLifecycleResult? Result { get; init; }

    /// <summary>失败种类（仅 <see cref="Success"/>=false 时有意义）。</summary>
    public ResumeFailureKind FailureKind { get; init; }

    /// <summary>
    /// 内部失败原因明细（用于 metrics 归因，比 <see cref="FailureKind"/> 更细粒度）。
    /// 仅 <see cref="Success"/>=false 时有意义。
    /// </summary>
    public ResumeFailureReason? FailureReason { get; init; }

    /// <summary>依赖不可用时的重试退避建议（毫秒）；仅 DependencyUnavailable 时有意义。</summary>
    public int? RetryAfterMs { get; init; }

    public static ResumeAttemptResult Succeeded(ResumeLifecycleResult result) =>
        new() { Success = true, Result = result };

    public static ResumeAttemptResult Failed(
        ResumeFailureKind kind,
        ResumeFailureReason reason,
        int? retryAfterMs = null) =>
        new()
        {
            Success = false,
            FailureKind = kind,
            FailureReason = reason,
            RetryAfterMs = retryAfterMs
        };
}

/// <summary>
/// P1-C：<see cref="SessionLifecycleCoordinator.OnAuthenticatedAsync"/> 的统一返回类型。
/// <para>
/// 成功时 <see cref="Success"/>=true 且 <see cref="ResumeToken"/> 为颁发的 token（可能为 null）；
/// 失败时 <see cref="Success"/>=false 且 <see cref="FailureKind"/> 指示失败种类，
/// 调用方据此发送 <see cref="AuthenticationResponse"/>(Success=false) 并关闭连接。
/// </para>
/// </summary>
internal sealed class AuthLifecycleResult
{
    public bool Success { get; init; }

    /// <summary>成功时颁发的 ResumeToken；未启用 Resume 或颁发失败时为 null。</summary>
    public string? ResumeToken { get; init; }

    /// <summary>失败种类（仅 <see cref="Success"/>=false 时有意义）。</summary>
    public AuthFailureKind FailureKind { get; init; }

    /// <summary>依赖不可用时的重试退避建议（毫秒）；仅 DependencyUnavailable 时有意义。</summary>
    public int? RetryAfterMs { get; init; }

    public static AuthLifecycleResult Succeeded(string? resumeToken) =>
        new() { Success = true, ResumeToken = resumeToken };

    public static AuthLifecycleResult Failed(
        AuthFailureKind kind,
        int? retryAfterMs = null) =>
        new()
        {
            Success = false,
            FailureKind = kind,
            RetryAfterMs = retryAfterMs
        };
}

/// <summary>
/// P1-C：Authentication 路径失败种类。
/// 与 <see cref="ResumeFailureKind"/> 对应，但 Auth 路径目前仅区分依赖不可用
/// （Token 本身无效在调用 <see cref="SessionLifecycleCoordinator.OnAuthenticatedAsync"/>
/// 之前已由 <c>IRealtimeAuthenticator.AuthenticateAsync</c> 拦截）。
/// </summary>
internal enum AuthFailureKind : byte
{
    /// <summary>未失败（成功）。</summary>
    None = 0,

    /// <summary>
    /// 依赖不可用（Redis 异常、熔断器开路、TakeOver 不可用）。
    /// 客户端可按 <see cref="AuthLifecycleResult.RetryAfterMs"/> 退避后重新认证。
    /// </summary>
    DependencyUnavailable = 1,

    /// <summary>三-3：账号已被冻结，认证拒绝。不可重试。</summary>
    UserFrozen = 2
}

/// <summary>
/// P1-B：<see cref="ResumeFailureReason"/> → <see cref="ResumeFailureKind"/> 映射扩展。
/// <list type="bullet">
/// <item><see cref="ResumeFailureReason.InvalidToken"/> / <see cref="ResumeFailureReason.LeaseMismatch"/>
///   → <see cref="ResumeFailureKind.InvalidToken"/>（不可恢复，客户端必须完整认证）</item>
/// <item><see cref="ResumeFailureReason.RedisFailure"/> / <see cref="ResumeFailureReason.CircuitOpen"/>
///   / <see cref="ResumeFailureReason.LeaseQueryFailed"/> / <see cref="ResumeFailureReason.TakeOverUnavailable"/>
///   → <see cref="ResumeFailureKind.DependencyUnavailable"/>（可重试）</item>
/// </list>
/// </summary>
internal static class ResumeFailureReasonExtensions
{
    /// <summary>将内部失败原因映射为客户端可见的失败种类。</summary>
    public static ResumeFailureKind ToFailureKind(this ResumeFailureReason reason) => reason switch
    {
        ResumeFailureReason.InvalidToken => ResumeFailureKind.InvalidToken,
        ResumeFailureReason.LeaseMismatch => ResumeFailureKind.InvalidToken,
        ResumeFailureReason.RedisFailure => ResumeFailureKind.DependencyUnavailable,
        ResumeFailureReason.CircuitOpen => ResumeFailureKind.DependencyUnavailable,
        ResumeFailureReason.LeaseQueryFailed => ResumeFailureKind.DependencyUnavailable,
        ResumeFailureReason.TakeOverUnavailable => ResumeFailureKind.DependencyUnavailable,
        ResumeFailureReason.UserFrozen => ResumeFailureKind.UserFrozen,
        _ => ResumeFailureKind.InvalidToken
    };

    /// <summary>将失败种类映射为协议错误码，供 Error 帧使用。</summary>
    public static ProtocolErrorCode ToErrorCode(this ResumeFailureKind kind) => kind switch
    {
        ResumeFailureKind.InvalidToken => ProtocolErrorCode.ResumeFailed,
        ResumeFailureKind.DependencyUnavailable => ProtocolErrorCode.DependencyUnavailable,
        ResumeFailureKind.UserFrozen => ProtocolErrorCode.AccountSuspended,
        _ => ProtocolErrorCode.ResumeFailed
    };
}
