using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// P1-D：Resume 两阶段提交（Prepare / Commit / Abort）。
/// <para>
/// 将原 <see cref="SessionLifecycleCoordinator.TryResumeAsync"/> 的单阶段流程拆分为：
/// <list type="number">
/// <item><term>Prepare</term>
///   <description>只读校验：熔断器、ResumeToken 验证、设备租约代次校验。
///   无可观测副作用（不注册会话、不广播 Presence、不接管租约）。
///   失败时直接返回 <see cref="ResumeAttemptResult"/>，无需 Abort。</description></item>
/// <item><term>Commit</term>
///   <description>执行状态变更：会话注册、Presence 上线、本机旧连接踢下线、
///   Redis 设备租约接管、跨 Gateway SessionRevoked 广播、新 ResumeToken 颁发、水位查询。
///   任一步骤失败时调用 <see cref="AbortLocalStateAsync"/> 回滚已完成的本地状态变更。</description></item>
/// <item><term>Abort</term>
///   <description>回滚 Commit 阶段已完成的部分本地状态：UserSessionRegistry.Remove、
///   Presence 下线广播。Redis 租约/Token 颁发等外部副作用依赖 TTL 或后续 TakeOver 自然收敛。</description></item>
/// </list>
/// </para>
/// <para>
/// 设计目标：
/// <list type="bullet">
/// <item>明确分离只读校验与状态变更，便于测试与未来跨 Gateway 协调；</item>
/// <item>Commit 失败时保证本地状态一致性（无半提交会话残留）；</item>
/// <item>Abort 仅回滚本地可逆状态，外部副作用（Redis/NATS）依赖幂等性与 TTL。</item>
/// </list>
/// </para>
/// </summary>
internal sealed partial class SessionLifecycleCoordinator
{
    /// <summary>
    /// P1-D：Prepare 阶段——只读校验，无可观测副作用。
    /// <para>
    /// 执行步骤：
    /// <list type="number">
    /// <item>熔断器检查（快速失败，不发起 Redis 调用）</item>
    /// <item>ResumeToken 验证（Redis 读）→ 获取 <see cref="ResumeContext"/></item>
    /// <item>设备租约代次校验（Redis 读，受 <see cref="TcpGatewayOptions.ResumeRedisFailMode"/> 控制）</item>
    /// </list>
    /// </para>
    /// <para>
    /// 成功时返回 <see cref="ResumePrepareResult.Succeeded"/>，携带 <see cref="PreparedResumeContext"/>
    /// 供 Commit 阶段使用。失败时返回 <see cref="ResumePrepareResult.Failed"/>，携带失败种类与原因。
    /// </para>
    /// </summary>
    /// <param name="resumeToken">客户端提供的 ResumeToken。</param>
    /// <param name="session">待恢复的会话（尚未认证）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>Prepare 结果；<c>null</c> 表示未尝试（store 未注入）。</returns>
    private async Task<ResumePrepareResult?> PrepareResumeAsync(
        string resumeToken,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        if (_resumeTokenStore is null)
            return null;

        // 熔断器开路：直接快速失败，不发起 Redis 调用，避免重连风暴串行排队。
        if (_circuitBreaker is { IsAvailable: false })
        {
            _metrics.RedisCircuitBreakerOpen();
            _metrics.ResumeFailed(ResumeFailureReason.CircuitOpen);
            return ResumePrepareResult.Failed(
                ResumeFailureKind.DependencyUnavailable,
                ResumeFailureReason.CircuitOpen,
                RetryAfterMsForDependency);
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
            return ResumePrepareResult.Failed(
                ResumeFailureKind.DependencyUnavailable,
                ResumeFailureReason.RedisFailure,
                RetryAfterMsForDependency);
        }

        if (context is null)
        {
            _metrics.ResumeFailed(ResumeFailureReason.InvalidToken);
            return ResumePrepareResult.Failed(
                ResumeFailureKind.InvalidToken,
                ResumeFailureReason.InvalidToken);
        }

        // 代次校验：若待恢复会话携带 DeviceIdHash，查询当前设备租约持有者。
        // 若租约已被另一个 SessionId 接管，说明此 ResumeContext 来自已被替换的旧会话，拒绝恢复。
        //
        // P1-C：fail-mode 可配置。
        // FailClosed（默认）：依赖不可用时拒绝恢复。Same-device fencing 属于安全不变量。
        // FailOpen：依赖不可用时跳过代次校验，继续恢复。
        if (context.DeviceIdHash is { } resumeDeviceHash
            && !string.IsNullOrWhiteSpace(context.SessionId))
        {
            // null 表示未查询到租约或 FailOpen 跳过查询：后续代次校验判断 not null 才执行。
            string? currentLeaseSessionId = null;
            try
            {
                currentLeaseSessionId = await _deviceSessionLeaseStore
                    .GetCurrentSessionIdAsync(
                        context.UserId,
                        resumeDeviceHash,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.DependencyOperationFailed(
                    GatewayDependency.Redis,
                    GatewayDependencyOperation.DeviceLeaseQuery,
                    ex);
                _metrics.ResumeFailed(ResumeFailureReason.LeaseQueryFailed);
                if (_options.ResumeRedisFailMode == RedisFailMode.FailOpen)
                {
                    // FailOpen：跳过代次校验，继续恢复。安全 fencing 可能被绕过。
                    _logger.TransportFailed(
                        GatewayTransportOperation.ClientProcessing,
                        session.ConnectionId,
                        new InvalidOperationException(
                            "Resume proceeding despite lease query failure (FailOpen).", ex));
                }
                else
                {
                    // FailClosed：依赖不可用时拒绝恢复，要求完整认证。
                    return ResumePrepareResult.Failed(
                        ResumeFailureKind.DependencyUnavailable,
                        ResumeFailureReason.LeaseQueryFailed,
                        RetryAfterMsForDependency);
                }
            }

            if (currentLeaseSessionId is not null
                && !string.IsNullOrWhiteSpace(currentLeaseSessionId)
                && !string.Equals(
                    currentLeaseSessionId,
                    context.SessionId,
                    StringComparison.Ordinal))
            {
                // 设备租约已归属另一个更新的会话，拒绝恢复旧会话（安全不变量）。
                _logger.TransportFailed(
                    GatewayTransportOperation.ClientProcessing,
                    session.ConnectionId,
                    new InvalidOperationException(
                        "Resume rejected: device lease owned by a newer session."));
                _metrics.ResumeFailed(ResumeFailureReason.LeaseMismatch);
                return ResumePrepareResult.Failed(
                    ResumeFailureKind.InvalidToken,
                    ResumeFailureReason.LeaseMismatch);
            }
        }

        return ResumePrepareResult.Succeeded(new PreparedResumeContext
        {
            Context = context
        });
    }

    /// <summary>
    /// P1-D：Commit 阶段——执行状态变更。
    /// <para>
    /// 执行步骤（有序）：
    /// <list type="number">
    /// <item><c>session.Authenticate</c> — 复用原身份</item>
    /// <item><c>_userSessions.Add</c> + Presence 上线广播</item>
    /// <item>本机旧连接踢下线（<c>TakeOverSameDevice</c> + <c>RevokeSessionAsync</c>）</item>
    /// <item>Redis 设备租约接管（<c>TakeOverAsync</c>）</item>
    /// <item>跨 Gateway SessionRevoked 广播（仅当旧 TransportId 不同且非本机）</item>
    /// <item>新 ResumeToken 颁发（<c>IssueAsync</c>）</item>
    /// <item>同步水位查询（<c>QueryResumeWatermarkAsync</c>，best-effort）</item>
    /// </list>
    /// </para>
    /// <para>
    /// 步骤 4（TakeOver）失败时调用 <see cref="AbortLocalStateAsync"/> 回滚步骤 1-2，
    /// 关闭连接并返回 <see cref="ResumeFailureKind.DependencyUnavailable"/>。
    /// 步骤 6（Token 颁发）失败不阻断恢复（best-effort，客户端下次重连走完整认证）。
    /// </para>
    /// </summary>
    private async Task<ResumeAttemptResult> CommitResumeAsync(
        PreparedResumeContext prepared,
        TcpClientSession session,
        CancellationToken cancellationToken)
    {
        var context = prepared.Context;

        // 步骤 1：复用原身份认证。
        session.Authenticate(
            context.UserId,
            context.SessionId,
            context.DeviceIdHash,
            context.DeviceId);

        // 步骤 2：注册会话 + Presence 上线广播。
        if (_userSessions.Add(session) && _options.EnableEphemeralPresenceAndTyping)
        {
            await PublishPresenceChangedAsync(context.UserId, isOnline: true, cancellationToken)
                .ConfigureAwait(false);
        }

        // 步骤 3：本机旧连接立即踢下线。
        // Resume 复用原 SessionId，旧连接按 ConnectionLeaseId 区分直接关闭，
        // 而非依赖 NATS SessionRevoked 事件往返。
        var localVictims = _userSessions.TakeOverSameDevice(session);
        if (localVictims.Length > 0)
        {
            var localOccurredAtMs = _timeProvider
                .GetUtcNow()
                .ToUnixTimeMilliseconds();
            foreach (var victim in localVictims)
                await RevokeSessionAsync(victim, localOccurredAtMs, cancellationToken)
                    .ConfigureAwait(false);
        }

        // 步骤 4：Redis 设备租约接管。
        // 仅当原会话携带 DeviceIdHash 时才接管。缺少 DeviceIdHash 的会话不持有设备租约。
        if (context.DeviceIdHash is { } deviceHash)
        {
            var takeover = await _deviceSessionLeaseStore.TakeOverAsync(
                context.UserId,
                deviceHash,
                context.SessionId,
                session.ConnectionLeaseId,
                session.LeaseOwnerToken,
                _options.IdleTimeout + TimeSpan.FromMinutes(5),
                cancellationToken).ConfigureAwait(false);

            if (takeover.Status == TakeOverStatus.DependencyUnavailable)
            {
                // Fail-closed：TakeOver 依赖不可用时拒绝恢复，要求完整认证。
                _logger.TransportFailed(
                    GatewayTransportOperation.ClientProcessing,
                    session.ConnectionId,
                    takeover.Exception ?? new InvalidOperationException("TakeOver dependency unavailable"));
                _metrics.ResumeFailed(ResumeFailureReason.TakeOverUnavailable);
                // P1-D：Abort 回滚步骤 1-2（Authenticate + Add + Presence）。
                await AbortLocalStateAsync(session, context.UserId, cancellationToken)
                    .ConfigureAwait(false);
                session.Close(SessionCloseReason.AuthenticationRejected);
                return ResumeAttemptResult.Failed(
                    ResumeFailureKind.DependencyUnavailable,
                    ResumeFailureReason.TakeOverUnavailable,
                    RetryAfterMsForDependency);
            }

            // 步骤 5：跨 Gateway SessionRevoked 广播（仅当旧 TransportId 不同且非本机已踢）。
            if (takeover.HasPreviousLease
                && !string.Equals(
                    takeover.PreviousTransportId,
                    session.ConnectionLeaseId,
                    StringComparison.Ordinal)
                && !localVictims.Any(v =>
                    string.Equals(v.ConnectionLeaseId, takeover.PreviousTransportId!, StringComparison.Ordinal)))
            {
                var occurredAtMs = _timeProvider
                    .GetUtcNow()
                    .ToUnixTimeMilliseconds();
                await PublishSessionRevokedEventAsync(
                    context.UserId,
                    !string.IsNullOrWhiteSpace(takeover.PreviousSessionId) ? takeover.PreviousSessionId! : context.SessionId!,
                    occurredAtMs,
                    session.ConnectionId,
                    takeover.PreviousTransportId,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        // 步骤 6：颁发新的 ResumeToken（旧 Token 已被消费）。
        // Prepare 阶段已校验 _resumeTokenStore 非空（null 时 Prepare 返回 null 不会进入 Commit）。
        string? newToken = null;
        if (_options.EnableResume && _resumeTokenStore is not null)
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
                // Token 颁发失败不阻断恢复：会话已注册，客户端下次重连走完整认证。
            }
        }

        // 步骤 7：查询服务端同步水位（best-effort，失败返回 null）。
        var lastConversationSequence = await QueryResumeWatermarkAsync(
                context.UserId,
                context.DeviceIdHash,
                cancellationToken)
            .ConfigureAwait(false);

        _metrics.ResumeSucceeded();

        return ResumeAttemptResult.Succeeded(
            new ResumeLifecycleResult
            {
                ResumeToken = newToken,
                UserId = context.UserId,
                SessionId = context.SessionId,
                DeviceId = context.DeviceId,
                LastConversationSequence = lastConversationSequence
            });
    }

    /// <summary>
    /// P1-D：Abort 阶段——回滚 Commit 阶段已完成的部分本地状态。
    /// <para>
    /// Auth 路径（<see cref="OnAuthenticatedAsync"/>）与 Resume 路径（<see cref="CommitResumeAsync"/>）
    /// 共用此方法回滚本地会话注册表与 Presence 广播。
    /// </para>
    /// <para>
    /// 回滚步骤（逆序）：
    /// <list type="number">
    /// <item>UserSessionRegistry.Remove（撤销 Add）</item>
    /// <item>PublishPresenceChangedAsync(isOnline: false)（撤销 Presence 上线，
    ///   仅当 Add 返回 true 时）</item>
    /// </list>
    /// </para>
    /// <para>
    /// 注意：session.Authenticate 设置的字段不回滚——连接即将关闭，
    /// 清理路径（<see cref="OnDisconnectedAsync"/>）按 UserId&gt;0 判定已认证，
    /// 但 Registry 已移除，清理不会重复移除或重复广播 Presence。
    /// </para>
    /// <para>
    /// 外部副作用（Redis 租约、NATS 广播）不回滚：
    /// <list type="bullet">
    /// <item>Redis 租约：若 TakeOver 已成功则保留（新会话拥有租约），连接关闭时由
    ///   <see cref="OnDisconnectedAsync"/> 的 ReleaseIfOwner 释放；</item>
    /// <item>NATS SessionRevoked：已广播的吊销事件不撤回（旧连接本就应被关闭）；</item>
    /// <item>本机旧连接踢下线（步骤 3）：不撤回（旧连接本就应被关闭）。</item>
    /// </list>
    /// </para>
    /// </summary>
    private async Task AbortLocalStateAsync(
        TcpClientSession session,
        long userId,
        CancellationToken cancellationToken)
    {
        var removedFromRegistry = _userSessions.Remove(session);
        if (removedFromRegistry && _options.EnableEphemeralPresenceAndTyping)
        {
            try
            {
                await PublishPresenceChangedAsync(userId, isOnline: false, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch
            {
                // 回滚失败不应阻塞 fail-closed 路径。Presence 下线丢失由 TTL 兜底。
            }
        }
    }
}

/// <summary>
/// P1-D：Prepare 阶段的结果。成功时携带 <see cref="PreparedResumeContext"/>，
/// 失败时携带失败种类与原因。
/// </summary>
internal sealed class ResumePrepareResult
{
    public bool Success { get; init; }

    /// <summary>成功时的 Prepare 上下文；失败时为 null。</summary>
    public PreparedResumeContext? Prepared { get; init; }

    /// <summary>失败种类（仅 <see cref="Success"/>=false 时有意义）。</summary>
    public ResumeFailureKind FailureKind { get; init; }

    /// <summary>内部失败原因明细（用于 metrics 归因）。</summary>
    public ResumeFailureReason? FailureReason { get; init; }

    /// <summary>依赖不可用时的重试退避建议（毫秒）。</summary>
    public int? RetryAfterMs { get; init; }

    public static ResumePrepareResult Succeeded(PreparedResumeContext prepared) =>
        new() { Success = true, Prepared = prepared };

    public static ResumePrepareResult Failed(
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

    /// <summary>
    /// 将 Prepare 失败转换为 <see cref="ResumeAttemptResult"/>（语义等价，仅类型转换）。
    /// 用于 <see cref="SessionLifecycleCoordinator.TryResumeAsync"/> 在 Prepare 失败时直接返回。
    /// </summary>
    public ResumeAttemptResult ToAttemptResult() => ResumeAttemptResult.Failed(
        FailureKind,
        FailureReason ?? ResumeFailureReason.InvalidToken,
        RetryAfterMs);
}

/// <summary>
/// P1-D：Prepare 阶段成功时携带的上下文，传递给 Commit 阶段使用。
/// </summary>
internal sealed class PreparedResumeContext
{
    /// <summary>从 ResumeToken 恢复的会话上下文（UserId/SessionId/DeviceId 等）。</summary>
    public required ResumeContext Context { get; init; }
}
