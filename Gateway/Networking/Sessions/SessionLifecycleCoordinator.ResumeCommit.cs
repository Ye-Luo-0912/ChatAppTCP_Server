using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Integration.Ephemeral;
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
    /// P0-3：改用 Claim/Commit/Release 原子模式替代破坏性 GETDEL。
    /// Prepare 调用 <see cref="IResumeTokenStore.TryClaimAsync"/> 原子占用 Token（不删除原 Key），
    /// Commit 成功后调用 <see cref="IResumeTokenStore.CommitClaimAsync"/> 最终消费；
    /// Commit 失败（Abort）调用 <see cref="IResumeTokenStore.ReleaseClaimAsync"/> 归还 Token。
    /// </para>
    /// <para>
    /// 执行步骤：
    /// <list type="number">
    /// <item>熔断器检查（快速失败，不发起 Redis 调用）</item>
    /// <item>ResumeToken Claim（Redis 原子 Lua）→ 获取 <see cref="ResumeContext"/> + attemptId</item>
    /// <item>设备租约代次校验（Redis 读，受 <see cref="TcpGatewayOptions.ResumeRedisFailMode"/> 控制）</item>
    /// </list>
    /// </para>
    /// <para>
    /// 成功时返回 <see cref="ResumePrepareResult.Succeeded"/>，携带 <see cref="PreparedResumeContext"/>
    /// （含 attemptId）供 Commit/Abort 使用。失败时返回 <see cref="ResumePrepareResult.Failed"/>，
    /// 携带失败种类与原因。Claim 失败时 Token 已被占用，需通过 Release 归还。
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

        // P0-3：原子 Claim 替代破坏性 GETDEL。
        // Claim 占用 Token（GETDEL 原 Key + SET claim Key），但不删除——
        // Commit 成功后 CommitClaim 才最终消费；Abort 时 ReleaseClaim 归还 Token。
        ResumeClaimResult? claim;
        try
        {
            claim = await _resumeTokenStore
                .TryClaimAsync(resumeToken, cancellationToken)
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

        if (claim is null)
        {
            _metrics.ResumeFailed(ResumeFailureReason.InvalidToken);
            return ResumePrepareResult.Failed(
                ResumeFailureKind.InvalidToken,
                ResumeFailureReason.InvalidToken);
        }

        var context = claim.Context;
        var attemptId = claim.AttemptId;

        // P0-3 缺口3：构造 prepared 上下文，供后续失败路径调用 ReleaseClaimSafeAsync 归还 Token。
        var prepared = new PreparedResumeContext
        {
            Context = context,
            AttemptId = attemptId,
            ResumeToken = resumeToken
        };

        // P0-3：冻结用户拒绝 Resume —— 权威生命周期校验（fail-closed）。
        // 1) 快速路径：缓存命中冻结 → 立即拒绝，避免不必要的 NATS 往返。
        // 2) 权威校验：查询 Server 获取用户当前生命周期状态。
        //    - 查询返回 Frozen → 拒绝（UserFrozen）。
        //    - 查询失败（依赖不可用）→ fail-closed 拒绝 Resume，避免
        //      "缓存未命中 + 后台刷新" 的 fail-open 窗口被冻结用户滥用。
        //    仅在缓存未标记冻结且权威查询成功且返回 Active 时才放行。
        if (_frozenUserCache is not null && _frozenUserCache.IsFrozen(context.UserId))
        {
            _metrics.ResumeFailed(ResumeFailureReason.UserFrozen);
            // 归还 Claim：不留 dangling claim Key，依赖 TTL 兜底。
            await ReleaseClaimSafeAsync(prepared, cancellationToken)
                .ConfigureAwait(false);
            return ResumePrepareResult.Failed(
                ResumeFailureKind.UserFrozen,
                ResumeFailureReason.UserFrozen);
        }

        var lifecycleDecision = await AuthoritativeLifecycleCheckAsync(
                context.UserId, cancellationToken)
            .ConfigureAwait(false);
        if (lifecycleDecision != ResumeLifecycleDecision.Allow)
        {
            // 归还 Claim：拒绝 Resume 前必须释放 Token，允许客户端走完整认证或重试。
            await ReleaseClaimSafeAsync(prepared, cancellationToken)
                .ConfigureAwait(false);
            if (lifecycleDecision == ResumeLifecycleDecision.Frozen)
            {
                _metrics.ResumeFailed(ResumeFailureReason.UserFrozen);
                return ResumePrepareResult.Failed(
                    ResumeFailureKind.UserFrozen,
                    ResumeFailureReason.UserFrozen);
            }
            // 依赖不可用：fail-closed 拒绝 Resume，客户端退避后重试或走完整认证。
            _metrics.ResumeFailed(ResumeFailureReason.LifecycleUnavailable);
            return ResumePrepareResult.Failed(
                ResumeFailureKind.DependencyUnavailable,
                ResumeFailureReason.LifecycleUnavailable,
                RetryAfterMsForDependency);
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
                    // P0-3 缺口3：Claim 已成功，失败退出前必须归还 Token，否则卡死至 claim TTL 过期。
                    await ReleaseClaimSafeAsync(prepared, cancellationToken)
                        .ConfigureAwait(false);
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
                // P0-3 缺口3：Claim 已成功，拒绝恢复前必须归还 Token，允许客户端重试。
                await ReleaseClaimSafeAsync(prepared, cancellationToken)
                    .ConfigureAwait(false);
                return ResumePrepareResult.Failed(
                    ResumeFailureKind.InvalidToken,
                    ResumeFailureReason.LeaseMismatch);
            }
        }

        return ResumePrepareResult.Succeeded(prepared);
    }

    /// <summary>
    /// P1-D：Commit 阶段——执行状态变更。
    /// <para>
    /// 执行步骤（有序）：
    /// <list type="number">
    /// <item><c>session.Authenticate</c> — 复用原身份</item>
    /// <item><c>_userSessions.Add</c> + Presence 上线广播</item>
    /// <item>Redis 设备租约接管（<c>TakeOverAsync</c>）— P0-5 前置于本机旧连接踢下线，
    ///   若 TakeOver 失败本机旧连接仍存活，避免用户失去所有连接</item>
    /// <item>本机旧连接踢下线（<c>TakeOverSameDevice</c> + <c>RevokeSessionAsync</c>）</item>
    /// <item>跨 Gateway SessionRevoked 广播（仅当旧 TransportId 不同且非本机已踢）</item>
    /// <item>新 ResumeToken 颁发（<c>IssueAsync</c>）</item>
    /// <item>同步水位查询（<c>QueryResumeWatermarkAsync</c>，best-effort）</item>
    /// <item>P0-3：<c>CommitClaimSafeAsync</c> 最终消费原 Token</item>
    /// </list>
    /// </para>
    /// <para>
    /// 步骤 3（TakeOver）失败时先调用 <see cref="ReleaseClaimSafeAsync"/> 归还 Token
    /// （允许客户端重试 Resume），再调用 <see cref="AbortLocalStateAsync"/> 回滚步骤 1-2，
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
        if (_userSessions.Add(session))
        {
            await UpdateGlobalPresenceAsync(context.UserId, isOnline: true, cancellationToken)
                .ConfigureAwait(false);
        }

        // 步骤 3：Redis 设备租约接管（P0-5：前置于本机旧连接踢下线）。
        // 仅当原会话携带 DeviceIdHash 时才接管。缺少 DeviceIdHash 的会话不持有设备租约。
        // 若 TakeOver 失败，本机旧连接仍存活，避免用户失去所有连接。
        TakeOverResult? takeoverResult = null;
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
            takeoverResult = takeover;

            if (takeover.Status == TakeOverStatus.DependencyUnavailable)
            {
                // Fail-closed：TakeOver 依赖不可用时拒绝恢复，要求完整认证。
                _logger.TransportFailed(
                    GatewayTransportOperation.ClientProcessing,
                    session.ConnectionId,
                    takeover.Exception ?? new InvalidOperationException("TakeOver dependency unavailable"));
                _metrics.ResumeFailed(ResumeFailureReason.TakeOverUnavailable);
                // P0-3：Release 归还 Token，允许客户端重试 Resume。
                await ReleaseClaimSafeAsync(prepared, cancellationToken)
                    .ConfigureAwait(false);
                // P1-D：Abort 回滚步骤 1-2（Authenticate + Add + Presence）。
                await AbortLocalStateAsync(session, context.UserId, cancellationToken)
                    .ConfigureAwait(false);
                session.Close(SessionCloseReason.AuthenticationRejected);
                return ResumeAttemptResult.Failed(
                    ResumeFailureKind.DependencyUnavailable,
                    ResumeFailureReason.TakeOverUnavailable,
                    RetryAfterMsForDependency);
            }
        }

        // 步骤 4-8：本机旧连接踢下线、跨 Gateway 广播、Token 颁发、水位查询、Commit Claim。
        // 主线二子项4：TakeOver 成功后若后续步骤因未预期异常失败，主动释放新 lease
        // （而非仅依赖 OnDisconnectedAsync 兜底），避免 lease 残留窗口。
        TakeOverResult? capturedTakeover = takeoverResult;
        ulong? capturedDeviceHash = context.DeviceIdHash;
        try
        {
        // 步骤 4：本机旧连接立即踢下线（P0-5：移至 TakeOver 之后）。
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

        // 步骤 5：跨 Gateway SessionRevoked 广播（仅当旧 TransportId 不同且非本机已踢）。
        if (takeoverResult is { } tr
            && tr.HasPreviousLease
            && !string.Equals(
                tr.PreviousTransportId,
                session.ConnectionLeaseId,
                StringComparison.Ordinal)
            && !localVictims.Any(v =>
                string.Equals(v.ConnectionLeaseId, tr.PreviousTransportId!, StringComparison.Ordinal)))
        {
            var occurredAtMs = _timeProvider
                .GetUtcNow()
                .ToUnixTimeMilliseconds();
            await PublishSessionRevokedEventAsync(
                context.UserId,
                !string.IsNullOrWhiteSpace(tr.PreviousSessionId) ? tr.PreviousSessionId! : context.SessionId!,
                occurredAtMs,
                session.ConnectionId,
                tr.PreviousTransportId,
                cancellationToken).ConfigureAwait(false);
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

        // P0-3：Commit Claim 最终消费原 Token。
        await CommitClaimSafeAsync(prepared, cancellationToken)
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // 停机取消：正常传播，不视为 Commit 失败。lease 由 OnDisconnectedAsync 兜底。
            throw;
        }
        catch (Exception ex)
        {
            // 主线二子项4：TakeOver 成功但后续步骤失败，主动释放新 lease。
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                ex);
            _metrics.ResumeFailed(ResumeFailureReason.TakeOverUnavailable);

            // 主动释放新 lease（若 TakeOver 成功且携带 DeviceIdHash）。
            if (capturedTakeover is not null && capturedDeviceHash is { } dh)
            {
                try
                {
                    await _deviceSessionLeaseStore
                        .ReleaseIfOwnerAsync(
                            context.UserId,
                            dh,
                            session.LeaseOwnerToken,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                catch
                {
                    // 释放失败不阻塞 fail-closed 路径，lease 由 TTL 兜底过期。
                }
            }

            await ReleaseClaimSafeAsync(prepared, cancellationToken)
                .ConfigureAwait(false);
            await AbortLocalStateAsync(session, context.UserId, cancellationToken)
                .ConfigureAwait(false);
            session.Close(SessionCloseReason.AuthenticationRejected);
            return ResumeAttemptResult.Failed(
                ResumeFailureKind.DependencyUnavailable,
                ResumeFailureReason.TakeOverUnavailable,
                RetryAfterMsForDependency);
        }
    }

    /// <summary>
    /// P0-3：安全调用 CommitClaimAsync，吞掉异常（claim Key 依赖 TTL 过期兜底）。
    /// <para>
    /// P0-3 缺口4：检查 CommitClaimAsync 返回值，false 时记 Warning 日志。
    /// Commit Claim 失败（attemptId 不匹配或 claim Key 已过期）不抛异常——
    /// 原 Token 已被 GETDEL 删除不会复活，claim Key 依赖 TTL 过期兜底。
    /// </para>
    /// </summary>
    private async Task CommitClaimSafeAsync(
        PreparedResumeContext prepared,
        CancellationToken cancellationToken)
    {
        if (_resumeTokenStore is null || string.IsNullOrEmpty(prepared.AttemptId))
            return;

        try
        {
            var committed = await _resumeTokenStore
                .CommitClaimAsync(prepared.ResumeToken, prepared.AttemptId, cancellationToken)
                .ConfigureAwait(false);
            if (!committed)
            {
                // P0-3 缺口4：CommitClaim 返回 false 不可观测会导致 claim 残留静默泄漏。
                // 记 Warning：attemptId 不匹配或 claim Key 已过期（超 10s 窗口）。
                _logger.DependencyOperationFailed(
                    GatewayDependency.Redis,
                    GatewayDependencyOperation.ResumeTokenRevoke,
                    new InvalidOperationException(
                        "CommitClaim returned false: attemptId mismatch or claim key expired."));
            }
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.ResumeTokenLookup,
                ex);
        }
    }

    /// <summary>
    /// P0-3：安全调用 ReleaseClaimAsync，吞掉异常（claim Key 依赖 TTL 过期兜底）。
    /// </summary>
    private async Task ReleaseClaimSafeAsync(
        PreparedResumeContext prepared,
        CancellationToken cancellationToken)
    {
        if (_resumeTokenStore is null || string.IsNullOrEmpty(prepared.AttemptId))
            return;

        try
        {
            await _resumeTokenStore
                .ReleaseClaimAsync(prepared.ResumeToken, prepared.AttemptId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.ResumeTokenLookup,
                ex);
        }
    }

    /// <summary>
    /// P0-3：权威生命周期校验。查询 Server 获取用户当前生命周期状态，fail-closed。
    /// <para>
    /// 返回 <see cref="ResumeLifecycleDecision.Allow"/> 仅当查询成功且用户未被冻结。
    /// 查询失败（NATS 依赖不可用）返回 <see cref="ResumeLifecycleDecision.Unavailable"/>，
    /// 由调用方 fail-closed 拒绝 Resume，杜绝"缓存未命中即放行"的 fail-open 窗口。
    /// </para>
    /// </summary>
    private async Task<ResumeLifecycleDecision> AuthoritativeLifecycleCheckAsync(
        long userId,
        CancellationToken cancellationToken)
    {
        try
        {
            var response = await _messageBus
                .QueryUserLifecycleAsync(
                    new UserLifecycleQuery { UserId = userId },
                    cancellationToken)
                .ConfigureAwait(false);

            if (response.State == UserLifecycleState.Frozen)
            {
                // 权威确认冻结：同步预热缓存，避免后续请求重复走 NATS 往返。
                _frozenUserCache?.MarkFrozen(
                    userId,
                    _timeProvider.GetUtcNow().ToUnixTimeMilliseconds());
                return ResumeLifecycleDecision.Frozen;
            }

            return ResumeLifecycleDecision.Allow;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.DependencyOperationFailed(
                GatewayDependency.RealtimeService,
                GatewayDependencyOperation.ResumeTokenLookup,
                ex);
            return ResumeLifecycleDecision.Unavailable;
        }
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
    /// <item>UpdateGlobalPresenceAsync(isOnline: false)（撤销全局在线路由租约与 Presence 上线，
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
    /// <item>本机旧连接踢下线（步骤 4）：不撤回（旧连接本就应被关闭）。</item>
    /// </list>
    /// </para>
    /// </summary>
    private async Task AbortLocalStateAsync(
        TcpClientSession session,
        long userId,
        CancellationToken cancellationToken)
    {
        var removedFromRegistry = _userSessions.Remove(session);
        if (removedFromRegistry)
        {
            try
            {
                await UpdateGlobalPresenceAsync(userId, isOnline: false, cancellationToken)
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

    /// <summary>
    /// P0-3：Claim 返回的 attemptId，用于 Commit/Release 时验证 Token 所有权。
    /// </summary>
    public string AttemptId { get; init; } = string.Empty;

    /// <summary>
    /// P0-3：客户端提交的原始 ResumeToken，用于 Commit/Release 调用。
    /// </summary>
    public string ResumeToken { get; init; } = string.Empty;
}

/// <summary>
/// P0-3：权威生命周期校验的决策结果。
/// </summary>
internal enum ResumeLifecycleDecision
{
    /// <summary>未尝试/不适用（仅占位）。</summary>
    None = 0,

    /// <summary>查询成功且用户未被冻结，允许 Resume。</summary>
    Allow = 1,

    /// <summary>权威查询确认用户已冻结，拒绝 Resume。</summary>
    Frozen = 2,

    /// <summary>权威查询依赖不可用（NATS 到 Server 失败），fail-closed 拒绝 Resume。</summary>
    Unavailable = 3,
}
