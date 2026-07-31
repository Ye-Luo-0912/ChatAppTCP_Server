using ChatApp.TcpGateway.Core.Authentication;

namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 断线重连 ResumeToken 存储。
/// 存储会话关键信息（UserId、SessionId、DeviceId、ConnectionLeaseId），
/// 客户端断线后短时间内可凭 ResumeToken 恢复会话，无需重新认证。
/// <para>
/// P0-3：提供 Claim/Commit/Release 原子模式替代破坏性 GETDEL：
/// <list type="number">
/// <item><see cref="TryClaimAsync"/>：原子占用 Token（不删除），返回上下文与 attemptId。</item>
/// <item><see cref="CommitClaimAsync"/>：Commit 成功后删除 Token（最终消费）。</item>
/// <item><see cref="ReleaseClaimAsync"/>：Abort 时归还 Token，允许客户端重试。</item>
/// </list>
/// 默认实现回退到旧 GETDEL 行为（TryValidateAsync），保持未实现新模式的存储兼容。
/// </para>
/// </summary>
public interface IResumeTokenStore
{
    /// <summary>
    /// 颁发新的 ResumeToken 并存储会话信息。
    /// </summary>
    /// <param name="context">会话上下文（UserId、SessionId、DeviceId、ConnectionLeaseId）。</param>
    /// <param name="ttl">Token 有效期。超时后自动失效。</param>
    /// <param name="ct">取消令牌。</param>
    /// <returns>32 字符十六进制的 ResumeToken。</returns>
    Task<string> IssueAsync(ResumeContext context, TimeSpan ttl, CancellationToken ct = default);

    /// <summary>
    /// 校验并消费 ResumeToken，返回会话上下文。
    /// <para>
    /// <b>已弃用</b>：此方法使用破坏性 GETDEL，Prepare 阶段调用会导致 Commit 失败时
    /// Token 不可恢复。新代码应使用 <see cref="TryClaimAsync"/> + Commit/Release 模式。
    /// 保留仅供未实现新模式的存储与旧测试兼容。
    /// </para>
    /// </summary>
    /// <param name="resumeToken">客户端提交的 ResumeToken。</param>
    /// <param name="ct">取消令牌。</param>
    /// <returns>会话上下文；Token 无效或已过期返回 null。</returns>
    Task<ResumeContext?> TryValidateAsync(string resumeToken, CancellationToken ct = default);

    /// <summary>
    /// 撤销 ResumeToken（会话主动登出或被吊销时调用）。
    /// </summary>
    Task RevokeAsync(string resumeToken, CancellationToken ct = default);

    /// <summary>
    /// P0-3：原子占用 ResumeToken（不删除原 Key），返回上下文与 attemptId。
    /// <para>
    /// 调用方在 Prepare 阶段调用，获得 <see cref="ResumeClaimResult"/> 后进入 Commit。
    /// Commit 成功调用 <see cref="CommitClaimAsync"/> 最终消费 Token；
    /// Commit 失败（Abort）调用 <see cref="ReleaseClaimAsync"/> 归还 Token，允许客户端重试。
    /// </para>
    /// <para>
    /// 默认实现回退到 <see cref="TryValidateAsync"/>（GETDEL），不保证 Abort 可重试——
    /// 未实现新模式的存储应尽快升级。
    /// </para>
    /// </summary>
    /// <param name="resumeToken">客户端提交的 ResumeToken。</param>
    /// <param name="ct">取消令牌。</param>
    /// <returns>占用结果（含上下文与 attemptId）；Token 无效返回 null。</returns>
    Task<ResumeClaimResult?> TryClaimAsync(string resumeToken, CancellationToken ct = default)
        => DefaultTryClaimAsync(resumeToken, ct);

    /// <summary>
    /// P0-3：Commit 成功后最终消费已占用的 Token。
    /// <para>
    /// 默认实现为 no-op（旧 GETDEL 模式下 Token 已在 Claim 时删除）。
    /// </para>
    /// </summary>
    /// <param name="resumeToken">已占用的 ResumeToken。</param>
    /// <param name="attemptId">Claim 返回的 attemptId，用于验证所有权。</param>
    /// <param name="ct">取消令牌。</param>
    /// <returns>是否成功消费（attemptId 不匹配或 claim 已过期返回 false）。</returns>
    Task<bool> CommitClaimAsync(string resumeToken, string attemptId, CancellationToken ct = default)
        => Task.FromResult(true);

    /// <summary>
    /// P0-3：Abort 时归还已占用的 Token，允许客户端重试。
    /// <para>
    /// 默认实现为 no-op（旧 GETDEL 模式下 Token 已被消费，无法恢复）。
    /// </para>
    /// </summary>
    /// <param name="resumeToken">已占用的 ResumeToken。</param>
    /// <param name="attemptId">Claim 返回的 attemptId，用于验证所有权。</param>
    /// <param name="ct">取消令牌。</param>
    Task ReleaseClaimAsync(string resumeToken, string attemptId, CancellationToken ct = default)
        => Task.CompletedTask;

    /// <summary>
    /// 默认 TryClaim 实现：回退到破坏性 TryValidateAsync。
    /// attemptId 设为空串，Commit/Release 默认 no-op，与旧行为一致（Abort 不可重试）。
    /// </summary>
    private async Task<ResumeClaimResult?> DefaultTryClaimAsync(
        string resumeToken, CancellationToken ct)
    {
        var context = await TryValidateAsync(resumeToken, ct).ConfigureAwait(false);
        if (context is null)
            return null;
        return new ResumeClaimResult
        {
            Context = context,
            AttemptId = string.Empty
        };
    }
}

/// <summary>
/// P0-3：ResumeToken 占用结果。
/// </summary>
public sealed class ResumeClaimResult
{
    /// <summary>从 Token 恢复的会话上下文。</summary>
    public required ResumeContext Context { get; init; }

    /// <summary>
    /// 占用凭证，用于 Commit/Release 时验证所有权。
    /// 实现可选（默认空串表示 GETDEL 兼容模式）。
    /// </summary>
    public string AttemptId { get; init; } = string.Empty;
}

/// <summary>
/// ResumeToken 关联的会话上下文。
/// </summary>
public sealed class ResumeContext
{
    public required long UserId { get; init; }
    public required string SessionId { get; init; }
    public required string ConnectionLeaseId { get; init; }
    public string? DeviceId { get; init; }
    public ulong? DeviceIdHash { get; init; }
}
