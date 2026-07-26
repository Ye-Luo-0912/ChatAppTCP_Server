using ChatApp.TcpGateway.Core.Authentication;

namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 断线重连 ResumeToken 存储。
/// 存储会话关键信息（UserId、SessionId、DeviceId、ConnectionLeaseId），
/// 客户端断线后短时间内可凭 ResumeToken 恢复会话，无需重新认证。
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
    /// </summary>
    /// <param name="resumeToken">客户端提交的 ResumeToken。</param>
    /// <param name="ct">取消令牌。</param>
    /// <returns>会话上下文；Token 无效或已过期返回 null。</returns>
    Task<ResumeContext?> TryValidateAsync(string resumeToken, CancellationToken ct = default);

    /// <summary>
    /// 撤销 ResumeToken（会话主动登出或被吊销时调用）。
    /// </summary>
    Task RevokeAsync(string resumeToken, CancellationToken ct = default);
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
