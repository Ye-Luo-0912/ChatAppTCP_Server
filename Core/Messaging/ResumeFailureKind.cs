namespace ChatApp.TcpGateway.Core.Messaging;

/// <summary>
/// P1-B：Resume 失败种类分类，供客户端区分不可恢复的 Token 失效与可重试的依赖故障。
/// <para>
/// 与 <see cref="Protocol.ProtocolErrorCode"/> 的关系：
/// <list type="bullet">
/// <item><see cref="InvalidToken"/> → <see cref="Protocol.ProtocolErrorCode.ResumeFailed"/>
///   （客户端必须走完整认证）</item>
/// <item><see cref="DependencyUnavailable"/> → <see cref="Protocol.ProtocolErrorCode.DependencyUnavailable"/>
///   （客户端可退避后重试 Resume 或回退完整认证）</item>
/// </list>
/// </para>
/// </summary>
public enum ResumeFailureKind : byte
{
    /// <summary>未失败（成功或未尝试）。</summary>
    None = 0,

    /// <summary>
    /// Token 无效、过期、被消费，或设备租约代次不匹配。
    /// 客户端必须走完整认证流程，重试 Resume 无意义。
    /// </summary>
    InvalidToken = 1,

    /// <summary>
    /// 依赖不可用（Redis 异常、熔断器开路、TakeOver 不可用）。
    /// 客户端可按 <see cref="ResumeResponse.RetryAfterMs"/> 退避后重试 Resume，
    /// 或回退到完整认证（完整认证路径同样可能受依赖故障影响）。
    /// </summary>
    DependencyUnavailable = 2,

    /// <summary>
    /// 三-3：账号已被冻结，Resume 拒绝。客户端应走完整认证（同样会被拒绝）。
    /// </summary>
    UserFrozen = 3
}
