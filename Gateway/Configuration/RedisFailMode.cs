namespace ChatApp.TcpGateway.Gateway.Configuration;

/// <summary>
/// P1-C：Redis 故障时的 fail-mode 策略。
/// <para>
/// 用于 <see cref="TcpGatewayOptions.ResumeRedisFailMode"/> 与
/// <see cref="TcpGatewayOptions.AuthRedisFailMode"/>，分别控制 Resume 路径与
/// 正常 Authentication 路径在 Redis 不可用时的行为。
/// </para>
/// <para>
/// 两条路径默认均 <see cref="FailClosed"/>，确保 Same-device fencing 等安全不变量
/// 在 Redis 故障期间不被绕过。运维可在降级模式下显式切换为 <see cref="FailOpen"/>
/// 以维持可用性，但需明确评估安全风险（旧 Transport 可能不被及时吊销）。
/// </para>
/// </summary>
public enum RedisFailMode : byte
{
    /// <summary>
    /// Fail-closed（默认）：Redis 不可用时拒绝操作。
    /// <list type="bullet">
    /// <item>Resume 路径：拒绝恢复、回滚本地状态、关闭连接，要求完整认证。</item>
    /// <item>Auth 路径：拒绝认证、回滚本地状态、关闭连接。
    ///   旧连接依赖设备租约 TTL 自然失效。</item>
    /// </list>
    /// 安全优先：Same-device fencing、ResumeToken 校验等安全不变量不被绕过。
    /// </summary>
    FailClosed = 0,

    /// <summary>
    /// Fail-open：Redis 不可用时放行操作，依赖本机状态与 TTL 自然收敛。
    /// <list type="bullet">
    /// <item>Resume 路径：跳过 TakeOver/代次校验，继续恢复会话。
    ///   旧 Transport 不被吊销，依赖租约 TTL 过期后自然释放。</item>
    /// <item>Auth 路径：跳过 TakeOver（best-effort），继续完成认证。
    ///   旧连接依赖本机 TakeOverSameDevice + 租约 TTL 自然失效。</item>
    /// </list>
    /// 可用性优先：Redis 故障期间允许 Resume 与认证继续，但安全 fencing 可能被绕过。
    /// 仅用于降级模式，需运维明确评估风险。
    /// </summary>
    FailOpen = 1
}
