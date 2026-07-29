namespace ChatApp.TcpGateway.Gateway.Configuration;

/// <summary>
/// Ephemeral 入站命令调度模式。取代历史布尔标志组合，明确四种运行形态。
/// <para>
/// 模式语义：
/// <list type="bullet">
/// <item><see cref="Disabled"/>：不创建任何 Ephemeral 调度资源（无 Worker / 无 Actor / 无 ConnectionQueue）。
///   用于 Specialized Typing 模式：TypingNotify 已被快路径截获，通用 Ephemeral 调度完全冗余。
///   Register/Unregister 为真正 no-op；TryEnqueue 不应被调用（返回 false）。</item>
/// <item><see cref="Legacy"/>：使用 <see cref="Networking.Executor.SessionCommandExecutor"/>（共享 Worker 池 + 每连接 ConcurrentQueue）。
///   A/B 回退路径，保留以验证 Actor Runtime 的正确性与性能。</item>
/// <item><see cref="GenericActor"/>：使用轻量 ActorRuntime（FIFO Mailbox + 异步操作执行器）。
///   默认推荐模式，替代 Legacy 以减少每连接常驻对象。</item>
/// </list>
/// Specialized Typing 模式由 <see cref="TcpGatewayOptions.UseTypingActorPipeline"/> + EnableEphemeralPresenceAndTyping 共同决定，
/// 它不是本枚举的成员：它描述的是 Typing 领域 Actor 是否启用，而非通用 Ephemeral 调度的形态。
/// 当 Specialized Typing 启用时，通用 Ephemeral 调度应设为 <see cref="Disabled"/>。
/// </para>
/// </summary>
public enum EphemeralPipelineMode
{
    /// <summary>
    /// 不创建任何 Ephemeral 调度资源。用于 Specialized Typing 模式下消除冗余 Legacy/Actor 资源。
    /// </summary>
    Disabled,

    /// <summary>
    /// 旧 SessionCommandExecutor 路径（Worker 池 + 每连接 ConcurrentQueue）。A/B 回退基线。
    /// </summary>
    Legacy,

    /// <summary>
    /// 轻量 ActorRuntime（FIFO Mailbox + 异步操作执行器）。默认推荐模式。
    /// </summary>
    GenericActor
}
