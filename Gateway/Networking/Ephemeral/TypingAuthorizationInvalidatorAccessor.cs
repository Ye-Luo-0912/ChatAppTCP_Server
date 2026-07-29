namespace ChatApp.TcpGateway.Gateway.Networking.Ephemeral;

/// <summary>
/// <see cref="ITypingAuthorizationInvalidator"/> 的可变单例持有器：
/// 解决 <see cref="TypingActorPipeline"/>（在 <see cref="Networking.TcpGatewayService"/>
/// 构造时创建）与 <see cref="Messaging.Realtime.Handlers.RelationshipListHandler"/>
/// （在 DI 构造时创建）之间的生命周期错位。
/// <para>
/// DI 注册为单例：RelationshipListHandler 注入本持有器并调用
/// <see cref="InvalidateAuthorization"/>；TcpGatewayService 在创建 TypingActorPipeline 后
/// 调用 <see cref="SetInstance"/> 注册实现。Specialized 未启用时持有 null，
/// 调用为 no-op（依赖 CachedDirectConversationAuthorizer 的 TTL 兜底）。
/// </para>
/// </summary>
internal sealed class TypingAuthorizationInvalidatorAccessor : ITypingAuthorizationInvalidator
{
    private volatile ITypingAuthorizationInvalidator? _inner;

    /// <summary>TcpGatewayService 创建 TypingActorPipeline 后调用，注册真实实现。</summary>
    public void SetInstance(ITypingAuthorizationInvalidator? instance) => _inner = instance;

    public void InvalidateAuthorization(long senderUserId, long targetUserId)
    {
        // 未注册（Specialized 未启用）时为 no-op。
        _inner?.InvalidateAuthorization(senderUserId, targetUserId);
    }
}
