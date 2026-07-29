namespace ChatApp.TcpGateway.Gateway.Networking.Ephemeral;

/// <summary>
/// Typing 授权失效桥接接口：关系变更时通知 Typing Actor 清空缓存的 Authorized=true。
/// <para>
/// 解耦 <see cref="TypingActorPipeline"/>（在 <see cref="Networking.TcpGatewayService"/> 内部创建）
/// 与 <see cref="Messaging.Realtime.Handlers.RelationshipListHandler"/>（在 DI 容器中创建）：
/// TypingActorPipeline 启动时注册自身为实现，RelationshipListHandler 通过 DI 注入本接口。
/// Specialized 未启用时实现为 null（默认 no-op）。
/// </para>
/// </summary>
internal interface ITypingAuthorizationInvalidator
{
    /// <summary>
    /// 失效指定 (sender, target) 双向 Actor 的授权缓存。
    /// 关系变更（拉黑/解除好友）后调用，避免已活跃 Actor 长期保留旧授权。
    /// </summary>
    void InvalidateAuthorization(long senderUserId, long targetUserId);
}

/// <summary>
/// 默认 no-op 实现：Specialized TypingActor 未启用时使用。
/// </summary>
internal sealed class NullTypingAuthorizationInvalidator : ITypingAuthorizationInvalidator
{
    public void InvalidateAuthorization(long senderUserId, long targetUserId)
    {
        // 特意空实现：Specialized TypingActor 未启用时无需失效。
    }
}
