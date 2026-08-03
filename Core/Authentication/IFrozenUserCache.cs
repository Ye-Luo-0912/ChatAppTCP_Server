namespace ChatApp.TcpGateway.Core.Authentication;

/// <summary>
/// 三-3：冻结用户缓存。维护已被管理员冻结的用户 Id 集合，
/// 供认证与 Resume 路径快速拒绝。
/// <para>
/// 缓存由 <c>UserLifecycleChanged</c> Realtime 事件驱动更新：
/// 收到 Frozen 事件时 <see cref="MarkFrozen"/>，收到 Active（解冻）事件时 <see cref="MarkUnfrozen"/>。
/// </para>
/// <para>
/// <b>Cache Miss 策略</b>：fail-open + 后台刷新。
/// <see cref="IsFrozen"/> 在缓存未命中时返回 false（允许认证/Resume），
/// 同时触发后台查询刷新缓存。认证路径权威性由 AccessTokenStore 保证
/// （冻结时 Server 撤销 access token）；Resume 路径在缓存预热后秒级拦截。
/// </para>
/// </summary>
public interface IFrozenUserCache
{
    /// <summary>
    /// 查询用户是否已被冻结。缓存未命中时返回 false（fail-open）。
    /// </summary>
    bool IsFrozen(long userId);

    /// <summary>
    /// 标记用户为冻结状态。由 <c>UserLifecycleChanged(Frozen)</c> 事件处理器调用。
    /// </summary>
    void MarkFrozen(long userId, long frozenAtMs);

    /// <summary>
    /// 清除用户的冻结标记。由 <c>UserLifecycleChanged(Active)</c> 事件处理器调用。
    /// </summary>
    void MarkUnfrozen(long userId);
}
