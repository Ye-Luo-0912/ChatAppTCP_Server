using System.Runtime.CompilerServices;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// 每个 Actor Key 的线程安全路由对象。独立于 <see cref="ActorCell{TKey,TState,TMessage}"/>，
/// 承担生产侧准入决策（邮件配额 + 激活配额 + 状态机），消除 P0-2 的 TOCTOU 竞态。
/// <para>
/// 状态机：Inactive → Activating → Active → Retiring →（Inactive）。
/// <list type="bullet">
/// <item><b>Inactive</b>：无 Actor、无配额。</item>
/// <item><b>Activating</b>：激活配额已持有，激活进行中（生产侧预留或退休转移）。</item>
/// <item><b>Active</b>：Actor 活跃，配额由活跃 Actor 持有。</item>
/// <item><b>Retiring</b>：Actor 正在被消费侧退休，配额仍由该 Actor 持有（直到退休完成）。</item>
/// </list>
/// </para>
/// <para>
/// 生产侧不直接探测 Cell（不再有 ContainsActor 快照竞态），而是通过本路由原子地决定：
/// <list type="bullet">
/// <item>Inactive → Activating：预留全局激活配额 + 邮件配额；</item>
/// <item>Activating / Active：仅预留邮件配额（激活已在进行或已存在）；</item>
/// <item>Retiring：接管（Retiring → Activating），转移配额并建立下一代激活。</item>
/// </list>
/// 消费侧在激活成功时提交为 Active，失败时释放配额并回滚；退休时经
/// <see cref="TryBeginRetirement"/> / <see cref="TryCompleteRetirement"/> 协调配额释放，
/// 确保"已返回 Accepted 的持久消息"不会被消费侧因配额满而静默丢弃。
/// </para>
/// </summary>
internal sealed class ActorRoute
{
    private const int Inactive = 0;
    private const int Activating = 1;
    private const int Active = 2;
    private const int Retiring = 3;

    private int _state;   // 状态机（Inactive/Activating/Active/Retiring）
    private int _pending; // 邮件配额：Ingress + Mailbox 中尚未完全处理的消息数
    private int _retired; // 邮件配额 retired 标志（FIFO 优化，防止字典无界增长）
    private long _generation; // 激活代数（每次 CommitActive 递增；0 = 从未激活）

    public bool IsRetired => Volatile.Read(ref _retired) != 0;
    public int State => Volatile.Read(ref _state);
    public long Generation => Volatile.Read(ref _generation);
    public int Pending => Volatile.Read(ref _pending);

    /// <summary>
    /// 当前是否持有激活配额（非 Inactive）。消费侧据此决定：
    /// 已持有 → 直接创建 Actor 并 <see cref="CommitActive"/>（复用预留）；
    /// 未持有（Inactive）→ 需生产侧/消费侧 TryAcquire 安全网。
    /// </summary>
    public bool HasReservation => Volatile.Read(ref _state) != Inactive;

    // ------------------------------------------------------------------
    // 邮件配额（Mailbox Credits）
    // ------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReserveMailbox(int capacity)
    {
        if (Volatile.Read(ref _retired) != 0)
            return false;

        var current = Volatile.Read(ref _pending);
        while (current < capacity)
        {
            var observed = Interlocked.CompareExchange(
                ref _pending,
                current + 1,
                current);
            if (observed == current)
            {
                if (Volatile.Read(ref _retired) == 0)
                    return true;

                Interlocked.Decrement(ref _pending);
                return false;
            }

            current = observed;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ReleaseMailbox()
    {
        var remaining = Interlocked.Decrement(ref _pending);
        if (remaining < 0)
            throw new InvalidOperationException("Actor route mailbox credit released more than once.");
    }

    public bool TryRetireIfIdle()
    {
        if (Volatile.Read(ref _pending) != 0)
            return false;

        return Interlocked.CompareExchange(ref _retired, 1, 0) == 0;
    }

    // ------------------------------------------------------------------
    // 激活配额（Actor Quota Reservation）与状态机
    // ------------------------------------------------------------------

    /// <summary>
    /// 生产侧持久入队准备：依据路由状态决定是否预留全局激活配额。
    /// <para>
    /// 调用方须先通过 <see cref="TryReserveMailbox"/> 预留邮件配额（FIFO 模式），
    /// 使在途消息计入 <see cref="_pending"/>，防止退休释放配额时遗漏在途消息。
    /// </para>
    /// </summary>
    /// <param name="globalQuota">全局激活配额。</param>
    /// <param name="quotaReserved">
    /// 输出：true 表示本次入队已预留全局激活配额（调用方入队失败时须释放）。
    /// </param>
    /// <returns>
    /// true = 可继续入队（<paramref name="quotaReserved"/> 指示是否已预留配额）；
    /// false = 全局激活配额已满，调用方应返回 AdmissionRejected。
    /// </returns>
    public bool TryBeginActivation(
        GlobalActorAdmissionQuota globalQuota,
        out bool quotaReserved)
    {
        quotaReserved = false;
        while (true)
        {
            var state = Volatile.Read(ref _state);
            switch (state)
            {
                case Inactive:
                    if (Interlocked.CompareExchange(ref _state, Activating, Inactive) != Inactive)
                        continue; // 竞态，重试

                    if (!globalQuota.TryAcquire())
                    {
                        // 回滚状态，避免占用 Activating 但无配额。
                        Volatile.Write(ref _state, Inactive);
                        return false;
                    }

                    quotaReserved = true;
                    return true;

                case Activating:
                case Active:
                    // 激活已在进行或已存在，无需预留配额。
                    return true;

                case Retiring:
                    // 接管：转移 Retiring Actor 持有的配额到下一代激活，无需新增 TryAcquire。
                    if (Interlocked.CompareExchange(ref _state, Activating, Retiring) != Retiring)
                        continue; // 竞态，重试

                    quotaReserved = true;
                    return true;

                default:
                    throw new InvalidOperationException("Unknown ActorRoute state.");
            }
        }
    }

    /// <summary>
    /// 消费侧成功激活后提交为 Active。调用方须已完成全局配额预留或 TryAcquire。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CommitActive()
    {
        Interlocked.Exchange(ref _state, Active);
        Interlocked.Increment(ref _generation);
    }

    /// <summary>
    /// 消费侧激活失败（如 Shard 满）时回滚：Activating → Inactive。
    /// 调用方须释放本路径持有的全局配额。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RollbackActivation()
        => Volatile.Write(ref _state, Inactive);

    /// <summary>
    /// 消费侧开始退休：Active → Retiring。返回 false 表示路由非 Active（无 cell 或已被接管），
    /// 调用方不应执行配额释放。
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryBeginRetirement()
        => Interlocked.CompareExchange(ref _state, Retiring, Active) == Active;

    /// <summary>
    /// 消费侧完成退休：决定是否释放全局配额。
    /// <para>
    /// 若存在在途消息（<see cref="_pending"/> &gt; 0）或已被生产侧接管（Retiring → Activating），
    /// 则保留配额（在途消息将重新激活），返回 false；否则转移 Retiring → Inactive 并返回 true，
    /// 调用方据此释放全局配额。
    /// </para>
    /// </summary>
    public bool TryCompleteRetirement()
    {
        if (Volatile.Read(ref _pending) > 0)
        {
            // 在途消息需要重新激活：保留配额，转移到 Activating。
            // 若已被生产侧接管（已是 Activating），则无需改动。
            Interlocked.CompareExchange(ref _state, Activating, Retiring);
            return false;
        }

        var prev = Interlocked.CompareExchange(ref _state, Inactive, Retiring);
        return prev == Retiring;
    }

    /// <summary>
    /// 回滚 Activating 状态：释放本路径持有的激活配额（若持有）。
    /// <para>
    /// 生产侧入队失败（ShardOverloaded）或消费侧激活失败（Activate 抛异常）时调用，
    /// 均会留下孤儿 Activating 状态。若存在其他在途消息（<see cref="_pending"/> &gt; 0），
    /// 保留 Activating 状态与配额（它们将激活），返回 false；否则回滚到 Inactive 并返回 true，
    /// 调用方据此释放全局配额。
    /// </para>
    /// </summary>
    public bool TryRollbackActivation()
    {
        if (Volatile.Read(ref _pending) > 0)
            return false;

        Interlocked.CompareExchange(ref _state, Inactive, Activating);
        return true;
    }
}