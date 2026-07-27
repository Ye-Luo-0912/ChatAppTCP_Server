namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// Actor Runtime 配置。所有时间均为 TimeSpan，内部按需换算为 TimeProvider timestamp units。
/// </summary>
public sealed class ActorRuntimeOptions
{
    /// <summary>
    /// Shard 数量。必须为 2 的幂。默认等于处理器数（向上取整到最近的 2 的幂），下限 2。
    /// <para>
    /// 路由：<c>StableHash(key) &amp; (ShardCount - 1)</c>。
    /// 数量过少会导致热点 Actor 拖累同 Shard 其他 Actor；过多增加内存与调度开销。
    /// </para>
    /// </summary>
    public int ShardCount { get; set; } = NextPow2(Environment.ProcessorCount);

    /// <summary>
    /// 每 Shard Ingress Ring 容量（跨线程生产者 → 单 Shard Consumer）。
    /// 默认 1024，足够吸收突发且不过度占用内存。
    /// </summary>
    public int ShardIngressCapacity { get; set; } = 1024;

    /// <summary>
    /// 每 Actor FIFO Mailbox 默认容量。
    /// <para>
    /// LatestOnly 模式下此值被忽略（容量始终为 1）。
    /// 默认 64：足以吸收短暂突发而不占用过多内存。
    /// </para>
    /// </summary>
    public int DefaultMailboxCapacity { get; set; } = 64;

    /// <summary>
    /// Shard Consumer Loop 每轮处理的最大 Actor 数（公平性 burst）。
    /// 达到上限后让出时间片，避免单 Shard 长时间独占。
    /// 默认 64。
    /// </summary>
    public int ShardBurstLimit { get; set; } = 64;

    /// <summary>
    /// 单个 Actor 一次被调度时最多处理的消息数。
    /// 达到上限后重新排到 Ready Queue 尾部，避免热点 Actor 饿死同 Shard 的其他 Actor。
    /// </summary>
    public int MaxMessagesPerActorTurn { get; set; } = 32;

    /// <summary>
    /// Shard DeadlineWheel 推进间隔（处理 Actor 超时与定时任务）。
    /// 默认 50ms，平衡精度与 CPU 开销。
    /// </summary>
    public TimeSpan ShardTickInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// AsyncOperationExecutor 配置：异步操作（NATS/Redis/Query 等 I/O）的全局并发上限。
    /// 默认等于处理器数 × 2，足够并发吸收后端 I/O 而不过度压垮 Redis/NATS。
    /// </summary>
    public int AsyncOperationConcurrency { get; set; } = Environment.ProcessorCount * 2;

    /// <summary>
    /// AsyncOperationExecutor 全局队列容量上限。
    /// 默认 4096：足以吸收短暂突发；超过时拒绝新提交（Actor 应 Suspend 直到 Completion 回来）。
    /// </summary>
    public int AsyncOperationQueueCapacity { get; set; } = 4096;

    /// <summary>
    /// 单条异步操作超时。Zero 表示不启用（依赖外部 I/O 自己的超时）。
    /// 默认 30 秒。
    /// </summary>
    public TimeSpan AsyncOperationTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Actor 空闲回收阈值：自 LastActiveTimestamp 起经过此时间未收到消息则 Deactivate。
    /// 默认 5 分钟。设为 <see cref="TimeSpan.Zero"/> 禁用空闲回收。
    /// </summary>
    public TimeSpan ActorIdleTimeout { get; set; } = TimeSpan.FromMinutes(5);

    private static int NextPow2(int v)
    {
        if (v <= 2) return 2;
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        return v + 1;
    }
}

internal static class ActorRuntimeOptionsValidation
{
    public static void Validate(ActorRuntimeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.ShardCount <= 0 || (options.ShardCount & (options.ShardCount - 1)) != 0)
            throw new ArgumentOutOfRangeException(
                paramName: nameof(options),
                message: $"options.{nameof(options.ShardCount)} must be a positive power of two");
        if (options.ShardIngressCapacity < 2 ||
            (options.ShardIngressCapacity &
             (options.ShardIngressCapacity - 1)) != 0)
        {
            throw new ArgumentOutOfRangeException(
                paramName: nameof(options),
                message:
                $"options.{nameof(options.ShardIngressCapacity)} must be a power of two and at least 2");
        }

        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.DefaultMailboxCapacity, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.ShardBurstLimit, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.MaxMessagesPerActorTurn, 0);
        if (options.ShardTickInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(
                paramName: nameof(options),
                message: $"options.{nameof(options.ShardTickInterval)} must be positive");
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.AsyncOperationConcurrency, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.AsyncOperationQueueCapacity, 0);
    }
}
