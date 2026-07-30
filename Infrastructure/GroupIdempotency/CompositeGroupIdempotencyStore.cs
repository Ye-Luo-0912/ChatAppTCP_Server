using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.TcpGateway.Observability.Metrics;

namespace ChatApp.TcpGateway.Infrastructure.GroupIdempotency;

/// <summary>
/// 两级幂等存储协调器：L1（内存）→ L2（Redis）。
/// <para>
/// <b>TryGetAsync</b>：先查 L1。L1 命中/冲突时直接返回（快速路径）；
/// L1 未命中时查 L2——L2 命中则回填 L1 并返回命中，L2 冲突则返回冲突，L2 未命中则返回未命中。
/// </para>
/// <para>
/// <b>TryAddAsync</b>：先写 L1（同步快速），再写 L2（await，异常静默吞咽——L2 已内部 fail-open）。
/// </para>
/// <para>
/// <b>EvictUser</b>：同时清除 L1 与 L2。
/// </para>
/// <para>
/// L2 可选——为 null 时 Composite 退化为仅 L1，不记录 Redis 层 metrics。
/// </para>
/// </summary>
internal sealed class CompositeGroupIdempotencyStore(
    IGroupIdempotencyStore l1Cache,
    IGroupIdempotencyStore? l2Cache,
    GatewayMetrics metrics) : IGroupIdempotencyStore
{
    public async ValueTask<GroupIdempotencyLookup> TryGetAsync(
        long userId,
        int operation,
        string requestId,
        int payloadHash,
        CancellationToken cancellationToken)
    {
        // L1 快速路径：命中或冲突时直接返回，不查 L2。
        var l1Lookup = await l1Cache.TryGetAsync(
            userId, operation, requestId, payloadHash, cancellationToken)
            .ConfigureAwait(false);

        if (l1Lookup.IsHit || l1Lookup.IsConflict)
            return l1Lookup;

        // L1 未命中：L2 不存在时直接返回 Miss。
        if (l2Cache is null)
            return GroupIdempotencyLookup.Miss;

        // L1 未命中：查 L2（Redis）。
        var l2Lookup = await l2Cache.TryGetAsync(
            userId, operation, requestId, payloadHash, cancellationToken)
            .ConfigureAwait(false);

        if (l2Lookup.IsHit)
        {
            // L2 命中：回填 L1，后续重试可从 L1 快速命中。
            metrics.GroupIdempotentRedisHit();
            await l1Cache.TryAddAsync(
                userId, operation, requestId, payloadHash,
                l2Lookup.Result!, cancellationToken)
                .ConfigureAwait(false);
            return l2Lookup;
        }

        if (l2Lookup.IsConflict)
        {
            // L2 冲突：L2 找到条目但负载指纹不匹配，记为 Redis 命中。
            metrics.GroupIdempotentRedisHit();
            return l2Lookup;
        }

        // L2 未命中（含 fail-open Miss）。
        metrics.GroupIdempotentRedisMiss();
        return GroupIdempotencyLookup.Miss;
    }

    public async ValueTask TryAddAsync(
        long userId,
        int operation,
        string requestId,
        int payloadHash,
        GroupConversationResult result,
        CancellationToken cancellationToken)
    {
        // 先写 L1（快速路径），再写 L2（持久化）。
        await l1Cache.TryAddAsync(
            userId, operation, requestId, payloadHash, result, cancellationToken)
            .ConfigureAwait(false);

        if (l2Cache is null)
            return;

        // L2 写入：await 但异常静默吞咽（L2 已内部 fail-open，此处为防御性兜底）。
        try
        {
            await l2Cache.TryAddAsync(
                userId, operation, requestId, payloadHash, result, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            // 防御性兜底：L2 实现应已内部 fail-open，不抛异常。
        }
    }

    public void EvictUser(long userId)
    {
        l1Cache.EvictUser(userId);
        l2Cache?.EvictUser(userId);
    }
}
