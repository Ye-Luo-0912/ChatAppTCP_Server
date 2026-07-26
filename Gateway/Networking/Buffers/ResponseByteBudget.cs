using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;

namespace ChatApp.TcpGateway.Gateway.Networking.Buffers;

/// <summary>
/// 按字节预算截断分页响应。
/// <para>
/// 后端分页不能只按条数截断，还要按序列化字节数截断，确保响应可装入单帧 TCP Payload。
/// 使用二分查找确定不超过 <see cref="PacketProtocol.WireResponseSoftLimit"/> 的最大条目前缀，
/// 截断时调用 <paramref name="rebuildWithPrefix"/> 重建响应（设置 HasMore/NextCursor）。
/// </para>
/// <para>
/// 序列化字节数随条目数单调递增（更多数组元素 → 更多 JSON 字节），因此二分查找适用。
/// </para>
/// </summary>
internal static class ResponseByteBudget
{
    /// <summary>
    /// 截断响应使其序列化字节数不超过 <paramref name="softByteLimit"/>（优先）或
    /// <paramref name="hardByteLimit"/>（兜底）。
    /// <para>
    /// 若完整响应已不超过 <paramref name="softByteLimit"/>，原样返回。
    /// 否则二分查找最大的条目前缀 k，使得 rebuildWithPrefix(response, k) 的序列化字节数
    /// 不超过 <paramref name="softByteLimit"/>，并返回该截断响应。
    /// </para>
    /// <para>
    /// 当即使 k=1 也超过软上限时，退而检查 k=1 是否不超过硬上限；若不超过则返回 k=1，
    /// 否则返回 k=0（空页，极端边界情况，按命令 Payload 上限应能预防）。
    /// </para>
    /// </summary>
    /// <typeparam name="T">响应类型。</typeparam>
    /// <param name="response">包含全部条目的原始响应。</param>
    /// <param name="itemCount">原始条目数。</param>
    /// <param name="codec">响应序列化编解码器。</param>
    /// <param name="softByteLimit">软字节上限（截断目标）。</param>
    /// <param name="hardByteLimit">硬字节上限（单帧绝对上限）。</param>
    /// <param name="rebuildWithPrefix">
    /// 给定原始响应与条目数 k，返回一个新的响应：Items 取前 k 条；
    /// 若 k 小于总数则 HasMore=true、NextCursor 由第 k 条（最后保留条目）派生；
    /// 若 k 等于总数则保持原始 HasMore/NextCursor。
    /// </param>
    /// <returns>已截断的响应（保证序列化字节数不超过 <paramref name="hardByteLimit"/>）。</returns>
    public static T Truncate<T>(
        T response,
        int itemCount,
        IPayloadCodec<T> codec,
        int softByteLimit,
        int hardByteLimit,
        Func<T, int, T> rebuildWithPrefix)
    {
        if (itemCount <= 0)
        {
            return response;
        }

        // 快速路径：完整响应不超过软上限，无需截断。
        var fullSize = MeasurePayload(codec, response, hardByteLimit);
        if (fullSize >= 0 && fullSize <= softByteLimit)
        {
            return response;
        }

        // 二分查找最大的 k ∈ [0, itemCount]，使得 rebuildWithPrefix(k) 的序列化字节数 ≤ softByteLimit。
        // 不变式：measure(rebuild(lo)) ≤ softByteLimit（lo=0 恒成立：空信封远小于软上限）。
        var lo = 0;
        var hi = itemCount;
        while (lo < hi)
        {
            var mid = lo + (hi - lo + 1) / 2;
            var candidate = rebuildWithPrefix(response, mid);
            var size = MeasurePayload(codec, candidate, hardByteLimit);
            if (size >= 0 && size <= softByteLimit)
            {
                lo = mid;
            }
            else
            {
                hi = mid - 1;
            }
        }

        // lo 为不超过软上限的最大前缀。若 lo=0（即使 1 条也超软上限），
        // 退而检查 k=1 是否不超过硬上限，保证至少推进 1 条（避免客户端分页死锁）。
        if (lo == 0)
        {
            var oneItem = rebuildWithPrefix(response, 1);
            var oneSize = MeasurePayload(codec, oneItem, hardByteLimit);
            if (oneSize >= 0 && oneSize <= hardByteLimit)
            {
                return oneItem;
            }

            // 即使 1 条也超硬上限：返回空页（极端边界，应预防）。
            return rebuildWithPrefix(response, 0);
        }

        return rebuildWithPrefix(response, lo);
    }

    /// <summary>
    /// 测量响应序列化后的 Payload 字节数。
    /// <para>
    /// 返回 -1 表示序列化超出 <paramref name="hardByteLimit"/>（PooledBufferWriter 抛出
    /// InvalidOperationException），调用方据此判定需进一步截断。
    /// </para>
    /// </summary>
    public static int MeasurePayload<T>(
        IPayloadCodec<T> codec,
        T value,
        int hardByteLimit)
    {
        using var writer = new PooledBufferWriter(
            PacketProtocol.HeaderSize + 256,
            PacketProtocol.HeaderSize + hardByteLimit);
        writer.Advance(PacketProtocol.HeaderSize);
        try
        {
            codec.Serialize(writer, value);
        }
        catch (InvalidOperationException)
        {
            return -1;
        }
        return writer.WrittenCount - PacketProtocol.HeaderSize;
    }

    /// <summary>
    /// 按字节预算截断裸数组（不含响应信封）。
    /// <para>
    /// 用于 SyncBootstrap 内部子集合（Conversations 列表、CatchUp.Items 列表）的独立截断。
    /// 二分查找最大的 k，使得前 k 条的序列化字节数不超过 <paramref name="softByteLimit"/>。
    /// </para>
    /// </summary>
    public static T[] TruncateArray<T>(
        IReadOnlyList<T> items,
        IPayloadCodec<T[]> codec,
        int softByteLimit,
        int hardByteLimit,
        Func<IReadOnlyList<T>, int, T[]> takePrefix)
    {
        if (items.Count <= 0)
        {
            return items is T[] arr ? arr : Array.Empty<T>();
        }

        var fullArray = items is T[] a ? a : items.ToArray();
        var fullSize = MeasurePayload(codec, fullArray, hardByteLimit);
        if (fullSize >= 0 && fullSize <= softByteLimit)
        {
            return fullArray;
        }

        var lo = 0;
        var hi = items.Count;
        while (lo < hi)
        {
            var mid = lo + (hi - lo + 1) / 2;
            var candidate = takePrefix(items, mid);
            var size = MeasurePayload(codec, candidate, hardByteLimit);
            if (size >= 0 && size <= softByteLimit)
            {
                lo = mid;
            }
            else
            {
                hi = mid - 1;
            }
        }

        if (lo == 0)
        {
            var oneItem = takePrefix(items, 1);
            var oneSize = MeasurePayload(codec, oneItem, hardByteLimit);
            if (oneSize >= 0 && oneSize <= hardByteLimit)
            {
                return oneItem;
            }

            return takePrefix(items, 0);
        }

        return takePrefix(items, lo);
    }
}
