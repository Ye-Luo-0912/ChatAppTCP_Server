using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;

namespace ChatApp.TcpGateway.Gateway.Networking.Buffers;

/// <summary>
/// 字节预算截断结果分类。调用方据此决定发送数据响应还是错误响应。
/// </summary>
internal enum TruncateOutcome
{
    /// <summary>完整响应在软上限内，无需截断。</summary>
    Full,

    /// <summary>截断到 k 条（k ≥ 1），HasMore=true、NextCursor 已设置，客户端可推进游标。</summary>
    Truncated,

    /// <summary>单条 item 超过硬上限，无法装入单帧。调用方应返回 <c>item_too_large</c> 错误响应。</summary>
    ItemTooLarge,

    /// <summary>空信封（0 条 item）仍超过硬上限（配置错误）。调用方应返回 <c>response_too_large</c> 错误响应。</summary>
    EnvelopeTooLarge
}

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
/// <para>
/// 边界正确性：当单条 item 超过硬上限时返回 <see cref="TruncateOutcome.ItemTooLarge"/>，
/// 调用方据此返回 <c>item_too_large</c> 错误，避免返回 HasMore=true、NextCursor=null 的空页
/// 导致客户端游标无法推进。
/// </para>
/// </summary>
internal static class ResponseByteBudget
{
    /// <summary>
    /// 截断响应使其序列化字节数不超过 <paramref name="softByteLimit"/>（优先）或
    /// <paramref name="hardByteLimit"/>（兜底）。
    /// <para>
    /// 若完整响应已不超过 <paramref name="softByteLimit"/>，原样返回（outcome = Full）。
    /// 否则二分查找最大的条目前缀 k，使得 rebuildWithPrefix(response, k) 的序列化字节数
    /// 不超过 <paramref name="softByteLimit"/>，并返回该截断响应（outcome = Truncated）。
    /// </para>
    /// <para>
    /// 当即使 k=1 也超过软上限时，退而检查 k=1 是否不超过硬上限；若不超过则返回 k=1
    /// （outcome = Truncated，保证至少推进 1 条）。
    /// 若 k=1 也超过硬上限，返回空响应（k=0）并设置 outcome = ItemTooLarge，
    /// 调用方应据此返回 <c>item_too_large</c> 错误而非无法推进的分页结果。
    /// </para>
    /// <para>
    /// 当 itemCount ≤ 0 时，校验空信封是否超过硬上限；若超过则 outcome = EnvelopeTooLarge。
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
    /// <param name="outcome">截断结果分类。</param>
    /// <returns>已截断的响应。当 outcome = ItemTooLarge/EnvelopeTooLarge 时返回空信封响应，调用方应忽略返回值并发送错误响应。</returns>
    public static T Truncate<T>(
        T response,
        int itemCount,
        IPayloadCodec<T> codec,
        int softByteLimit,
        int hardByteLimit,
        Func<T, int, T> rebuildWithPrefix,
        out TruncateOutcome outcome)
    {
        if (itemCount <= 0)
        {
            // 0 条 item：校验空信封是否超过硬上限。
            // 超过说明协议信封本身已超出单帧容量（配置错误），调用方应返回错误。
            var emptySize = MeasurePayload(codec, response, hardByteLimit);
            outcome = emptySize < 0 || emptySize > hardByteLimit
                ? TruncateOutcome.EnvelopeTooLarge
                : TruncateOutcome.Full;
            return response;
        }

        // 快速路径：完整响应不超过软上限，无需截断。
        var fullSize = MeasurePayload(codec, response, hardByteLimit);
        if (fullSize >= 0 && fullSize <= softByteLimit)
        {
            outcome = TruncateOutcome.Full;
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
                outcome = TruncateOutcome.Truncated;
                return oneItem;
            }

            // 即使 1 条也超硬上限：返回空信封并标记 ItemTooLarge。
            // 调用方应返回 item_too_large 错误，不发送无法推进游标的空页。
            outcome = TruncateOutcome.ItemTooLarge;
            return rebuildWithPrefix(response, 0);
        }

        outcome = TruncateOutcome.Truncated;
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
    /// <para>
    /// 边界正确性：当单条 item 超过硬上限时返回 <see cref="TruncateOutcome.ItemTooLarge"/>，
    /// 调用方应据此返回错误或跳过该 item，避免返回 HasMore=true、NextCursor=null 的空页。
    /// </para>
    /// </summary>
    public static T[] TruncateArray<T>(
        IReadOnlyList<T> items,
        IPayloadCodec<T[]> codec,
        int softByteLimit,
        int hardByteLimit,
        Func<IReadOnlyList<T>, int, T[]> takePrefix,
        out TruncateOutcome outcome)
    {
        if (items.Count <= 0)
        {
            // 0 条：校验空数组序列化是否超过硬上限（理论上空数组很小，但保持防御性检查）。
            var empty = items is T[] arr ? arr : Array.Empty<T>();
            var emptySize = MeasurePayload(codec, empty, hardByteLimit);
            outcome = emptySize < 0 || emptySize > hardByteLimit
                ? TruncateOutcome.EnvelopeTooLarge
                : TruncateOutcome.Full;
            return empty;
        }

        var fullArray = items is T[] a ? a : items.ToArray();
        var fullSize = MeasurePayload(codec, fullArray, hardByteLimit);
        if (fullSize >= 0 && fullSize <= softByteLimit)
        {
            outcome = TruncateOutcome.Full;
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
                outcome = TruncateOutcome.Truncated;
                return oneItem;
            }

            outcome = TruncateOutcome.ItemTooLarge;
            return takePrefix(items, 0);
        }

        outcome = TruncateOutcome.Truncated;
        return takePrefix(items, lo);
    }
}
