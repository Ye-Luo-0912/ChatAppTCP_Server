using System.Text.Json;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class ResponseByteBudgetTests
{
    private static readonly IPayloadCodec<MessageHistoryResponse> ResponseCodec =
        new JsonPayloadCodec<MessageHistoryResponse>(
            GatewayJsonSerializerContext.Default.MessageHistoryResponse);

    private static readonly IPayloadCodec<MessageHistoryItem[]> ItemCodec =
        new JsonPayloadCodec<MessageHistoryItem[]>(
            GatewayJsonSerializerContext.Default.MessageHistoryItemArray);

    // --- MeasurePayload ---

    [Fact]
    public void MeasurePayload_ReturnsPositiveSize_ForValidPayload()
    {
        var response = CreateResponse(itemCount: 3);
        var size = ResponseByteBudget.MeasurePayload(
            ResponseCodec,
            response,
            PacketProtocol.WireResponseHardLimit);
        Assert.True(size > 0);
        Assert.True(size < PacketProtocol.WireResponseHardLimit);
    }

    [Fact]
    public void MeasurePayload_ReturnsNegativeOne_WhenPayloadExceedsHardLimit()
    {
        // 每条约 500 字节；200 条 → ~100KB > 80KB 硬上限。
        var response = CreateResponse(itemCount: 200);
        var size = ResponseByteBudget.MeasurePayload(
            ResponseCodec,
            response,
            PacketProtocol.WireResponseHardLimit);
        Assert.Equal(-1, size);
    }

    // --- Truncate: 快速路径 ---

    [Fact]
    public void Truncate_ReturnsOriginal_WhenWithinSoftLimit()
    {
        var response = CreateResponse(itemCount: 3);
        var result = ResponseByteBudget.Truncate(
            response,
            3,
            ResponseCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            RebuildWithPrefix,
            out var outcome);
        Assert.Same(response, result);
        Assert.Equal(TruncateOutcome.Full, outcome);
        Assert.Equal(3, result.Items.Count);
        Assert.False(result.HasMore);
    }

    // --- Truncate: 截断路径 ---

    [Fact]
    public void Truncate_ReducesItems_WhenExceedingSoftLimit()
    {
        // 200 条 ~100KB > 64KB 软上限 → 截断。
        var response = CreateResponse(itemCount: 200);
        var result = ResponseByteBudget.Truncate(
            response,
            200,
            ResponseCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            RebuildWithPrefix,
            out var outcome);

        Assert.Equal(TruncateOutcome.Truncated, outcome);
        Assert.True(result.Items.Count < 200);
        Assert.True(result.Items.Count > 0);
        Assert.True(result.HasMore);
        Assert.NotNull(result.NextCursor);

        // 截断后必须在软上限以内。
        var size = ResponseByteBudget.MeasurePayload(
            ResponseCodec,
            result,
            PacketProtocol.WireResponseHardLimit);
        Assert.True(size <= PacketProtocol.WireResponseSoftLimit);
    }

    [Fact]
    public void Truncate_SetsNextCursor_FromLastRetainedItem()
    {
        var response = CreateResponse(itemCount: 200);
        var result = ResponseByteBudget.Truncate(
            response,
            200,
            ResponseCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            RebuildWithPrefix,
            out _);

        Assert.NotNull(result.NextCursor);
        var lastItem = result.Items[^1];
        Assert.Equal(lastItem.ReceivedAtMs, result.NextCursor!.ReceivedAtMs);
        Assert.Equal(lastItem.MessageId, result.NextCursor.MessageId);
    }

    [Fact]
    public void Truncate_PreservesRequestId_AndEnvelopeFields()
    {
        var response = new MessageHistoryResponse
        {
            RequestId = "test-request-id",
            Succeeded = true,
            ErrorCode = null,
            ErrorMessage = null,
            Items = Enumerable.Range(0, 200)
                .Select(i => CreateItem(i))
                .ToArray(),
            NextCursor = null,
            HasMore = false
        };

        var result = ResponseByteBudget.Truncate(
            response,
            200,
            ResponseCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            RebuildWithPrefix,
            out _);

        Assert.Equal("test-request-id", result.RequestId);
        Assert.True(result.Succeeded);
    }

    // --- Truncate: 边界情况 ---

    [Fact]
    public void Truncate_ReturnsAtLeastOneItem_WhenSingleItemFitsHardLimit()
    {
        // 软上限设为 1 字节（即使 1 条也超软上限），但 1 条远小于 80KB 硬上限。
        var response = CreateResponse(itemCount: 5);
        var result = ResponseByteBudget.Truncate(
            response,
            5,
            ResponseCodec,
            softByteLimit: 1,
            PacketProtocol.WireResponseHardLimit,
            RebuildWithPrefix,
            out var outcome);

        Assert.Equal(TruncateOutcome.Truncated, outcome);
        Assert.Single(result.Items);
        Assert.True(result.HasMore);
    }

    [Fact]
    public void Truncate_ReturnsFullOutcome_WhenZeroItems()
    {
        var response = CreateResponse(itemCount: 0);
        var result = ResponseByteBudget.Truncate(
            response,
            0,
            ResponseCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            RebuildWithPrefix,
            out var outcome);
        Assert.Same(response, result);
        Assert.Equal(TruncateOutcome.Full, outcome);
        Assert.Empty(result.Items);
    }

    [Fact]
    public void Truncate_ReturnsItemTooLarge_WhenSingleItemExceedsHardLimit()
    {
        // 单条消息 ~100KB > 80KB 硬上限 → ItemTooLarge。
        var hugeItem = new MessageHistoryItem
        {
            MessageId = "msg-huge",
            ClientMessageId = "client-huge",
            SenderUserId = 1,
            ReceiverUserId = 2,
            ConversationId = "dm:1:2",
            Content = new string('x', 100_000),
            ReceivedAtMs = 1700000000000L,
            ChangedAtMs = 1700000000000L
        };
        var response = new MessageHistoryResponse
        {
            RequestId = "req-huge",
            Succeeded = true,
            Items = new[] { hugeItem },
            HasMore = false
        };

        var result = ResponseByteBudget.Truncate(
            response,
            1,
            ResponseCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            RebuildWithPrefix,
            out var outcome);

        Assert.Equal(TruncateOutcome.ItemTooLarge, outcome);
        // 返回空信封，调用方应忽略返回值并发送错误响应。
        Assert.Empty(result.Items);
    }

    [Fact]
    public void TruncateArray_ReturnsItemTooLarge_WhenSingleItemExceedsHardLimit()
    {
        var hugeItem = new MessageHistoryItem
        {
            MessageId = "msg-huge",
            ClientMessageId = "client-huge",
            SenderUserId = 1,
            ReceiverUserId = 2,
            ConversationId = "dm:1:2",
            Content = new string('x', 100_000),
            ReceivedAtMs = 1700000000000L,
            ChangedAtMs = 1700000000000L
        };
        var items = new[] { hugeItem };

        var result = ResponseByteBudget.TruncateArray(
            items,
            ItemCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            static (src, k) => k <= 0
                ? Array.Empty<MessageHistoryItem>()
                : src.Take(k).ToArray(),
            out var outcome);

        Assert.Equal(TruncateOutcome.ItemTooLarge, outcome);
        Assert.Empty(result);
    }

    // --- TruncateArray ---

    [Fact]
    public void TruncateArray_ReturnsOriginal_WhenWithinSoftLimit()
    {
        var items = Enumerable.Range(0, 3)
            .Select(i => CreateItem(i))
            .ToArray();
        var result = ResponseByteBudget.TruncateArray(
            items,
            ItemCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            static (src, k) => k <= 0
                ? Array.Empty<MessageHistoryItem>()
                : src.Take(k).ToArray(),
            out var outcome);
        Assert.Same(items, result);
        Assert.Equal(TruncateOutcome.Full, outcome);
    }

    [Fact]
    public void TruncateArray_ReducesItems_WhenExceedingSoftLimit()
    {
        var items = Enumerable.Range(0, 200)
            .Select(i => CreateItem(i))
            .ToArray();
        var result = ResponseByteBudget.TruncateArray(
            items,
            ItemCodec,
            PacketProtocol.WireResponseSoftLimit,
            PacketProtocol.WireResponseHardLimit,
            static (src, k) => k <= 0
                ? Array.Empty<MessageHistoryItem>()
                : src.Take(k).ToArray(),
            out var outcome);

        Assert.Equal(TruncateOutcome.Truncated, outcome);
        Assert.True(result.Length < 200);
        Assert.True(result.Length > 0);

        var size = ResponseByteBudget.MeasurePayload(
            ItemCodec,
            result,
            PacketProtocol.WireResponseHardLimit);
        Assert.True(size <= PacketProtocol.WireResponseSoftLimit);
    }

    // --- 协议常量验证 ---

    [Fact]
    public void ProtocolConstants_AreConsistent()
    {
        // 契约：软上限 ≤ 硬上限 = MaxPayloadSize。
        Assert.True(PacketProtocol.WireResponseSoftLimit <= PacketProtocol.WireResponseHardLimit);
        Assert.Equal(PacketProtocol.MaxPayloadSize, PacketProtocol.WireResponseHardLimit);

        // 分页条数上限。
        Assert.Equal(100, PacketProtocol.HistoryPageMaxItems);
        Assert.Equal(100, PacketProtocol.ConversationListMaxItems);
        Assert.Equal(50, PacketProtocol.SyncMaxWatermarks);
    }

    // --- 辅助方法 ---

    private static MessageHistoryResponse CreateResponse(int itemCount)
    {
        var items = Enumerable.Range(0, itemCount)
            .Select(CreateItem)
            .ToArray();
        return new MessageHistoryResponse
        {
            RequestId = "req-" + Guid.NewGuid().ToString("N"),
            Succeeded = true,
            Items = items,
            HasMore = false
        };
    }

    private static MessageHistoryItem CreateItem(int i)
    {
        return new MessageHistoryItem
        {
            MessageId = $"msg-{i:D6}",
            ClientMessageId = $"client-{i:D6}",
            SenderUserId = 1,
            ReceiverUserId = 2,
            ConversationId = "dm:1:2",
            Content = $"消息内容 {i} - " + new string('x', 200),
            ReceivedAtMs = 1700000000000L + i,
            ChangedAtMs = 1700000000000L + i
        };
    }

    private static MessageHistoryResponse RebuildWithPrefix(
        MessageHistoryResponse original,
        int k)
    {
        if (k >= original.Items.Count)
        {
            return original;
        }

        var prefix = k <= 0
            ? Array.Empty<MessageHistoryItem>()
            : original.Items.Take(k).ToArray();
        var cursor = k > 0
            ? new MessageHistoryCursor
            {
                ReceivedAtMs = prefix[k - 1].ReceivedAtMs,
                MessageId = prefix[k - 1].MessageId
            }
            : null;
        return original with
        {
            Items = prefix,
            NextCursor = cursor,
            HasMore = true
        };
    }
}
