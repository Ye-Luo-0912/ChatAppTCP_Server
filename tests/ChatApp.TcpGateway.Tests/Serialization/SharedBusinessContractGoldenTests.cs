using System.Text.Json;
using ChatApp.Shared.Protocol.Tcp;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Serialization;

public sealed class SharedBusinessContractGoldenTests
{
    private const string HistoryRequestJson =
        "{\"requestId\":\"history-01\",\"conversationId\":\"conversation-01\",\"afterReceivedAtMs\":1735689600100,\"afterMessageId\":\"message-10\",\"limit\":50}";

    private const string HistoryResponseJson =
        "{\"requestId\":\"history-01\",\"conversationId\":\"conversation-01\",\"succeeded\":true,\"items\":[],\"nextCursor\":{\"receivedAtMs\":1735689600000,\"changedAtMs\":1735689600100,\"messageId\":\"message-10\"},\"hasMore\":true}";

    private const string SyncRequestJson =
        "{\"requestId\":\"sync-01\",\"listLimit\":50,\"historyLimitPerConversation\":20,\"maxConversationsWithHistory\":10,\"watermarks\":[{\"conversationId\":\"conversation-01\",\"afterReceivedAtMs\":1735689600100,\"afterMessageId\":\"message-10\"}]}";

    private const string SyncResponseJson =
        "{\"requestId\":\"sync-01\",\"succeeded\":true,\"serverTimeMs\":1735689600200,\"conversations\":[],\"conversationsHasMore\":false,\"catchUps\":[],\"resetsRequired\":[]}";

    [Fact]
    public void GatewayReadsClientHistoryRequestGolden()
    {
        var value = JsonSerializer.Deserialize(
            HistoryRequestJson,
            GatewayJsonSerializerContext.Default.MessageHistoryRequest);

        Assert.NotNull(value);
        Assert.Equal("conversation-01", value.ConversationId);
        Assert.Equal(1_735_689_600_100, value.AfterReceivedAtMs);
        Assert.Equal("message-10", value.AfterMessageId);
    }

    [Fact]
    public void GatewayWritesClientHistoryResponseGolden()
    {
        var value = new MessageHistoryResponse
        {
            RequestId = "history-01",
            ConversationId = "conversation-01",
            Succeeded = true,
            NextCursor = new MessageHistoryCursor
            {
                ReceivedAtMs = 1_735_689_600_000,
                ChangedAtMs = 1_735_689_600_100,
                MessageId = "message-10"
            },
            HasMore = true
        };

        Assert.Equal(
            HistoryResponseJson,
            JsonSerializer.Serialize(
                value,
                GatewayJsonSerializerContext.Default.MessageHistoryResponse));
    }

    [Fact]
    public void GatewayReadsClientSyncRequestGolden()
    {
        var value = JsonSerializer.Deserialize(
            SyncRequestJson,
            GatewayJsonSerializerContext.Default.SyncBootstrapRequest);

        Assert.NotNull(value);
        Assert.Equal(1_735_689_600_100, value.Watermarks?.Single().AfterReceivedAtMs);
    }

    [Fact]
    public void GatewayWritesClientSyncResponseGolden()
    {
        var value = new SyncBootstrapResponse
        {
            RequestId = "sync-01",
            Succeeded = true,
            ServerTimeMs = 1_735_689_600_200
        };

        Assert.Equal(
            SyncResponseJson,
            JsonSerializer.Serialize(
                value,
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse));
    }

    [Fact]
    public void GatewayReadsLegacyHistoryRequestAndRetainsDefaults()
    {
        const string legacyJson =
            "{\"requestId\":\"history-legacy\",\"conversationId\":\"conversation-01\"}";

        var value = JsonSerializer.Deserialize(
            legacyJson,
            GatewayJsonSerializerContext.Default.MessageHistoryRequest);

        Assert.NotNull(value);
        Assert.Equal(50, value.Limit);
        Assert.Null(value.BeforeReceivedAtMs);
        Assert.Null(value.AfterReceivedAtMs);
    }

    [Fact]
    public void GatewayIgnoresUnknownOptionalRequestFields()
    {
        const string futureJson =
            "{\"requestId\":\"sync-future\",\"listLimit\":25,\"futureOptional\":{\"enabled\":true}}";

        var value = JsonSerializer.Deserialize(
            futureJson,
            GatewayJsonSerializerContext.Default.SyncBootstrapRequest);

        Assert.NotNull(value);
        Assert.Equal("sync-future", value.RequestId);
        Assert.Equal(25, value.ListLimit);
    }

    [Fact]
    public void GatewayWritesCorrelatableHistoryError()
    {
        var value = new MessageHistoryResponse
        {
            RequestId = "history-error",
            ConversationId = "conversation-01",
            Succeeded = false,
            ErrorCode = "response_too_large",
            ErrorMessage = "response exceeds the hard payload budget"
        };

        var json = JsonSerializer.Serialize(
            value,
            GatewayJsonSerializerContext.Default.MessageHistoryResponse);
        var roundTrip = JsonSerializer.Deserialize(
            json,
            GatewayJsonSerializerContext.Default.MessageHistoryResponse);

        Assert.NotNull(roundTrip);
        Assert.Equal("history-error", roundTrip.RequestId);
        Assert.Equal("conversation-01", roundTrip.ConversationId);
        Assert.Equal("response_too_large", roundTrip.ErrorCode);
    }

    [Fact]
    public void GatewayRejectsTruncatedClientPayload()
    {
        const string truncated =
            "{\"requestId\":\"sync-01\",\"watermarks\":[{";

        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize(
            truncated,
            GatewayJsonSerializerContext.Default.SyncBootstrapRequest));
    }
}
