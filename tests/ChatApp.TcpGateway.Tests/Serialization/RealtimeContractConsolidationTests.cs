using System.Globalization;
using System.Reflection;
using System.Text.Json;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using SharedAttachmentRef = ChatApp.Realtime.Abstractions.Messaging.AttachmentRef;
using SharedAttachmentWireStatus = ChatApp.Realtime.Abstractions.Messaging.AttachmentWireStatus;
using SharedConversationListCursor = ChatApp.Realtime.Abstractions.Conversations.ConversationListCursor;
using SharedConversationListItem = ChatApp.Realtime.Abstractions.Conversations.ConversationListItem;
using SharedConversationMemberItem = ChatApp.Realtime.Abstractions.Conversations.ConversationMemberItem;
using SharedConversationMemberRole = ChatApp.Realtime.Abstractions.Conversations.ConversationMemberRole;
using SharedConversationType = ChatApp.Realtime.Abstractions.Conversations.ConversationType;
using SharedMessageHistoryCursor = ChatApp.Realtime.Abstractions.Messaging.History.MessageHistoryCursor;
using TcpSharedMessageHistoryCursor = ChatApp.Shared.Protocol.Tcp.MessageHistoryCursor;
using SharedMessageReactionSummary = ChatApp.Realtime.Abstractions.Messaging.History.MessageReactionSummary;
using TcpMessageReactionSummary = ChatApp.Shared.Protocol.Tcp.MessageReactionSummary;
using TcpRelationshipCatchUp = ChatApp.Shared.Protocol.Tcp.RelationshipCatchUp;
using TcpRelationshipSyncWatermark = ChatApp.Shared.Protocol.Tcp.RelationshipSyncWatermark;
using TcpSyncCursorResetReason = ChatApp.Shared.Protocol.Tcp.TcpSyncCursorResetReason;
using SharedRelationshipCatchUp = ChatApp.Realtime.Abstractions.Sync.RelationshipCatchUp;
using SharedRelationshipChangeLogEntry = ChatApp.Realtime.Abstractions.Sync.RelationshipChangeLogEntry;
using SharedRelationshipChangeOperation = ChatApp.Realtime.Abstractions.Relationships.RelationshipChangeOperation;
using SharedRelationshipItem = ChatApp.Realtime.Abstractions.Relationships.RelationshipListItem;
using SharedRelationshipListResponse = ChatApp.Realtime.Abstractions.Relationships.RelationshipListResult;
using SharedRelationshipListType = ChatApp.Realtime.Abstractions.Relationships.RelationshipListType;
using SharedRelationshipOperation = ChatApp.Realtime.Abstractions.Relationships.RelationshipOperation;
using SharedRelationshipSyncWatermark = ChatApp.Realtime.Abstractions.Sync.RelationshipSyncWatermark;
using SharedSyncCursorResetReason = ChatApp.Realtime.Abstractions.Sync.SyncCursorResetReason;
using SharedSyncCursorResetRequired = ChatApp.Realtime.Abstractions.Sync.SyncCursorResetRequired;
using SharedConversationSyncWatermark = ChatApp.Realtime.Abstractions.Sync.ConversationSyncWatermark;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Serialization;

/// <summary>
/// Locks the ownership and wire boundary between Gateway and ChatApp.Realtime.Contracts 2.2.0.
/// A type is shared only when its meaning and JSON surface are identical.
/// </summary>
public sealed class RealtimeContractConsolidationTests
{
    private static readonly Assembly CoreAssembly = typeof(ChatMessage).Assembly;

    [Fact]
    public void Core_Does_Not_Define_Migrated_Realtime_Contracts()
    {
        string[] removedDuplicateNames =
        [
            "AttachmentRef",
            "AttachmentWireStatus",
            "ConversationListCursor",
            "ConversationListItem",
            "ConversationMemberItem",
            "ConversationMemberRole",
            "ConversationType",
            "MessageReactionSummary",
            "RelationshipCatchUp",
            "RelationshipChangeLogEntry",
            "RelationshipChangeOperation",
            "RelationshipListType",
            "RelationshipOperation",
            "RelationshipSyncWatermark",
            "SyncCursorResetReason",
            // These two Gateway aliases intentionally use the client-facing historical names.
            "RelationshipItem",
            "RelationshipListResponse"
        ];

        var duplicates = CoreAssembly
            .GetTypes()
            .Where(type => removedDuplicateNames.Contains(type.Name, StringComparer.Ordinal))
            .Select(type => type.FullName)
            .ToArray();

        Assert.Empty(duplicates);
    }

    [Fact]
    public void Gateway_Envelope_Properties_Reference_Shared_Types_Directly()
    {
        AssertPropertyType<ChatMessage>(
            nameof(ChatMessage.Attachments),
            typeof(IReadOnlyList<SharedAttachmentRef>));
        AssertPropertyType<MessageHistoryItem>(
            nameof(MessageHistoryItem.Reactions),
            typeof(IReadOnlyList<TcpMessageReactionSummary>));
        AssertPropertyType<ConversationListResponse>(
            nameof(ConversationListResponse.Items),
            typeof(IReadOnlyList<SharedConversationListItem>));
        AssertPropertyType<ConversationListResponse>(
            nameof(ConversationListResponse.NextCursor),
            typeof(SharedConversationListCursor));
        AssertPropertyType<CreateGroupResponse>(
            nameof(CreateGroupResponse.Members),
            typeof(IReadOnlyList<SharedConversationMemberItem>));
        AssertPropertyType<RelationshipCommandRequest>(
            nameof(RelationshipCommandRequest.Operation),
            typeof(SharedRelationshipOperation));
        AssertPropertyType<RelationshipListRequest>(
            nameof(RelationshipListRequest.ListType),
            typeof(SharedRelationshipListType));
        AssertPropertyType<SyncBootstrapRequest>(
            nameof(SyncBootstrapRequest.RelationshipWatermarks),
            typeof(IReadOnlyList<TcpRelationshipSyncWatermark>));
        AssertPropertyType<SyncBootstrapResponse>(
            nameof(SyncBootstrapResponse.RelationshipCatchUps),
            typeof(IReadOnlyList<TcpRelationshipCatchUp>));
        AssertPropertyType<SyncCursorResetRequired>(
            nameof(SyncCursorResetRequired.Reason),
            typeof(TcpSyncCursorResetReason));

        Assert.Equal(typeof(SharedRelationshipItem), typeof(RelationshipItem));
        Assert.Equal(typeof(SharedRelationshipListResponse), typeof(RelationshipListResponse));
    }

    [Fact]
    public void Shared_Enum_Name_Value_Surfaces_Are_Stable()
    {
        AssertEnumSurface<SharedAttachmentWireStatus>(
            ("Scanning", 0),
            ("Available", 1));
        AssertEnumSurface<SharedConversationMemberRole>(
            ("Owner", 1),
            ("Admin", 2),
            ("Member", 3));
        AssertEnumSurface<SharedConversationType>(
            ("Direct", 1),
            ("Group", 2));
        AssertEnumSurface<SharedRelationshipOperation>(
            ("SendFriendRequest", 1),
            ("AcceptFriendRequest", 2),
            ("DeclineFriendRequest", 3),
            ("RemoveFriend", 4),
            ("BlockUser", 5),
            ("UnblockUser", 6));
        AssertEnumSurface<SharedRelationshipListType>(
            ("Friends", 1),
            ("FriendRequests", 2),
            ("BlockedUsers", 3));
        AssertEnumSurface<SharedRelationshipChangeOperation>(
            ("Upsert", 0),
            ("Delete", 1));
        AssertEnumSurface<SharedSyncCursorResetReason>(
            ("MessageNotFound", 1),
            ("AheadOfTip", 2),
            ("MembershipLost", 3),
            ("GapTooLarge", 4),
            ("BeyondRetention", 5));
    }

    [Fact]
    public void Shared_Models_Keep_Gateway_CamelCase_Json_Goldens()
    {
        var attachment = new SharedAttachmentRef
        {
            AttachmentId = "att-1",
            FileName = "photo.png",
            ContentType = "image/png",
            SizeBytes = 42,
            Status = SharedAttachmentWireStatus.Available,
            DownloadApiHint = "att-1"
        };
        var attachmentJson = JsonSerializer.Serialize(
            attachment,
            GatewayJsonSerializerContext.Default.AttachmentRef);
        Assert.Equal(
            "{\"refVersion\":1,\"attachmentId\":\"att-1\",\"fileName\":\"photo.png\",\"contentType\":\"image/png\",\"sizeBytes\":42,\"status\":1,\"downloadApiHint\":\"att-1\"}",
            attachmentJson);

        var cursor = new SharedConversationListCursor(true, 120, 100, "conv-1");
        var cursorJson = JsonSerializer.Serialize(
            cursor,
            GatewayJsonSerializerContext.Default.ConversationListCursor);
        Assert.Equal(
            "{\"isPinned\":true,\"pinnedAtMs\":120,\"lastMessageAtMs\":100,\"conversationId\":\"conv-1\"}",
            cursorJson);

        var catchUp = new SharedRelationshipCatchUp
        {
            ListType = SharedRelationshipListType.Friends,
            Changes =
            [
                new SharedRelationshipChangeLogEntry
                {
                    ChangeSequence = 9,
                    Operation = SharedRelationshipChangeOperation.Upsert,
                    ResourceId = "friendship-1",
                    UserId = 42,
                    Status = "Accepted",
                    CreatedAtMs = 10,
                    OccurredAtMs = 11,
                    RequestId = "req-1"
                }
            ],
            NextSequence = 9,
            RetentionFloorSequence = 1
        };
        var catchUpJson = JsonSerializer.Serialize(
            catchUp,
            GatewayJsonSerializerContext.Default.RealtimeRelationshipCatchUp);
        Assert.Equal(
            "{\"listType\":1,\"changes\":[{\"changeSequence\":9,\"operation\":0,\"resourceId\":\"friendship-1\",\"userId\":42,\"status\":\"Accepted\",\"createdAtMs\":10,\"occurredAtMs\":11,\"requestId\":\"req-1\"}],\"hasMore\":false,\"nextSequence\":9,\"retentionFloorSequence\":1,\"resetRequired\":false}",
            catchUpJson);

        Assert.NotNull(JsonSerializer.Deserialize(
            attachmentJson,
            GatewayJsonSerializerContext.Default.AttachmentRef));
        Assert.NotNull(JsonSerializer.Deserialize(
            cursorJson,
            GatewayJsonSerializerContext.Default.ConversationListCursor));
        Assert.NotNull(JsonSerializer.Deserialize(
            catchUpJson,
            GatewayJsonSerializerContext.Default.RealtimeRelationshipCatchUp));
    }

    [Fact]
    public void Older_Gateway_Ignores_New_Relationship_Projection_Delta()
    {
        const string json = """
            {
              "resource": "friendship",
              "action": "Upsert",
              "resourceId": "1:2",
              "projection": {
                "schemaVersion": 1,
                "eventId": "relproj-1",
                "ownerUserId": 1,
                "listType": 2,
                "version": 1,
                "operation": 1,
                "resourceId": "1:2",
                "subjectUserId": 2,
                "actorUserId": 1,
                "occurredAtMs": 100
              }
            }
            """;

        var payload = JsonSerializer.Deserialize(
            json,
            GatewayJsonSerializerContext.Default.RealtimeDomainNotificationPayload);

        Assert.NotNull(payload);
        Assert.Equal("friendship", payload.Resource);
        Assert.Equal("Upsert", payload.Action);
        Assert.Equal("1:2", payload.ResourceId);
    }

    [Fact]
    public void Similar_But_Different_Wire_Models_Remain_Gateway_Owned()
    {
        Assert.NotEqual(typeof(ConversationSyncWatermark), typeof(SharedConversationSyncWatermark));
        AssertPropertyType<ConversationSyncWatermark>("AfterReceivedAtMs", typeof(long));
        Assert.Null(typeof(ConversationSyncWatermark).GetProperty("AfterChangedAtMs"));
        AssertPropertyType<SharedConversationSyncWatermark>("AfterChangedAtMs", typeof(long));

        Assert.Equal(typeof(TcpSharedMessageHistoryCursor), typeof(MessageHistoryCursor));
        Assert.Equal("ChatApp.Protocol.Tcp", typeof(MessageHistoryItem).Assembly.GetName().Name);
        // The Client/Gateway cursor is independent from the Realtime query cursor.
        Assert.NotEqual(typeof(MessageHistoryCursor), typeof(SharedMessageHistoryCursor));
        AssertPropertyType<MessageHistoryCursor>("ChangedAtMs", typeof(long?));
        AssertPropertyType<SharedMessageHistoryCursor>("ChangedAtMs", typeof(long?));

        Assert.NotEqual(typeof(SyncCursorResetRequired), typeof(SharedSyncCursorResetRequired));
        AssertPropertyType<SyncCursorResetRequired>("TipReceivedAtMs", typeof(long?));
        Assert.Null(typeof(SyncCursorResetRequired).GetProperty("TipChangedAtMs"));
        AssertPropertyType<SharedSyncCursorResetRequired>("TipChangedAtMs", typeof(long?));

        AssertPropertyType<AttachmentLifecycleUpdate>(nameof(AttachmentLifecycleUpdate.Status), typeof(short));
        Assert.Equal(
            typeof(SharedAttachmentWireStatus),
            typeof(SharedAttachmentRef).GetProperty(nameof(SharedAttachmentRef.Status))!.PropertyType);
    }

    private static void AssertPropertyType<T>(string propertyName, Type expectedType) =>
        Assert.Equal(expectedType, typeof(T).GetProperty(propertyName)!.PropertyType);

    private static void AssertEnumSurface<TEnum>(params (string Name, ulong Value)[] expected)
        where TEnum : struct, Enum
    {
        var actual = Enum.GetNames<TEnum>()
            .Select(name =>
                (name, Convert.ToUInt64(Enum.Parse<TEnum>(name), CultureInfo.InvariantCulture)))
            .ToArray();

        Assert.Equal(expected, actual);
    }
}
