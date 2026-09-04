using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Push;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Attachments;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Commands.Attachments;
using ChatApp.TcpGateway.Gateway.Commands.Push;
using ChatApp.TcpGateway.Gateway.Commands.Reactions;
using ChatApp.TcpGateway.Gateway.Commands.Messaging;
using ChatApp.TcpGateway.Gateway.Commands.Queries;
using ChatApp.TcpGateway.Gateway.Commands.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;
using ChatApp.TcpGateway.Gateway.Commands.Presence;
using ChatApp.TcpGateway.Gateway.Commands.Relationships;
using ChatApp.TcpGateway.Gateway.Commands.Calls;
using ChatApp.Shared.Protocol.Tcp;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RealtimeHistory = ChatApp.Realtime.Abstractions.Messaging.History;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// REL-E2E-4 关系只读列表读取集成测试：通过真实 TCP 连接验证
/// <c>RelationshipListRequest</c> → <c>RelationshipCommandHandler</c> →
/// <c>IRelationshipBackend.QueryListAsync</c> → 显式映射到 Shared
/// <c>TcpRelationshipListResponse</c> 的完整路径。
/// <para>
/// 覆盖 item 映射、分页（HasMore/NextCursor）、reset（gap/projection-changed）
/// 与 fail-closed（unavailable）语义，确保 Gateway 只用 Shared <c>TcpRelationship*</c>
/// 作为 wire 输入/输出，不把 Realtime 内部 DTO 直接序列化给客户端。
/// </para>
/// </summary>
public sealed class RelationshipListReadIntegrationTests
{
    [Fact(Timeout = 15_000)]
    public async Task ListRead_MapsRealtimeItems_ToSharedWireItems()
    {
        var backend = new DataRelationshipBackend
        {
            ListResult = RelationshipListBackendResult.Success(
                "req-1",
                [
                    new RelationshipListItem
                    {
                        UserId = 101,
                        ResourceId = "friend-1",
                        Status = "Accepted",
                        Message = null,
                        CreatedAtMs = 1_700_000_000_000
                    },
                    new RelationshipListItem
                    {
                        UserId = 102,
                        ResourceId = "friend-2",
                        Status = "Accepted",
                        Message = null,
                        CreatedAtMs = 1_700_000_000_100
                    }
                ],
                nextCursor: null,
                hasMore: false)
        };

        await using var env = await RelationshipEnv.BuildAsync(backend);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, env.Port, timeout.Token);
        await using var stream = client.GetStream();

        await env.AuthenticateAsync(stream, timeout.Token);

        var requestId = Guid.CreateVersion7().ToString("N");
        await env.WriteListRequestAsync(
            stream,
            new TcpRelationshipListRequest
            {
                RequestId = requestId,
                ListType = TcpRelationshipListType.Friends,
                PageSize = 50
            },
            timeout.Token);

        var frame = await RelationshipEnv.ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.RelationshipListResponse, frame.Command);
        var response = env.ListResponseCodec.Deserialize(new ReadOnlySequence<byte>(frame.Payload));
        Assert.NotNull(response);
        Assert.True(response.Succeeded);
        Assert.Null(response.ErrorCode);
        Assert.Equal(requestId, response.RequestId);
        Assert.Equal(TcpRelationshipListType.Friends, response.ListType);
        Assert.False(response.HasMore);
        Assert.Null(response.NextCursor);
        Assert.False(response.ResetRequired.GetValueOrDefault());

        Assert.Collection(
            response.Items,
            item =>
            {
                Assert.Equal(101, item.UserId);
                Assert.Equal("friend-1", item.ResourceId);
                Assert.Equal("Accepted", item.Status);
                Assert.Null(item.Message);
                Assert.Equal(1_700_000_000_000, item.CreatedAtMs);
            },
            item =>
            {
                Assert.Equal(102, item.UserId);
                Assert.Equal("friend-2", item.ResourceId);
                Assert.Equal("Accepted", item.Status);
                Assert.Equal(1_700_000_000_100, item.CreatedAtMs);
            });

        // 后端确实收到与 wire 一致的查询参数。
        Assert.Equal((long)RelationshipListType.Friends, (long)backend.LastListType!.Value);
        Assert.Equal(50, backend.LastPageSize!.Value);
        Assert.Null(backend.LastCursor);
    }

    [Fact(Timeout = 15_000)]
    public async Task ListRead_Paginates_WithNextCursor()
    {
        var backend = new DataRelationshipBackend
        {
            ListResult = RelationshipListBackendResult.Success(
                "req-2",
                [
                    new RelationshipListItem
                    {
                        UserId = 201,
                        ResourceId = "req-item-1",
                        Status = "Pending",
                        Message = "hi",
                        CreatedAtMs = 1_700_000_100_000
                    }
                ],
                nextCursor: "opaque-cursor-v2",
                hasMore: true)
        };

        await using var env = await RelationshipEnv.BuildAsync(backend);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, env.Port, timeout.Token);
        await using var stream = client.GetStream();

        await env.AuthenticateAsync(stream, timeout.Token);

        var requestId = Guid.CreateVersion7().ToString("N");
        await env.WriteListRequestAsync(
            stream,
            new TcpRelationshipListRequest
            {
                RequestId = requestId,
                ListType = TcpRelationshipListType.FriendRequests,
                PageSize = 10,
                Cursor = "opaque-cursor-v1"
            },
            timeout.Token);

        var frame = await RelationshipEnv.ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.RelationshipListResponse, frame.Command);
        var response = env.ListResponseCodec.Deserialize(new ReadOnlySequence<byte>(frame.Payload));
        Assert.NotNull(response);
        Assert.True(response.Succeeded);
        Assert.True(response.HasMore);
        Assert.Equal("opaque-cursor-v2", response.NextCursor);
        Assert.Equal("opaque-cursor-v1", backend.LastCursor);
        var item = Assert.Single(response.Items);
        Assert.Equal("req-item-1", item.ResourceId);
        Assert.Equal("hi", item.Message);
    }

    [Fact(Timeout = 15_000)]
    public async Task ListRead_GapDetected_SetsResetRequired()
    {
        var backend = new DataRelationshipBackend
        {
            ListResult = RelationshipListBackendResult.Failed(
                "req-3",
                TcpRelationshipListErrorCode.GapDetected,
                "投影版本缺口，需 reset。")
        };

        await using var env = await RelationshipEnv.BuildAsync(backend);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, env.Port, timeout.Token);
        await using var stream = client.GetStream();

        await env.AuthenticateAsync(stream, timeout.Token);

        var requestId = Guid.CreateVersion7().ToString("N");
        await env.WriteListRequestAsync(
            stream,
            new TcpRelationshipListRequest
            {
                RequestId = requestId,
                ListType = TcpRelationshipListType.BlockedUsers,
                Cursor = "opaque-cursor-x"
            },
            timeout.Token);

        var frame = await RelationshipEnv.ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.RelationshipListResponse, frame.Command);
        var response = env.ListResponseCodec.Deserialize(new ReadOnlySequence<byte>(frame.Payload));
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpRelationshipListErrorCode.GapDetected, response.ErrorCode);
        Assert.True(response.ResetRequired.GetValueOrDefault());
        Assert.Empty(response.Items);
    }

    [Fact(Timeout = 15_000)]
    public async Task ListRead_FailClosed_WhenProjectionUnavailable()
    {
        var backend = new DataRelationshipBackend
        {
            ListResult = RelationshipListBackendResult.Failed(
                "req-4",
                TcpRelationshipListErrorCode.ProjectionUnavailable,
                "投影片刻尚不可用。")
        };

        await using var env = await RelationshipEnv.BuildAsync(backend);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, env.Port, timeout.Token);
        await using var stream = client.GetStream();

        await env.AuthenticateAsync(stream, timeout.Token);

        var requestId = Guid.CreateVersion7().ToString("N");
        await env.WriteListRequestAsync(
            stream,
            new TcpRelationshipListRequest
            {
                RequestId = requestId,
                ListType = TcpRelationshipListType.Friends
            },
            timeout.Token);

        var frame = await RelationshipEnv.ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.RelationshipListResponse, frame.Command);
        var response = env.ListResponseCodec.Deserialize(new ReadOnlySequence<byte>(frame.Payload));
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpRelationshipListErrorCode.ProjectionUnavailable, response.ErrorCode);
        Assert.False(response.ResetRequired.GetValueOrDefault());
        Assert.Empty(response.Items);
    }

    [Fact(Timeout = 15_000)]
    public async Task ListRead_BadRequest_WhenPageSizeOutOfRange()
    {
        var backend = new DataRelationshipBackend();
        await using var env = await RelationshipEnv.BuildAsync(backend);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(IPAddress.Loopback, env.Port, timeout.Token);
        await using var stream = client.GetStream();

        await env.AuthenticateAsync(stream, timeout.Token);

        var requestId = Guid.CreateVersion7().ToString("N");
        await env.WriteListRequestAsync(
            stream,
            new TcpRelationshipListRequest
            {
                RequestId = requestId,
                ListType = TcpRelationshipListType.Friends,
                PageSize = 1000
            },
            timeout.Token);

        var frame = await RelationshipEnv.ReadFrameAsync(stream, timeout.Token);
        Assert.Equal(PacketCommand.RelationshipListResponse, frame.Command);
        var response = env.ListResponseCodec.Deserialize(new ReadOnlySequence<byte>(frame.Payload));
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpRelationshipListErrorCode.PageSizeOutOfRange, response.ErrorCode);
        // 语义：bad request 是请求非法，不推动水位，也不要求 reset。
        Assert.False(response.ResetRequired.GetValueOrDefault());
        // 后端不应被调用。
        Assert.Null(backend.LastListType);
    }

    /// <summary>
    /// 可配置数据的关系后端：QueryListAsync 返回预设结果并记录收到的查询参数。
    /// </summary>
    private sealed class DataRelationshipBackend : IRelationshipBackend
    {
        public RelationshipListBackendResult ListResult { get; set; } =
            RelationshipListBackendResult.Failed(
                "x", TcpRelationshipListErrorCode.ProjectionUnavailable, "未配置。");

        public RelationshipListType? LastListType { get; private set; }
        public int? LastPageSize { get; private set; }
        public string? LastCursor { get; private set; }

        public Task<RelationshipCommandBackendResult> MutateAsync(
            string requestId,
            long actorUserId,
            RelationshipOperation operation,
            long? targetUserId,
            string? message,
            string? requestIdToRespond,
            string? actorSessionId,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(RelationshipCommandBackendResult.Failed(
                requestId, "relationship_service_unavailable", "关系服务暂未配置。"));

        public Task<RelationshipListBackendResult> QueryListAsync(
            string requestId,
            long actorUserId,
            RelationshipListType listType,
            int? pageSize,
            string? cursor,
            CancellationToken cancellationToken = default)
        {
            LastListType = listType;
            LastPageSize = pageSize;
            LastCursor = cursor;
            // 模仿生产后端：回显客户端 requestId，业务字段取自预设 ListResult。
            var result = ListResult;
            return Task.FromResult(new RelationshipListBackendResult(
                requestId,
                result.Succeeded,
                result.ErrorCode,
                result.ErrorMessage,
                result.Items,
                result.NextCursor,
                result.HasMore));
        }
    }

    /// <summary>
    /// 构建带关系读取 handler 的完整 <see cref="TcpGatewayService"/> 测试环境。
    /// 复用 CommandDispatcher 全 handler 装配，保持与其它 Networking 集成测试一致。
    /// </summary>
    private sealed class RelationshipEnv : IAsyncDisposable
    {
        private readonly TcpGatewayService _service;

        public int Port { get; }
        public JsonPayloadCodec<TcpRelationshipListRequest> ListRequestCodec { get; }
        public JsonPayloadCodec<TcpRelationshipListResponse> ListResponseCodec { get; }
        public JsonPayloadCodec<AuthenticationRequest> AuthenticationRequestCodec { get; }
        public JsonPayloadCodec<AuthenticationResponse> AuthenticationResponseCodec { get; }

        private RelationshipEnv(
            TcpGatewayService service,
            int port,
            JsonPayloadCodec<TcpRelationshipListRequest> listRequestCodec,
            JsonPayloadCodec<TcpRelationshipListResponse> listResponseCodec,
            JsonPayloadCodec<AuthenticationRequest> authenticationRequestCodec,
            JsonPayloadCodec<AuthenticationResponse> authenticationResponseCodec)
        {
            _service = service;
            Port = port;
            ListRequestCodec = listRequestCodec;
            ListResponseCodec = listResponseCodec;
            AuthenticationRequestCodec = authenticationRequestCodec;
            AuthenticationResponseCodec = authenticationResponseCodec;
        }

        public static async Task<RelationshipEnv> BuildAsync(IRelationshipBackend relationshipBackend)
        {
            var port = ReserveLoopbackPort();
            var options = new TcpGatewayOptions
            {
                ListenAddress = IPAddress.Loopback.ToString(),
                Port = port,
                ListenBacklog = 8,
                MaxConnections = 8,
                ReceiveBufferSize = 1024,
                PipePauseWriterThreshold = 32 * 1024,
                PipeResumeWriterThreshold = 16 * 1024,
                OutboundQueueCapacity = 8,
                MaxOutboundQueuedBytes = 128 * 1024,
                AuthenticationTimeout = TimeSpan.FromSeconds(2),
                IdleTimeout = TimeSpan.FromSeconds(10),
                HeartbeatScanInterval = TimeSpan.FromMilliseconds(200),
                SendTimeout = TimeSpan.FromSeconds(2),
                MaxPacketsPerSecond = 40,
                MaxInboundBytesPerSecond = 256 * 1024,
                MaxInboundPayloadBytes = PacketProtocol.MaxPayloadSize,
                RequireClientHello = false
            };

            var metrics = new GatewayMetrics();
            var userSessions = new UserSessionRegistry();
            var messageBus = new StubMessageBus();
            var pushStore = new InMemoryPushTokenStore();

            var authenticationRequestCodec = new JsonPayloadCodec<AuthenticationRequest>(
                GatewayJsonSerializerContext.Default.AuthenticationRequest);
            var authenticationResponseCodec = new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse);
            var chatMessageCodec = new JsonPayloadCodec<ChatMessage>(
                GatewayJsonSerializerContext.Default.ChatMessage);
            var acknowledgementCodec = new JsonPayloadCodec<MessageAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageAcknowledgement);
            var receiptRequestCodec = new JsonPayloadCodec<MessageReceiptRequest>(
                GatewayJsonSerializerContext.Default.MessageReceiptRequest);
            var receiptAcknowledgementCodec = new JsonPayloadCodec<MessageReceiptAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageReceiptAcknowledgement);
            var historyRequestCodec = new JsonPayloadCodec<MessageHistoryRequest>(
                GatewayJsonSerializerContext.Default.MessageHistoryRequest);
            var historyResponseCodec = new JsonPayloadCodec<MessageHistoryResponse>(
                GatewayJsonSerializerContext.Default.MessageHistoryResponse);
            var historyItemCodec = new JsonPayloadCodec<MessageHistoryItem[]>(
                GatewayJsonSerializerContext.Default.MessageHistoryItemArray);
            var conversationListRequestCodec = new JsonPayloadCodec<ConversationListRequest>(
                GatewayJsonSerializerContext.Default.ConversationListRequest);
            var conversationListResponseCodec = new JsonPayloadCodec<ConversationListResponse>(
                GatewayJsonSerializerContext.Default.ConversationListResponse);
            var conversationListItemCodec = new JsonPayloadCodec<ChatApp.Realtime.Abstractions.Conversations.ConversationListItem[]>(
                GatewayJsonSerializerContext.Default.ConversationListItemArray);
            var conversationMarkReadRequestCodec = new JsonPayloadCodec<ConversationMarkReadRequest>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadRequest);
            var conversationMarkReadResponseCodec = new JsonPayloadCodec<ConversationMarkReadResponse>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadResponse);
            var conversationSetPrefsRequestCodec = new JsonPayloadCodec<ConversationSetPrefsRequest>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsRequest);
            var conversationSetPrefsResponseCodec = new JsonPayloadCodec<ConversationSetPrefsResponse>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsResponse);
            var messageRecallRequestCodec = new JsonPayloadCodec<MessageRecallRequest>(
                GatewayJsonSerializerContext.Default.MessageRecallRequest);
            var messageRecallAcknowledgementCodec = new JsonPayloadCodec<MessageRecallAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageRecallAcknowledgement);
            var messageEditRequestCodec = new JsonPayloadCodec<MessageEditRequest>(
                GatewayJsonSerializerContext.Default.MessageEditRequest);
            var messageEditAcknowledgementCodec = new JsonPayloadCodec<MessageEditAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageEditAcknowledgement);
            var syncBootstrapRequestCodec = new JsonPayloadCodec<SyncBootstrapRequest>(
                GatewayJsonSerializerContext.Default.SyncBootstrapRequest);
            var syncBootstrapResponseCodec = new JsonPayloadCodec<SyncBootstrapResponse>(
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse);

            var registerPushRequestCodec = new JsonPayloadCodec<RegisterPushTokenRequest>(
                GatewayJsonSerializerContext.Default.RegisterPushTokenRequest);
            var registerPushResponseCodec = new JsonPayloadCodec<RegisterPushTokenResponse>(
                GatewayJsonSerializerContext.Default.RegisterPushTokenResponse);
            var unregisterPushRequestCodec = new JsonPayloadCodec<UnregisterPushTokenRequest>(
                GatewayJsonSerializerContext.Default.UnregisterPushTokenRequest);
            var unregisterPushResponseCodec = new JsonPayloadCodec<UnregisterPushTokenResponse>(
                GatewayJsonSerializerContext.Default.UnregisterPushTokenResponse);
            var addReactionRequestCodec = new JsonPayloadCodec<AddReactionRequest>(
                GatewayJsonSerializerContext.Default.AddReactionRequest);
            var addReactionAckCodec = new JsonPayloadCodec<AddReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.AddReactionAcknowledgement);
            var removeReactionRequestCodec = new JsonPayloadCodec<RemoveReactionRequest>(
                GatewayJsonSerializerContext.Default.RemoveReactionRequest);
            var removeReactionAckCodec = new JsonPayloadCodec<RemoveReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.RemoveReactionAcknowledgement);

            var pushHandler = new PushTokenCommandHandler(
                registerPushRequestCodec,
                registerPushResponseCodec,
                unregisterPushRequestCodec,
                unregisterPushResponseCodec,
                metrics,
                NullLogger<PushTokenCommandHandler>.Instance,
                pushStore);
            var reactionHandler = new ReactionCommandHandler(
                messageBus,
                addReactionRequestCodec,
                addReactionAckCodec,
                removeReactionRequestCodec,
                removeReactionAckCodec,
                metrics,
                TimeProvider.System,
                NullLogger<ReactionCommandHandler>.Instance);
            var messagingHandler = new MessagingCommandHandler(
                messageBus,
                chatMessageCodec,
                acknowledgementCodec,
                receiptRequestCodec,
                receiptAcknowledgementCodec,
                messageRecallRequestCodec,
                messageRecallAcknowledgementCodec,
                messageEditRequestCodec,
                messageEditAcknowledgementCodec,
                metrics,
                TimeProvider.System,
                NullLogger<MessagingCommandHandler>.Instance,
                Options.Create(options));
            var historyQueryHandler = new HistoryQueryCommandHandler(
                messageBus,
                historyRequestCodec,
                historyResponseCodec,
                historyItemCodec,
                conversationListRequestCodec,
                conversationListResponseCodec,
                conversationListItemCodec,
                syncBootstrapRequestCodec,
                syncBootstrapResponseCodec,
                metrics,
                NullLogger<HistoryQueryCommandHandler>.Instance);
            var conversationPrefsHandler = new ConversationPrefsCommandHandler(
                messageBus,
                conversationMarkReadRequestCodec,
                conversationMarkReadResponseCodec,
                conversationSetPrefsRequestCodec,
                conversationSetPrefsResponseCodec,
                metrics,
                NullLogger<ConversationPrefsCommandHandler>.Instance);

            var createGroupRequestCodec = new JsonPayloadCodec<CreateGroupRequest>(
                GatewayJsonSerializerContext.Default.CreateGroupRequest);
            var createGroupResponseCodec = new JsonPayloadCodec<CreateGroupResponse>(
                GatewayJsonSerializerContext.Default.CreateGroupResponse);
            var addGroupMembersRequestCodec = new JsonPayloadCodec<AddGroupMembersRequest>(
                GatewayJsonSerializerContext.Default.AddGroupMembersRequest);
            var addGroupMembersResponseCodec = new JsonPayloadCodec<AddGroupMembersResponse>(
                GatewayJsonSerializerContext.Default.AddGroupMembersResponse);
            var removeGroupMemberRequestCodec = new JsonPayloadCodec<RemoveGroupMemberRequest>(
                GatewayJsonSerializerContext.Default.RemoveGroupMemberRequest);
            var removeGroupMemberResponseCodec = new JsonPayloadCodec<RemoveGroupMemberResponse>(
                GatewayJsonSerializerContext.Default.RemoveGroupMemberResponse);
            var leaveGroupRequestCodec = new JsonPayloadCodec<LeaveGroupRequest>(
                GatewayJsonSerializerContext.Default.LeaveGroupRequest);
            var leaveGroupResponseCodec = new JsonPayloadCodec<LeaveGroupResponse>(
                GatewayJsonSerializerContext.Default.LeaveGroupResponse);
            var changeMemberRoleRequestCodec = new JsonPayloadCodec<ChangeMemberRoleRequest>(
                GatewayJsonSerializerContext.Default.ChangeMemberRoleRequest);
            var changeMemberRoleResponseCodec = new JsonPayloadCodec<ChangeMemberRoleResponse>(
                GatewayJsonSerializerContext.Default.ChangeMemberRoleResponse);
            var listGroupMembersRequestCodec = new JsonPayloadCodec<ListGroupMembersRequest>(
                GatewayJsonSerializerContext.Default.ListGroupMembersRequest);
            var listGroupMembersResponseCodec = new JsonPayloadCodec<ListGroupMembersResponse>(
                GatewayJsonSerializerContext.Default.ListGroupMembersResponse);
            var messageReadReceiptQueryRequestCodec = new JsonPayloadCodec<MessageReadReceiptQueryRequest>(
                GatewayJsonSerializerContext.Default.MessageReadReceiptQueryRequest);
            var messageReadReceiptQueryResponseCodec = new JsonPayloadCodec<MessageReadReceiptQueryResponse>(
                GatewayJsonSerializerContext.Default.MessageReadReceiptQueryResponse);
            var dissolveGroupRequestCodec = new JsonPayloadCodec<DissolveGroupRequest>(
                GatewayJsonSerializerContext.Default.DissolveGroupRequest);
            var dissolveGroupResponseCodec = new JsonPayloadCodec<DissolveGroupResponse>(
                GatewayJsonSerializerContext.Default.DissolveGroupResponse);
            var groupHandler = new GroupCommandHandler(
                messageBus,
                createGroupRequestCodec,
                createGroupResponseCodec,
                addGroupMembersRequestCodec,
                addGroupMembersResponseCodec,
                removeGroupMemberRequestCodec,
                removeGroupMemberResponseCodec,
                leaveGroupRequestCodec,
                leaveGroupResponseCodec,
                changeMemberRoleRequestCodec,
                changeMemberRoleResponseCodec,
                listGroupMembersRequestCodec,
                listGroupMembersResponseCodec,
                messageReadReceiptQueryRequestCodec,
                messageReadReceiptQueryResponseCodec,
                dissolveGroupRequestCodec,
                dissolveGroupResponseCodec,
                metrics,
                NullLogger<GroupCommandHandler>.Instance);

            var typingNotifyCodec = new JsonPayloadCodec<TypingNotify>(
                GatewayJsonSerializerContext.Default.TypingNotify);
            var typingFanout = new TypingFanoutCoordinator(TimeProvider.System);
            var typingHandler = new TypingCommandHandler(
                typingNotifyCodec,
                typingFanout,
                directConversationAuthorizer: null,
                Options.Create(options),
                NullLogger<TypingCommandHandler>.Instance);

            var presenceQueryRequestCodec = new JsonPayloadCodec<PresenceQueryRequest>(
                GatewayJsonSerializerContext.Default.PresenceQueryRequest);
            var presenceUnwatchRequestCodec = new JsonPayloadCodec<PresenceUnwatchRequest>(
                GatewayJsonSerializerContext.Default.PresenceUnwatchRequest);
            var presenceSnapshotResponseCodec = new JsonPayloadCodec<PresenceSnapshotResponse>(
                GatewayJsonSerializerContext.Default.PresenceSnapshotResponse);

            var typingUpdateCodec = new JsonPayloadCodec<TypingUpdate>(
                GatewayJsonSerializerContext.Default.TypingUpdate);
            var presenceChangedCodec = new JsonPayloadCodec<PresenceChanged>(
                GatewayJsonSerializerContext.Default.PresenceChanged);

            var integrationOptions = new RealtimeIntegrationOptions { InstanceId = "relationship-read-test" };
            var globalPresence = new NoopGlobalPresenceStore();
            var presenceWatchers = new PresenceWatcherRegistry();
            var presenceHandler = new PresenceCommandHandler(
                Options.Create(options),
                messageBus,
                integrationOptions,
                globalPresence,
                userSessions,
                presenceWatchers,
                NullWatcherGatewayDirectory.Instance,
                presenceQueryRequestCodec,
                presenceUnwatchRequestCodec,
                presenceSnapshotResponseCodec,
                metrics,
                NullLogger<PresenceCommandHandler>.Instance);

            var attachmentHandler = new AttachmentCommandHandler(
                new StubAttachmentBackend(NullLogger<StubAttachmentBackend>.Instance),
                new JsonPayloadCodec<AttachmentFinalizeRequest>(
                    GatewayJsonSerializerContext.Default.AttachmentFinalizeRequest),
                new JsonPayloadCodec<AttachmentFinalizeResponse>(
                    GatewayJsonSerializerContext.Default.AttachmentFinalizeResponse),
                new JsonPayloadCodec<AttachmentDownloadAuthorizeRequest>(
                    GatewayJsonSerializerContext.Default.AttachmentDownloadAuthorizeRequest),
                new JsonPayloadCodec<AttachmentDownloadAuthorizeResponse>(
                    GatewayJsonSerializerContext.Default.AttachmentDownloadAuthorizeResponse),
                metrics,
                NullLogger<AttachmentCommandHandler>.Instance);

            var listRequestCodec = new JsonPayloadCodec<TcpRelationshipListRequest>(
                GatewayJsonSerializerContext.Default.TcpRelationshipListRequest);
            var listResponseCodec = new JsonPayloadCodec<TcpRelationshipListResponse>(
                GatewayJsonSerializerContext.Default.TcpRelationshipListResponse);
            var relationshipHandler = new RelationshipCommandHandler(
                relationshipBackend,
                new JsonPayloadCodec<RelationshipCommandRequest>(
                    GatewayJsonSerializerContext.Default.RelationshipCommandRequest),
                new JsonPayloadCodec<RelationshipCommandResponse>(
                    GatewayJsonSerializerContext.Default.RelationshipCommandResponse),
                listRequestCodec,
                listResponseCodec,
                metrics,
                NullLogger<RelationshipCommandHandler>.Instance);

            var callHandler = new CallCommandHandler(
                new StubCallBackend(NullLogger<StubCallBackend>.Instance),
                CallSignalingIntegrationTests.DisabledGroupRelay(),
                new JsonPayloadCodec<TcpCallCommandRequest>(
                    GatewayJsonSerializerContext.Default.TcpCallCommandRequest),
                new JsonPayloadCodec<TcpCallCommandResponse>(
                    GatewayJsonSerializerContext.Default.TcpCallCommandResponse),
                new JsonPayloadCodec<TcpCallSignal>(
                    GatewayJsonSerializerContext.Default.TcpCallSignal),
                userSessions,
                metrics,
                NullLogger<CallCommandHandler>.Instance);

            var dispatcher = new CommandDispatcher(
                pushHandler,
                reactionHandler,
                messagingHandler,
                historyQueryHandler,
                conversationPrefsHandler,
                groupHandler,
                typingHandler,
                presenceHandler,
                attachmentHandler,
                relationshipHandler,
                callHandler);

            var service = new TcpGatewayService(
                Options.Create(options),
                new FakeAuthenticator(),
                authenticationRequestCodec,
                authenticationResponseCodec,
                acknowledgementCodec,
                typingUpdateCodec,
                presenceChangedCodec,
                messageBus,
                integrationOptions,
                new NoopLeaseStore(),
                globalPresence,
                userSessions,
                presenceWatchers,
                typingFanout,
                metrics,
                TimeProvider.System,
                NullLogger<TcpGatewayService>.Instance,
                NullLogger<TcpClientSession>.Instance,
                commandDispatcher: dispatcher);

            await service.StartAsync(CancellationToken.None);

            return new RelationshipEnv(
                service,
                port,
                listRequestCodec,
                listResponseCodec,
                authenticationRequestCodec,
                authenticationResponseCodec);
        }

        public async Task AuthenticateAsync(Stream stream, CancellationToken cancellationToken)
        {
            await WriteFrameAsync(
                stream,
                PacketCommand.AuthenticationRequest,
                AuthenticationRequestCodec,
                new AuthenticationRequest
                {
                    AccessToken = "valid-token",
                    DeviceIdHash = 7
                },
                cancellationToken);

            var frame = await ReadFrameAsync(stream, cancellationToken);
            Assert.Equal(PacketCommand.AuthenticationResponse, frame.Command);
            var response = AuthenticationResponseCodec.Deserialize(new ReadOnlySequence<byte>(frame.Payload));
            Assert.NotNull(response);
            Assert.True(response.Success);
            Assert.Equal(42, response.UserId);
        }

        public ValueTask WriteListRequestAsync(
            Stream stream,
            TcpRelationshipListRequest request,
            CancellationToken cancellationToken) =>
            WriteFrameAsync(stream, PacketCommand.RelationshipListRequest, ListRequestCodec, request, cancellationToken);

        public async ValueTask DisposeAsync() =>
            await _service.StopAsync(CancellationToken.None);

        private static int ReserveLoopbackPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            try
            {
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            finally
            {
                listener.Stop();
            }
        }

        public static async ValueTask WriteFrameAsync<T>(
            Stream stream,
            PacketCommand command,
            JsonPayloadCodec<T> codec,
            T value,
            CancellationToken cancellationToken)
        {
            var payload = new ArrayBufferWriter<byte>();
            codec.Serialize(payload, value);
            var frame = new byte[PacketProtocol.HeaderSize + payload.WrittenCount];
            PacketParser.WriteHeader(frame, command, payload.WrittenCount);
            payload.WrittenSpan.CopyTo(frame.AsSpan(PacketProtocol.HeaderSize));
            await stream.WriteAsync(frame, cancellationToken);
        }

        public static async ValueTask<ReceivedFrame> ReadFrameAsync(
            Stream stream,
            CancellationToken cancellationToken)
        {
            var header = new byte[PacketProtocol.HeaderSize];
            await stream.ReadExactlyAsync(header, cancellationToken);
            Assert.Equal(
                PacketProtocol.MagicNumber,
                BinaryPrimitives.ReadUInt32LittleEndian(header));

            var command = (PacketCommand)BinaryPrimitives.ReadUInt16LittleEndian(
                header.AsSpan(PacketProtocol.CommandOffset));
            var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
                header.AsSpan(PacketProtocol.LengthOffset));
            Assert.InRange(payloadLength, 0, PacketProtocol.MaxPayloadSize);

            var payload = new byte[payloadLength];
            if (payloadLength != 0)
            {
                await stream.ReadExactlyAsync(payload, cancellationToken);
            }

            return new ReceivedFrame(command, payload);
        }
    }

    private sealed record ReceivedFrame(PacketCommand Command, byte[] Payload);

    private sealed class FakeAuthenticator : IRealtimeAuthenticator
    {
        public ValueTask<RealtimeAuthenticationResult> AuthenticateAsync(
            string accessToken,
            ulong? deviceIdHash,
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(
                RealtimeAuthenticationResult.Success(
                    userId: 42,
                    sessionId: "relationship-read-test",
                    userName: "test-user",
                    deviceIdHash,
                    roles: []));
    }

    private sealed class NoopLeaseStore : IDeviceSessionLeaseStore
    {
        public ValueTask<TakeOverResult> TakeOverAsync(
            long userId,
            ulong deviceIdHash,
            string sessionId,
            string transportId,
            string leaseOwnerToken,
            TimeSpan ttl,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(TakeOverResult.NoPreviousLease());

        public ValueTask ReleaseIfOwnerAsync(
            long userId,
            ulong deviceIdHash,
            string leaseOwnerToken,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<bool> RefreshIfOwnerAsync(
            long userId,
            ulong deviceIdHash,
            string leaseOwnerToken,
            TimeSpan ttl,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);

        public ValueTask<string?> GetCurrentSessionIdAsync(
            long userId,
            ulong deviceIdHash,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult<string?>(null);
    }

    private sealed class StubMessageBus : IRealtimeMessageBus
    {
        public Task PublishIncomingMessageAsync(
            IncomingMessageCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishMessageReceiptAsync(
            MessageReceiptCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<MessageHistoryPage> QueryMessageHistoryAsync(
            MessageHistoryQuery query, CancellationToken ct = default) =>
            Task.FromResult(MessageHistoryPage.Failed(query.RequestId, "x", "x"));

        public Task<ConversationListPage> QueryConversationListAsync(
            ConversationListQuery query, CancellationToken ct = default) =>
            Task.FromResult(ConversationListPage.Failed(query.RequestId, "x", "x"));

        public Task<ConversationMarkReadResult> MarkConversationReadAsync(
            ConversationMarkReadCommand command, CancellationToken ct = default) =>
            Task.FromResult(ConversationMarkReadResult.Failed(command.RequestId, "x", "x"));

        public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(
            ConversationSetPrefsCommand command, CancellationToken ct = default) =>
            Task.FromResult(ConversationSetPrefsResult.Failed(command.RequestId, "x", "x"));

        public Task<GroupConversationResult> MutateGroupConversationAsync(
            GroupConversationCommand command, CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));

        public Task<GroupConversationResult> QueryReadReceiptsAsync(
            GroupConversationCommand command, CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));

        public Task<AttachmentFinalizeResult> FinalizeAttachmentUploadAsync(
            AttachmentFinalizeCommand command, CancellationToken ct = default) =>
            Task.FromResult(AttachmentFinalizeResult.Failed(command.RequestId, "x", "x"));

        public Task<AttachmentDownloadAuthorizeResult> AuthorizeAttachmentDownloadAsync(
            AttachmentDownloadAuthorizeCommand command, CancellationToken ct = default) =>
            Task.FromResult(AttachmentDownloadAuthorizeResult.Failed(command.RequestId, "x", "x"));

        public Task<RelationshipCommandResult> MutateRelationshipAsync(
            RelationshipCommand command, CancellationToken ct = default) =>
            Task.FromResult(RelationshipCommandResult.Failed(command.RequestId, "x", "x"));

        public Task<RelationshipListResult> QueryRelationshipListAsync(
            RelationshipListQuery query, CancellationToken ct = default) =>
            Task.FromResult(RelationshipListResult.Failed(query.RequestId, "x", "x"));

        public Task<MessageRecallResult> RecallMessageAsync(
            MessageRecallCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageRecallResult.Failed(command.RequestId, "x", "x"));

        public Task<MessageEditResult> EditMessageAsync(
            MessageEditCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageEditResult.Failed(command.RequestId, "x", "x"));

        public Task<MessageReactionResult> ReactToMessageAsync(
            MessageReactionCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageReactionResult.Failed(command.RequestId, "x", "x"));

        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
            SyncBootstrapQuery query, CancellationToken ct = default) =>
            Task.FromResult(SyncBootstrapPage.Failed(query.RequestId, "x", "x"));

        public Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(
            long userId, string messageId, CancellationToken ct = default) =>
            Task.FromResult<RealtimeHistoryMessage?>(null);

        public Task<CallProcessResult> SendCallCommandAsync(
            CallCommand command, CancellationToken ct = default) =>
            Task.FromResult(CallProcessResult.Failed(CallErrorCode.StateStoreUnavailable, "unavailable"));

        public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeEventsAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<RealtimeEventDelivery> ConsumeAccountCleanupEventsAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public Task PublishEphemeralTypingAsync(EphemeralTypingEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishEphemeralPresenceAsync(EphemeralPresenceEvent evt, CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<EphemeralTypingEvent> ConsumeEphemeralTypingAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<EphemeralPresenceEvent> ConsumeEphemeralPresenceAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public Task<PresenceAuthorizeResponse> AuthorizePresenceAsync(
            PresenceAuthorizeQuery query, CancellationToken ct = default) =>
            Task.FromResult(new PresenceAuthorizeResponse { AllowedUserIds = query.TargetUserIds });

        public Task ServePresenceAuthorizeAsync(
            Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishPushDeliveryAsync(PushDeliveryCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<PushDelivery> ConsumePushDeliveriesAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            await Task.CompletedTask;
            yield break;
        }

        public Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.Zero);
    }
}