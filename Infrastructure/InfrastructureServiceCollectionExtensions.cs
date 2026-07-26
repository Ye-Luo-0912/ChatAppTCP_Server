using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Push;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Core.Server;
using ChatApp.TcpGateway.Infrastructure.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Infrastructure.Routing;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Infrastructure.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace ChatApp.TcpGateway.Infrastructure;

public static class InfrastructureServiceCollectionExtensions
{
    public static IServiceCollection AddGatewayInfrastructure(
        this IServiceCollection services)
    {
        services.AddSingleton<RedisConnectionProvider>();
        services.AddSingleton<IHostedService>(
            static provider =>
                provider.GetRequiredService<RedisConnectionProvider>());

        services.AddSingleton<IAccessTokenStore, RedisAccessTokenStore>();
        services.AddSingleton<IDeviceSessionLeaseStore, RedisDeviceSessionLeaseStore>();
        services.AddSingleton<IResumeTokenStore, RedisResumeTokenStore>();
        services.AddSingleton<IRealtimeAuthenticator, RealtimeAuthenticator>();
        services.AddSingleton<IServerIdentity, ServerIdentity>();
        services.AddSingleton<IDirectConversationAuthorizer, CachedDirectConversationAuthorizer>();
        services.AddSingleton<IPushTokenStore, RedisPushTokenStore>();
        services.AddSingleton<IGatewayDirectory, RedisGatewayDirectory>();
        services.AddSingleton<IWatcherGatewayDirectory, RedisWatcherGatewayDirectory>();

        services.AddSingleton<IPayloadCodec<AuthenticationRequest>>(
            static _ => new JsonPayloadCodec<AuthenticationRequest>(
                GatewayJsonSerializerContext.Default.AuthenticationRequest));
        services.AddSingleton<IPayloadCodec<AuthenticationResponse>>(
            static _ => new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse));
        services.AddSingleton<IPayloadCodec<ChatMessage>>(
            static _ => new JsonPayloadCodec<ChatMessage>(
                GatewayJsonSerializerContext.Default.ChatMessage));
        services.AddSingleton<IPayloadCodec<MessageAcknowledgement>>(
            static _ => new JsonPayloadCodec<MessageAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageAcknowledgement));
        services.AddSingleton<IPayloadCodec<MessageReceiptRequest>>(
            static _ => new JsonPayloadCodec<MessageReceiptRequest>(
                GatewayJsonSerializerContext.Default.MessageReceiptRequest));
        services.AddSingleton<IPayloadCodec<MessageReceiptAcknowledgement>>(
            static _ => new JsonPayloadCodec<MessageReceiptAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageReceiptAcknowledgement));
        services.AddSingleton<IPayloadCodec<MessageReceiptUpdate>>(
            static _ => new JsonPayloadCodec<MessageReceiptUpdate>(
                GatewayJsonSerializerContext.Default.MessageReceiptUpdate));
        services.AddSingleton<IPayloadCodec<MessageHistoryRequest>>(
            static _ => new JsonPayloadCodec<MessageHistoryRequest>(
                GatewayJsonSerializerContext.Default.MessageHistoryRequest));
        services.AddSingleton<IPayloadCodec<MessageHistoryResponse>>(
            static _ => new JsonPayloadCodec<MessageHistoryResponse>(
                GatewayJsonSerializerContext.Default.MessageHistoryResponse));
        services.AddSingleton<IPayloadCodec<MessageHistoryItem[]>>(
            static _ => new JsonPayloadCodec<MessageHistoryItem[]>(
                GatewayJsonSerializerContext.Default.MessageHistoryItemArray));
        services.AddSingleton<IPayloadCodec<ConversationListRequest>>(
            static _ => new JsonPayloadCodec<ConversationListRequest>(
                GatewayJsonSerializerContext.Default.ConversationListRequest));
        services.AddSingleton<IPayloadCodec<ConversationListResponse>>(
            static _ => new JsonPayloadCodec<ConversationListResponse>(
                GatewayJsonSerializerContext.Default.ConversationListResponse));
        services.AddSingleton<IPayloadCodec<ConversationListItem[]>>(
            static _ => new JsonPayloadCodec<ConversationListItem[]>(
                GatewayJsonSerializerContext.Default.ConversationListItemArray));
        services.AddSingleton<IPayloadCodec<ConversationMarkReadRequest>>(
            static _ => new JsonPayloadCodec<ConversationMarkReadRequest>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadRequest));
        services.AddSingleton<IPayloadCodec<ConversationMarkReadResponse>>(
            static _ => new JsonPayloadCodec<ConversationMarkReadResponse>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadResponse));
        services.AddSingleton<IPayloadCodec<ConversationSetPrefsRequest>>(
            static _ => new JsonPayloadCodec<ConversationSetPrefsRequest>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsRequest));
        services.AddSingleton<IPayloadCodec<ConversationSetPrefsResponse>>(
            static _ => new JsonPayloadCodec<ConversationSetPrefsResponse>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsResponse));
        services.AddSingleton<IPayloadCodec<MemberJoinedUpdate>>(
            static _ => new JsonPayloadCodec<MemberJoinedUpdate>(
                GatewayJsonSerializerContext.Default.MemberJoinedUpdate));
        services.AddSingleton<IPayloadCodec<MemberLeftUpdate>>(
            static _ => new JsonPayloadCodec<MemberLeftUpdate>(
                GatewayJsonSerializerContext.Default.MemberLeftUpdate));
        services.AddSingleton<IPayloadCodec<MemberRemovedUpdate>>(
            static _ => new JsonPayloadCodec<MemberRemovedUpdate>(
                GatewayJsonSerializerContext.Default.MemberRemovedUpdate));
        services.AddSingleton<IPayloadCodec<RoleChangedUpdate>>(
            static _ => new JsonPayloadCodec<RoleChangedUpdate>(
                GatewayJsonSerializerContext.Default.RoleChangedUpdate));
        services.AddSingleton<IPayloadCodec<MessageRecallRequest>>(
            static _ => new JsonPayloadCodec<MessageRecallRequest>(
                GatewayJsonSerializerContext.Default.MessageRecallRequest));
        services.AddSingleton<IPayloadCodec<MessageRecallAcknowledgement>>(
            static _ => new JsonPayloadCodec<MessageRecallAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageRecallAcknowledgement));
        services.AddSingleton<IPayloadCodec<MessageRecalledUpdate>>(
            static _ => new JsonPayloadCodec<MessageRecalledUpdate>(
                GatewayJsonSerializerContext.Default.MessageRecalledUpdate));
        services.AddSingleton<IPayloadCodec<MessageEditRequest>>(
            static _ => new JsonPayloadCodec<MessageEditRequest>(
                GatewayJsonSerializerContext.Default.MessageEditRequest));
        services.AddSingleton<IPayloadCodec<MessageEditAcknowledgement>>(
            static _ => new JsonPayloadCodec<MessageEditAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageEditAcknowledgement));
        services.AddSingleton<IPayloadCodec<MessageEditedUpdate>>(
            static _ => new JsonPayloadCodec<MessageEditedUpdate>(
                GatewayJsonSerializerContext.Default.MessageEditedUpdate));
        services.AddSingleton<IPayloadCodec<AddReactionRequest>>(
            static _ => new JsonPayloadCodec<AddReactionRequest>(
                GatewayJsonSerializerContext.Default.AddReactionRequest));
        services.AddSingleton<IPayloadCodec<AddReactionAcknowledgement>>(
            static _ => new JsonPayloadCodec<AddReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.AddReactionAcknowledgement));
        services.AddSingleton<IPayloadCodec<ReactionAddedUpdate>>(
            static _ => new JsonPayloadCodec<ReactionAddedUpdate>(
                GatewayJsonSerializerContext.Default.ReactionAddedUpdate));
        services.AddSingleton<IPayloadCodec<RemoveReactionRequest>>(
            static _ => new JsonPayloadCodec<RemoveReactionRequest>(
                GatewayJsonSerializerContext.Default.RemoveReactionRequest));
        services.AddSingleton<IPayloadCodec<RemoveReactionAcknowledgement>>(
            static _ => new JsonPayloadCodec<RemoveReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.RemoveReactionAcknowledgement));
        services.AddSingleton<IPayloadCodec<ReactionRemovedUpdate>>(
            static _ => new JsonPayloadCodec<ReactionRemovedUpdate>(
                GatewayJsonSerializerContext.Default.ReactionRemovedUpdate));
        services.AddSingleton<IPayloadCodec<ConversationChanged>>(
            static _ => new JsonPayloadCodec<ConversationChanged>(
                GatewayJsonSerializerContext.Default.ConversationChanged));
        services.AddSingleton<IPayloadCodec<UnreadCountChanged>>(
            static _ => new JsonPayloadCodec<UnreadCountChanged>(
                GatewayJsonSerializerContext.Default.UnreadCountChanged));
        services.AddSingleton<IPayloadCodec<ConversationReadUpdate>>(
            static _ => new JsonPayloadCodec<ConversationReadUpdate>(
                GatewayJsonSerializerContext.Default.ConversationReadUpdate));
        services.AddSingleton<IPayloadCodec<SyncBootstrapRequest>>(
            static _ => new JsonPayloadCodec<SyncBootstrapRequest>(
                GatewayJsonSerializerContext.Default.SyncBootstrapRequest));
        services.AddSingleton<IPayloadCodec<SyncBootstrapResponse>>(
            static _ => new JsonPayloadCodec<SyncBootstrapResponse>(
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse));

        // 协议握手与连接管理
        services.AddSingleton<IPayloadCodec<ClientHello>>(
            static _ => new JsonPayloadCodec<ClientHello>(
                GatewayJsonSerializerContext.Default.ClientHello));
        services.AddSingleton<IPayloadCodec<ServerHello>>(
            static _ => new JsonPayloadCodec<ServerHello>(
                GatewayJsonSerializerContext.Default.ServerHello));
        services.AddSingleton<IPayloadCodec<GoAway>>(
            static _ => new JsonPayloadCodec<GoAway>(
                GatewayJsonSerializerContext.Default.GoAway));
        services.AddSingleton<IPayloadCodec<ResumeResponse>>(
            static _ => new JsonPayloadCodec<ResumeResponse>(
                GatewayJsonSerializerContext.Default.ResumeResponse));
        services.AddSingleton<IPayloadCodec<ProtocolErrorFrame>>(
            static _ => new JsonPayloadCodec<ProtocolErrorFrame>(
                GatewayJsonSerializerContext.Default.ProtocolErrorFrame));
        services.AddSingleton<IPayloadCodec<RelationshipListChangedUpdate>>(
            static _ => new JsonPayloadCodec<RelationshipListChangedUpdate>(
                GatewayJsonSerializerContext.Default.RelationshipListChangedUpdate));
        services.AddSingleton<IPayloadCodec<AttachmentLifecycleUpdate>>(
            static _ => new JsonPayloadCodec<AttachmentLifecycleUpdate>(
                GatewayJsonSerializerContext.Default.AttachmentLifecycleUpdate));

        return services;
    }
}
