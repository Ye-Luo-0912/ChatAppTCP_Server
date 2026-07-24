using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Infrastructure.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
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
        services.AddSingleton<IRealtimeAuthenticator, RealtimeAuthenticator>();

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
        services.AddSingleton<IPayloadCodec<ConversationListRequest>>(
            static _ => new JsonPayloadCodec<ConversationListRequest>(
                GatewayJsonSerializerContext.Default.ConversationListRequest));
        services.AddSingleton<IPayloadCodec<ConversationListResponse>>(
            static _ => new JsonPayloadCodec<ConversationListResponse>(
                GatewayJsonSerializerContext.Default.ConversationListResponse));
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
        services.AddSingleton<IPayloadCodec<ConversationChanged>>(
            static _ => new JsonPayloadCodec<ConversationChanged>(
                GatewayJsonSerializerContext.Default.ConversationChanged));
        services.AddSingleton<IPayloadCodec<UnreadCountChanged>>(
            static _ => new JsonPayloadCodec<UnreadCountChanged>(
                GatewayJsonSerializerContext.Default.UnreadCountChanged));
        services.AddSingleton<IPayloadCodec<SyncBootstrapRequest>>(
            static _ => new JsonPayloadCodec<SyncBootstrapRequest>(
                GatewayJsonSerializerContext.Default.SyncBootstrapRequest));
        services.AddSingleton<IPayloadCodec<SyncBootstrapResponse>>(
            static _ => new JsonPayloadCodec<SyncBootstrapResponse>(
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse));

        return services;
    }
}
