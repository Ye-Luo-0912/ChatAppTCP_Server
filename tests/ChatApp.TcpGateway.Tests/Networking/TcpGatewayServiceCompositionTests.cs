using System.Net;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Networking.Sessions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using EphemeralPresenceTypingConsumerService = ChatApp.TcpGateway.Gateway.Messaging.EphemeralPresenceTypingConsumerService;
using RealtimeEventDispatcher = ChatApp.TcpGateway.Gateway.Messaging.RealtimeEventDispatcher;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// Composition test: gateway DI graph resolves and shares ephemeral registries.
/// </summary>
public sealed class TcpGatewayServiceCompositionTests
{
    [Fact]
    public void GatewayServiceGraphResolvesAndSharesEphemeralRegistries()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddOptions<TcpGatewayOptions>()
            .Configure(ConfigureValidGatewayOptions);
        services.AddOptions<RedisOptions>()
            .Configure(static o =>
            {
                o.ConnectionString = "127.0.0.1:6379";
                o.StartupTimeout = TimeSpan.FromSeconds(1);
            });

        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<GatewayMetrics>();
        services.AddSingleton<UserSessionRegistry>();
        services.AddSingleton<PresenceWatcherRegistry>();
        services.AddSingleton<TypingFanoutCoordinator>();
        services.AddSingleton<IGlobalPresenceStore, NoopGlobalPresenceStore>();
        services.AddSingleton<RealtimeEventDispatcher>();
        services.AddSingleton(new RealtimeIntegrationOptions { InstanceId = "composition-test" });
        services.AddSingleton<IRealtimeMessageBus, EmptyMessageBus>();

        services.AddGatewayInfrastructure();

        services.AddHostedService<TcpGatewayService>();
        services.AddHostedService<EphemeralPresenceTypingConsumerService>();

        using var provider = services.BuildServiceProvider(validateScopes: true);

        Assert.Same(
            provider.GetRequiredService<PresenceWatcherRegistry>(),
            provider.GetRequiredService<PresenceWatcherRegistry>());
        Assert.Same(
            provider.GetRequiredService<TypingFanoutCoordinator>(),
            provider.GetRequiredService<TypingFanoutCoordinator>());

        var hosted = provider.GetServices<IHostedService>();
        Assert.Contains(hosted, static h => h is TcpGatewayService);
        Assert.Contains(hosted, static h => h is EphemeralPresenceTypingConsumerService);
    }

    private static void ConfigureValidGatewayOptions(TcpGatewayOptions o)
    {
        o.ListenAddress = IPAddress.Loopback.ToString();
        o.Port = 8888;
        o.ListenBacklog = 8;
        o.MaxConnections = 8;
        o.ReceiveBufferSize = 1024;
        o.PipePauseWriterThreshold = 32 * 1024;
        o.PipeResumeWriterThreshold = 16 * 1024;
        o.OutboundQueueCapacity = 8;
        o.MaxOutboundQueuedBytes = 128 * 1024;
        o.AuthenticationTimeout = TimeSpan.FromSeconds(1);
        o.IdleTimeout = TimeSpan.FromSeconds(5);
        o.EnableEphemeralPresenceAndTyping = false;
    }

    private sealed class NoopGlobalPresenceStore : IGlobalPresenceStore
    {
        public Task SetOnlineAsync(long userId, string sessionId, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task SetOfflineAsync(long userId, string sessionId, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task RefreshOnlineAsync(long userId, string sessionId, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(false);

        public Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
            IReadOnlyList<long> userIds,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyDictionary<long, bool>>(
                userIds.ToDictionary(static id => id, static _ => false));
    }

    private sealed class EmptyMessageBus : IRealtimeMessageBus
    {
        public Task PublishIncomingMessageAsync(IncomingMessageCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishMessageReceiptAsync(MessageReceiptCommand command, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<MessageHistoryPage> QueryMessageHistoryAsync(MessageHistoryQuery query, CancellationToken ct = default) =>
            Task.FromResult(MessageHistoryPage.Failed(query.RequestId, "x", "x"));

        public Task<ConversationListPage> QueryConversationListAsync(ConversationListQuery query, CancellationToken ct = default) =>
            Task.FromResult(ConversationListPage.Failed(query.RequestId, "x", "x"));

        public Task<ConversationMarkReadResult> MarkConversationReadAsync(ConversationMarkReadCommand command, CancellationToken ct = default) =>
            Task.FromResult(ConversationMarkReadResult.Failed(command.RequestId, "x", "x"));

        public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(ConversationSetPrefsCommand command, CancellationToken ct = default) =>
            Task.FromResult(ConversationSetPrefsResult.Failed(command.RequestId, "x", "x"));

        public Task<GroupConversationResult> MutateGroupConversationAsync(GroupConversationCommand command, CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));

        public Task<MessageRecallResult> RecallMessageAsync(MessageRecallCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageRecallResult.Failed(command.RequestId, "x", "x"));

        public Task<MessageEditResult> EditMessageAsync(MessageEditCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageEditResult.Failed(command.RequestId, "x", "x"));

        public Task<MessageReactionResult> ReactToMessageAsync(MessageReactionCommand command, CancellationToken ct = default) =>
            Task.FromResult(MessageReactionResult.Failed(command.RequestId, "x", "x"));

        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(SyncBootstrapQuery query, CancellationToken ct = default) =>
            Task.FromResult(SyncBootstrapPage.Failed(query.RequestId, "x", "x"));

        public Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(long userId, string messageId, CancellationToken ct = default) =>
            Task.FromResult<RealtimeHistoryMessage?>(null);

        public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default) => Task.CompletedTask;

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
            PresenceAuthorizeQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(new PresenceAuthorizeResponse { AllowedUserIds = query.TargetUserIds });

        public Task ServePresenceAuthorizeAsync(
            Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.Zero);
    }
}
