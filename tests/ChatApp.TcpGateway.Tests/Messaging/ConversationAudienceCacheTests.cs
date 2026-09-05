using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Gateway.Messaging.Realtime;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Messaging;

/// <summary>
/// P1-2：会话受众缓存（ConversationAudienceCache）测试。
/// 验证：冷启动拉取、命中不重复拉取、AudienceVersion 不匹配触发重拉、
/// 拉取失败 fail-closed 抛异常、TTL 过期触发重拉。
/// </summary>
public sealed class ConversationAudienceCacheTests
{
    private const string ConversationId = "grp:audience-test";

    [Fact]
    public async Task GetOrResolveAsync_OnColdCache_QueriesAndReturnsMembers()
    {
        var bus = new StubMessageBus(version: 7, members: [42, 43, 44]);

        var cache = new ConversationAudienceCache(bus);

        var members = await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: 7, CancellationToken.None);

        Assert.Equal([42, 43, 44], members);
        Assert.Equal(1, bus.QueryCount);
    }

    [Fact]
    public async Task GetOrResolveAsync_CachedAndVersionMatch_DoesNotRequery()
    {
        var bus = new StubMessageBus(version: 7, members: [42, 43]);

        var cache = new ConversationAudienceCache(bus);

        var first = await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: 7, CancellationToken.None);
        var second = await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: 7, CancellationToken.None);

        Assert.Equal([42, 43], first);
        Assert.Equal([42, 43], second);
        // 快速路径命中：仅首次拉取。
        Assert.Equal(1, bus.QueryCount);
    }

    [Fact]
    public async Task GetOrResolveAsync_VersionMismatch_Refetches()
    {
        var bus = new StubMessageBus(version: 7, members: [42, 43]);

        var cache = new ConversationAudienceCache(bus);

        // 首次以匹配版本填充缓存。
        var first = await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: 7, CancellationToken.None);
        Assert.Equal([42, 43], first);
        Assert.Equal(1, bus.QueryCount);

        // 事件携带版本 5，与缓存版本 7 不一致 → 视为过期，重新拉取。
        var second = await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: 5, CancellationToken.None);

        Assert.Equal([42, 43], second);
        Assert.Equal(2, bus.QueryCount);
    }

    [Fact]
    public async Task GetOrResolveAsync_QueryFailure_ThrowsInvalidOperationException()
    {
        var bus = new StubMessageBus(version: 7, members: [42])
        {
            FailQueries = true
        };

        var cache = new ConversationAudienceCache(bus);

        // fail-closed：拉取失败向上抛异常，由调用方决定 NAK 重投，绝不投递错误受众。
        async Task Act() => await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: 7, CancellationToken.None);
        await Assert.ThrowsAsync<InvalidOperationException>(Act);
    }

    [Fact]
    public async Task GetOrResolveAsync_TtlExpiry_Refetches()
    {
        var bus = new StubMessageBus(version: 7, members: [42, 43]);

        // 用可推进的 TimeProvider 模拟 TTL 过期。
        var clock = new ManualTimeProvider();
        var cache = new ConversationAudienceCache(
            bus,
            timeProvider: clock,
            ttl: TimeSpan.FromMinutes(5));

        var first = await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: 7, CancellationToken.None);
        Assert.Equal(1, bus.QueryCount);

        // 时钟前进 6 分钟，超过 TTL → 重新拉取。
        clock.Advance(TimeSpan.FromMinutes(6));
        var second = await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: 7, CancellationToken.None);

        Assert.Equal([42, 43], second);
        Assert.Equal(2, bus.QueryCount);
    }

    [Fact]
    public async Task GetOrResolveAsync_NonexistentConversation_ReturnsEmptyMembers()
    {
        var bus = new StubMessageBus(version: 0, members: []);

        var cache = new ConversationAudienceCache(bus);

        var members = await cache.GetOrResolveAsync(ConversationId, expectedAudienceVersion: null, CancellationToken.None);

        Assert.Empty(members);
    }

    /// <summary>可推进的 TimeProvider，用于 TTL 过期测试。</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        private long _ticks;

        public ManualTimeProvider() => _ticks = DateTimeOffset.UtcNow.UtcTicks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => _ticks;

        public void Advance(TimeSpan delta) => _ticks += delta.Ticks;
    }

    /// <summary>
    /// 可配置的 IRealtimeMessageBus 桩：仅 MutateGroupConversationAsync 返回可控受众结果，
    /// 其余方法返回默认空值。记录 QueryCount 以验证拉取次数。
    /// </summary>
    private sealed class StubMessageBus(
        long version,
        IReadOnlyList<long> members) : IRealtimeMessageBus
    {
        public bool FailQueries { get; init; }

        public int QueryCount { get; private set; }

        public Task<GroupConversationResult> MutateGroupConversationAsync(
            GroupConversationCommand command,
            CancellationToken ct = default)
        {
            QueryCount++;
            if (FailQueries)
                return Task.FromResult(GroupConversationResult.Failed(command.RequestId, "server_busy", "busy"));

            return Task.FromResult(
                GroupConversationResult.SuccessAudience(
                    command.RequestId,
                    command.ConversationId!,
                    version,
                    members));
        }

        public Task<GroupConversationResult> QueryReadReceiptsAsync(
            GroupConversationCommand command, CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(command.RequestId, "x", "x"));

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

        public Task<AttachmentFinalizeResult> FinalizeAttachmentUploadAsync(AttachmentFinalizeCommand command, CancellationToken ct = default) =>
            Task.FromResult(AttachmentFinalizeResult.Failed(command.RequestId, "x", "x"));

        public Task<AttachmentDownloadAuthorizeResult> AuthorizeAttachmentDownloadAsync(AttachmentDownloadAuthorizeCommand command, CancellationToken ct = default) =>
            Task.FromResult(AttachmentDownloadAuthorizeResult.Failed(command.RequestId, "x", "x"));

        public Task<RelationshipCommandResult> MutateRelationshipAsync(RelationshipCommand command, CancellationToken ct = default) =>
            Task.FromResult(RelationshipCommandResult.Failed(command.RequestId, "x", "x"));

        public Task<RelationshipListResult> QueryRelationshipListAsync(RelationshipListQuery query, CancellationToken ct = default) =>
            Task.FromResult(RelationshipListResult.Failed(query.RequestId, "x", "x"));

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

        public Task<CallProcessResult> SendCallCommandAsync(
            CallCommand command, CancellationToken ct = default) =>
            Task.FromResult(CallProcessResult.Failed(CallErrorCode.StateStoreUnavailable, "unavailable"));

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
            Task.FromResult(TimeSpan.FromMilliseconds(1));
    }
}