using System.Diagnostics.Metrics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Logging.Abstractions;
using RealtimeHistory = ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// Resume 可靠性主线核心测试：验证 TryResumeAsync 在各种场景下的行为——
/// 跨 Gateway 设备租约接管时广播 SessionRevoked、代次校验拒绝旧会话、
/// Redis 熔断器开路快速失败、Token 无效/Redis 故障归因。
/// </summary>
/// <remarks>
/// 使用 MeterListener 串行集合避免并行测试污染指标捕获。
/// </remarks>
[Collection("MeterListenerSerial")]
public sealed class SessionLifecycleCoordinatorTests
{
    private static readonly RealtimeIntegrationOptions IntegrationOptions = new()
    {
        InstanceId = "test-instance"
    };

    [Fact]
    public async Task TryResumeAsync_BroadcastsSessionRevoked_WhenLeaseTakeoverFindsPreviousSession()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            // TakeOver 发现旧 SessionId（跨 Gateway），应触发 SessionRevoked 广播。
            OnTakeOver = _ => ValueTask.FromResult<string?>("old-remote-session")
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(
                new ResumeContext
                {
                    UserId = 1001,
                    SessionId = "resuming-session",
                    ConnectionLeaseId = "old-lease",
                    DeviceId = "dev-1",
                    DeviceIdHash = 0xAA
                }),
            OnIssue = _ => Task.FromResult("new-token")
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("valid-token", session, ct);

        // Resume 成功
        Assert.NotNull(result);
        Assert.Equal("new-token", result!.ResumeToken);
        Assert.Equal(1001, result.UserId);
        Assert.Equal("resuming-session", result.SessionId);

        // 应广播 SessionRevoked 关闭跨 Gateway 旧连接
        var revoked = Assert.Single(bus.PublishedEvents,
            e => e.Type == RealtimeEventType.SessionRevoked);
        Assert.Equal(1001, revoked.TargetUserId);
        Assert.Equal("old-remote-session", revoked.SessionId);
        Assert.True(revoked.OccurredAtMs > 0);
    }

    [Fact]
    public async Task TryResumeAsync_FailsWithLeaseMismatch_WhenDeviceLeaseOwnedByNewerSession()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            // 当前租约已归属另一个更新的 SessionId
            OnGetCurrentSessionId = _ => ValueTask.FromResult<string?>("newer-session")
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(
                new ResumeContext
                {
                    UserId = 1001,
                    SessionId = "old-session",
                    ConnectionLeaseId = "old-lease",
                    DeviceIdHash = 0xAA
                })
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("valid-token", session, ct);

        // 拒绝恢复
        Assert.Null(result);
        // 不应广播 SessionRevoked（拒绝恢复 ≠ 关闭旧连接）
        Assert.DoesNotContain(bus.PublishedEvents,
            e => e.Type == RealtimeEventType.SessionRevoked);
    }

    [Fact]
    public async Task TryResumeAsync_FailsWithLeaseQueryFailed_WhenLeaseQueryThrows()
    {
        // R-3: 设备租约查询失败时必须 fail-closed（拒绝恢复），而非旧行为的 fail-open（继续恢复）。
        // Same-device fencing 属于安全不变量，依赖不可用时要求完整认证。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnGetCurrentSessionId = _ => throw new InvalidOperationException("simulated lease query failure")
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(
                new ResumeContext
                {
                    UserId = 1001,
                    SessionId = "resuming-session",
                    ConnectionLeaseId = "old-lease",
                    DeviceIdHash = 0xAA
                })
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("valid-token", session, ct);

        // 拒绝恢复（fail-closed）
        Assert.Null(result);
        // 不应广播任何事件（未恢复成功）
        Assert.Empty(bus.PublishedEvents);
    }

    [Fact]
    public async Task TryResumeAsync_RecordsLeaseQueryFailedMetric_WhenLeaseQueryThrows()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        using var failedListener = new SingleCounterListener("gateway.resume.failed");
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnGetCurrentSessionId = _ => throw new InvalidOperationException("simulated lease query failure")
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(
                new ResumeContext
                {
                    UserId = 1001,
                    SessionId = "resuming-session",
                    ConnectionLeaseId = "old-lease",
                    DeviceIdHash = 0xAA
                })
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        await coordinator.TryResumeAsync("valid-token", session, ct);

        Assert.True(failedListener.WaitForIncrement(TimeSpan.FromSeconds(2)),
            "gateway.resume.failed was not incremented for lease query failure");
    }

    [Fact]
    public async Task TryResumeAsync_FailsWithInvalidToken_WhenTokenStoreReturnsNullContext()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore();
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(null)
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("invalid-token", session, ct);

        Assert.Null(result);
        // 不应触发任何事件
        Assert.Empty(bus.PublishedEvents);
    }

    [Fact]
    public async Task TryResumeAsync_FailsWithRedisFailure_WhenTokenStoreThrows()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore();
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => throw new InvalidOperationException("simulated redis failure")
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("any-token", session, ct);

        Assert.Null(result);
        Assert.Empty(bus.PublishedEvents);
    }

    [Fact]
    public async Task TryResumeAsync_FailsWithCircuitOpen_WhenBreakerIsOpen_AndSkipsRedisCalls()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore();
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => throw new InvalidOperationException("must not be called")
        };
        // 预先开路的熔断器
        var breaker = new RedisCircuitBreaker(failureThreshold: 1);
        breaker.RecordFailure();
        Assert.False(breaker.IsAvailable);

        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore, breaker);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("any-token", session, ct);

        Assert.Null(result);
        // 熔断器开路时不应调用 Redis 存储
        Assert.False(tokenStore.TryValidateCalled);
        Assert.Empty(bus.PublishedEvents);
    }

    [Fact]
    public async Task TryResumeAsync_Succeeds_AndIssuesNewToken_WhenNoLeaseConflict()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            // 当前租约归属待恢复 SessionId（无冲突）
            OnGetCurrentSessionId = _ => ValueTask.FromResult<string?>("resuming-session"),
            OnTakeOver = _ => ValueTask.FromResult<string?>(null) // 无旧 Session
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(
                new ResumeContext
                {
                    UserId = 2002,
                    SessionId = "resuming-session",
                    ConnectionLeaseId = "old-lease",
                    DeviceId = "dev-2",
                    DeviceIdHash = 0xBB
                }),
            OnIssue = _ => Task.FromResult("fresh-token")
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("valid-token", session, ct);

        Assert.NotNull(result);
        Assert.Equal("fresh-token", result!.ResumeToken);
        Assert.Equal(2002, result.UserId);
        // 无旧 Session，不应广播 SessionRevoked
        Assert.DoesNotContain(bus.PublishedEvents,
            e => e.Type == RealtimeEventType.SessionRevoked);
    }

    [Fact]
    public async Task TryResumeAsync_RecordsResumeSucceededMetric_OnHappyPath()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        using var succeededListener = new SingleCounterListener("gateway.resume.succeeded");
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnTakeOver = _ => ValueTask.FromResult<string?>(null)
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(
                new ResumeContext
                {
                    UserId = 3003,
                    SessionId = "session-x",
                    ConnectionLeaseId = "lease-x",
                    DeviceIdHash = 0xCC
                }),
            OnIssue = _ => Task.FromResult("token-x")
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        await coordinator.TryResumeAsync("valid-token", session, ct);

        Assert.True(succeededListener.WaitForIncrement(TimeSpan.FromSeconds(2)),
            "gateway.resume.succeeded was not incremented");
    }

    [Fact]
    public async Task TryResumeAsync_RecordsResumeFailedMetric_OnInvalidToken()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        using var failedListener = new SingleCounterListener("gateway.resume.failed");
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore();
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(null)
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        await coordinator.TryResumeAsync("invalid", session, ct);

        Assert.True(failedListener.WaitForIncrement(TimeSpan.FromSeconds(2)),
            "gateway.resume.failed was not incremented");
    }

    [Fact]
    public async Task TryResumeAsync_RecordsCircuitOpenMetric_WhenBreakerIsOpen()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        using var cbListener = new SingleCounterListener("gateway.redis.circuit_breaker.open");
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore();
        var tokenStore = new FakeResumeTokenStore();
        var breaker = new RedisCircuitBreaker(failureThreshold: 1);
        breaker.RecordFailure();

        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore, breaker);
        await using var session = CreateSession(metrics);

        await coordinator.TryResumeAsync("any", session, ct);

        Assert.True(cbListener.WaitForIncrement(TimeSpan.FromSeconds(2)),
            "gateway.redis.circuit_breaker.open was not incremented");
    }

    private static SessionLifecycleCoordinator CreateCoordinator(
        GatewayMetrics metrics,
        IRealtimeMessageBus bus,
        IDeviceSessionLeaseStore leaseStore,
        IResumeTokenStore tokenStore,
        IRedisCircuitBreaker? circuitBreaker = null)
    {
        var options = new TcpGatewayOptions
        {
            EnableResume = true,
            ResumeTokenTtl = TimeSpan.FromSeconds(30),
            EnableEphemeralPresenceAndTyping = false,
            ReplaceSameDeviceSession = false,
            IdleTimeout = TimeSpan.FromSeconds(90)
        };

        return new SessionLifecycleCoordinator(
            leaseStore,
            new NoopGlobalPresenceStore(),
            tokenStore,
            new UserSessionRegistry(),
            new PresenceWatcherRegistry(),
            bus,
            IntegrationOptions,
            options,
            metrics,
            TimeProvider.System,
            NullLogger.Instance,
            new JsonPayloadCodec<PresenceChanged>(
                GatewayJsonSerializerContext.Default.PresenceChanged),
            circuitBreaker);
    }

    private static TcpClientSession CreateSession(GatewayMetrics metrics) =>
        new(
            new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp),
            connectionId: 1,
            outboundQueueCapacity: 8,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);

    /// <summary>
    /// 捕获 PublishEventAsync 与 QuerySyncBootstrapAsync 调用，验证 Resume 路径事件广播。
    /// </summary>
    private sealed class CapturingMessageBus : IRealtimeMessageBus
    {
        public List<RealtimeEvent> PublishedEvents { get; } = [];

        public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default)
        {
            PublishedEvents.Add(evt);
            return Task.CompletedTask;
        }

        public Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
            SyncBootstrapQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(SyncBootstrapPage.Success(
                query.RequestId,
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                [],
                conversationsNextCursor: null,
                conversationsHasMore: false,
                catchUps: []));

        // 以下方法 Resume 路径不使用，提供空实现。
        public Task PublishIncomingMessageAsync(
            IncomingMessageCommand command,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishMessageReceiptAsync(
            MessageReceiptCommand command,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<RealtimeHistory.MessageHistoryPage> QueryMessageHistoryAsync(
            RealtimeHistory.MessageHistoryQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(RealtimeHistory.MessageHistoryPage.Success(
                query.RequestId, [], nextCursor: null, hasMore: false));

        public Task<ConversationListPage> QueryConversationListAsync(
            ConversationListQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(ConversationListPage.Success(
                query.RequestId, [], nextCursor: null, hasMore: false));

        public Task<ConversationMarkReadResult> MarkConversationReadAsync(
            ConversationMarkReadCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(ConversationMarkReadResult.Success(
                command.RequestId,
                command.ConversationId,
                unreadCount: 0,
                lastReadMessageId: command.ReadMessageId,
                lastReadAtMs: command.ReadAtMs ?? 0,
                changed: true));

        public Task<ConversationSetPrefsResult> SetConversationPrefsAsync(
            ConversationSetPrefsCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(ConversationSetPrefsResult.Success(
                command.RequestId, command.ConversationId,
                isPinned: false, isMuted: false, mutedUntilMs: null, changed: false));

        public Task<GroupConversationResult> MutateGroupConversationAsync(
            GroupConversationCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(
                command.RequestId, "not_used", "not used"));

        public Task<MessageRecallResult> RecallMessageAsync(
            MessageRecallCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(MessageRecallResult.Success(
                command.RequestId, command.MessageId,
                conversationId: null, recalledAtMs: command.OccurredAtMs));

        public Task<MessageEditResult> EditMessageAsync(
            MessageEditCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(MessageEditResult.Success(
                command.RequestId, command.MessageId,
                conversationId: null, content: command.Content,
                editVersion: 2, editedAtMs: command.OccurredAtMs));

        public Task<MessageReactionResult> ReactToMessageAsync(
            MessageReactionCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(MessageReactionResult.Failed(
                command.RequestId, "not_used", "not used"));

        public Task<RealtimeHistory.RealtimeHistoryMessage?> TryGetMessageByIdAsync(
            long userId,
            string messageId,
            CancellationToken ct = default) =>
            Task.FromResult<RealtimeHistory.RealtimeHistoryMessage?>(null);

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

        public Task PublishEphemeralTypingAsync(
            EphemeralTypingEvent evt,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task PublishEphemeralPresenceAsync(
            EphemeralPresenceEvent evt,
            CancellationToken ct = default) =>
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
            Task.FromResult(new PresenceAuthorizeResponse { AllowedUserIds = [] });

        public Task ServePresenceAuthorizeAsync(
            Func<PresenceAuthorizeQuery, CancellationToken, ValueTask<PresenceAuthorizeResponse>> handler,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
            Task.FromResult(TimeSpan.Zero);
    }

    private sealed class FakeDeviceSessionLeaseStore : IDeviceSessionLeaseStore
    {
        public Func<long, ValueTask<string?>>? OnTakeOver { get; set; }
        public Func<long, ValueTask<string?>>? OnGetCurrentSessionId { get; set; }

        public ValueTask<string?> TakeOverAsync(
            long userId,
            ulong deviceIdHash,
            string sessionId,
            string connectionLeaseId,
            TimeSpan ttl,
            CancellationToken cancellationToken) =>
            OnTakeOver?.Invoke(userId) ?? ValueTask.FromResult<string?>(null);

        public ValueTask ReleaseIfOwnerAsync(
            long userId,
            ulong deviceIdHash,
            string connectionLeaseId,
            CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<bool> RefreshIfOwnerAsync(
            long userId,
            ulong deviceIdHash,
            string connectionLeaseId,
            TimeSpan ttl,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);

        public ValueTask<string?> GetCurrentSessionIdAsync(
            long userId,
            ulong deviceIdHash,
            CancellationToken cancellationToken) =>
            OnGetCurrentSessionId?.Invoke(userId) ?? ValueTask.FromResult<string?>(null);
    }

    private sealed class FakeResumeTokenStore : IResumeTokenStore
    {
        public Func<string, Task<ResumeContext?>>? OnTryValidate { get; set; }
        public Func<ResumeContext, Task<string>>? OnIssue { get; set; }
        public bool TryValidateCalled { get; private set; }

        public Task<string> IssueAsync(
            ResumeContext context,
            TimeSpan ttl,
            CancellationToken ct = default) =>
            OnIssue?.Invoke(context) ?? Task.FromResult("issued-token");

        public Task<ResumeContext?> TryValidateAsync(
            string resumeToken,
            CancellationToken ct = default)
        {
            TryValidateCalled = true;
            return OnTryValidate?.Invoke(resumeToken) ?? Task.FromResult<ResumeContext?>(null);
        }

        public Task RevokeAsync(string resumeToken, CancellationToken ct = default) =>
            Task.CompletedTask;
    }

    /// <summary>
    /// 单一指标监听器：捕获指定 Counter 的累计增量。
    /// 必须串行化（MeterListener 全局监听器会捕获并行测试的测量）。
    /// </summary>
    private sealed class SingleCounterListener : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _count;
        private readonly string _instrumentName;

        public SingleCounterListener(string instrumentName)
        {
            _instrumentName = instrumentName;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == GatewayMetrics.MeterName
                    && instrument.Name == _instrumentName)
                {
                    listener.EnableMeasurementEvents(instrument, this);
                }
            };
            _listener.SetMeasurementEventCallback<long>(static (_, measurement, _, state) =>
            {
                if (state is SingleCounterListener l && measurement > 0)
                    Interlocked.Add(ref l._count, measurement);
            });
            _listener.Start();
        }

        public long Count => Volatile.Read(ref _count);

        public bool WaitForIncrement(TimeSpan timeout)
        {
            var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
            while (Environment.TickCount64 < deadline)
            {
                if (Volatile.Read(ref _count) > 0)
                    return true;
                Thread.Sleep(10);
            }
            return false;
        }

        public void Dispose() => _listener.Dispose();
    }
}
