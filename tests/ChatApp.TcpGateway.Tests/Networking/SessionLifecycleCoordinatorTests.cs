using System.Diagnostics.Metrics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Push;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
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
    public async Task Authentication_MaintainsRoutingLease_WhenEphemeralPresenceIsDisabled()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var presence = new RecordingGlobalPresenceStore();
        var options = new TcpGatewayOptions
        {
            EnableResume = false,
            EnableEphemeralPresenceAndTyping = false,
            ReplaceSameDeviceSession = false,
            IdleTimeout = TimeSpan.FromSeconds(90)
        };
        var coordinator = CreateCoordinator(
            metrics,
            bus,
            new FakeDeviceSessionLeaseStore(),
            new FakeResumeTokenStore(),
            options: options,
            globalPresence: presence);
        await using var session = CreateSession(metrics);

        var result = await coordinator.OnAuthenticatedAsync(
            session,
            new RealtimeAuthenticationResult
            {
                Succeeded = true,
                UserId = 12001,
                SessionId = "routing-session",
                DeviceId = "routing-device",
                DeviceIdHash = 0xD1
            },
            ct);

        Assert.True(result.Success);
        Assert.Equal([(12001L, "test-instance")], presence.OnlineCalls);
        Assert.Empty(bus.PublishedPresenceEvents);

        await coordinator.OnDisconnectedAsync(session, ct);

        Assert.Equal([(12001L, "test-instance")], presence.OfflineCalls);
        Assert.Empty(bus.PublishedPresenceEvents);
    }

    [Fact]
    public async Task TryResumeAsync_BroadcastsSessionRevoked_WhenLeaseTakeoverFindsPreviousSession()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            // TakeOver 发现旧 SessionId（跨 Gateway），应触发 SessionRevoked 广播。
            // P0-7：TakeOverResult.Success 携带旧 SessionId 和旧 ConnectionLeaseId。
            OnTakeOver = _ => ValueTask.FromResult(
                TakeOverResult.Success("old-remote-session", "old-remote-lease"))
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
        Assert.True(result!.Success);
        Assert.Equal("new-token", result.Result!.ResumeToken);
        Assert.Equal(1001, result.Result.UserId);
        Assert.Equal("resuming-session", result.Result.SessionId);

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
        Assert.NotNull(result);
        Assert.False(result!.Success);
        Assert.Equal(ResumeFailureKind.InvalidToken, result.FailureKind);
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
        Assert.NotNull(result);
        Assert.False(result!.Success);
        Assert.Equal(ResumeFailureKind.DependencyUnavailable, result.FailureKind);
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

        Assert.NotNull(result);
        Assert.False(result!.Success);
        Assert.Equal(ResumeFailureKind.InvalidToken, result.FailureKind);
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

        Assert.NotNull(result);
        Assert.False(result!.Success);
        Assert.Equal(ResumeFailureKind.DependencyUnavailable, result.FailureKind);
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

        Assert.NotNull(result);
        Assert.False(result!.Success);
        Assert.Equal(ResumeFailureKind.DependencyUnavailable, result.FailureKind);
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
            OnTakeOver = _ => ValueTask.FromResult(TakeOverResult.NoPreviousLease()) // 无旧 Session
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
        Assert.True(result!.Success);
        Assert.Equal("fresh-token", result.Result!.ResumeToken);
        Assert.Equal(2002, result.Result.UserId);
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
            OnTakeOver = _ => ValueTask.FromResult(TakeOverResult.NoPreviousLease())
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

    [Fact]
    public async Task TryResumeAsync_FailsWithTakeOverUnavailable_WhenTakeOverThrows()
    {
        // P0-A: TakeOverAsync 依赖不可用时必须 fail-closed（拒绝恢复 + 关闭连接），
        // 而非旧行为的 fail-open（吞异常继续恢复，旧 Transport 不被吊销）。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnGetCurrentSessionId = _ => ValueTask.FromResult<string?>(null), // 租约查询通过
            OnTakeOver = _ => ValueTask.FromResult(
                TakeOverResult.Unavailable(new InvalidOperationException("simulated takeover failure")))
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
        Assert.NotNull(result);
        Assert.False(result!.Success);
        Assert.Equal(ResumeFailureKind.DependencyUnavailable, result.FailureKind);
        // 不应广播任何事件（未恢复成功）
        Assert.Empty(bus.PublishedEvents);
    }

    [Fact]
    public async Task TryResumeAsync_RecordsTakeOverUnavailableMetric_WhenTakeOverThrows()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        using var failedListener = new SingleCounterListener("gateway.resume.failed");
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnGetCurrentSessionId = _ => ValueTask.FromResult<string?>(null),
            OnTakeOver = _ => ValueTask.FromResult(
                TakeOverResult.Unavailable(new InvalidOperationException("simulated takeover failure")))
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
            "gateway.resume.failed was not incremented");
    }

    [Fact]
    public async Task TryResumeAsync_FailOpen_ProceedsWhenLeaseQueryThrows()
    {
        // P1-C：ResumeRedisFailMode=FailOpen 时，代次校验依赖不可用应跳过校验继续恢复，
        // 而非默认的 FailClosed（拒绝恢复）。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            // GetCurrentSessionId 抛异常模拟 Redis 故障
            OnGetCurrentSessionId = _ => throw new RedisException("simulated redis failure"),
            // TakeOver 成功（无旧租约）
            OnTakeOver = _ => ValueTask.FromResult(TakeOverResult.NoPreviousLease())
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
                }),
            OnIssue = _ => Task.FromResult("new-token")
        };
        var options = new TcpGatewayOptions
        {
            EnableResume = true,
            ResumeTokenTtl = TimeSpan.FromSeconds(30),
            EnableEphemeralPresenceAndTyping = false,
            ReplaceSameDeviceSession = false,
            IdleTimeout = TimeSpan.FromSeconds(90),
            ResumeRedisFailMode = RedisFailMode.FailOpen
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore, options: options);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("valid-token", session, ct);

        // FailOpen：恢复成功，跳过代次校验
        Assert.NotNull(result);
        Assert.True(result!.Success);
        Assert.NotNull(result.Result);
        Assert.Equal("new-token", result.Result!.ResumeToken);
    }

    [Fact]
    public async Task TryResumeAsync_FailClosed_RejectsWhenLeaseQueryThrows()
    {
        // P1-C：ResumeRedisFailMode=FailClosed（默认）时，代次校验依赖不可用应拒绝恢复。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnGetCurrentSessionId = _ => throw new RedisException("simulated redis failure")
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
        // 默认 ResumeRedisFailMode=FailClosed，无需显式设置
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("valid-token", session, ct);

        // FailClosed：拒绝恢复
        Assert.NotNull(result);
        Assert.False(result!.Success);
        Assert.Equal(ResumeFailureKind.DependencyUnavailable, result.FailureKind);
        Assert.Equal(ResumeFailureReason.LeaseQueryFailed, result.FailureReason);
    }

    [Fact]
    public async Task OnAuthenticatedAsync_FailClosed_RejectsAuthWhenTakeOverUnavailable()
    {
        // P1-C：AuthRedisFailMode=FailClosed 时，TakeOver 依赖不可用应拒绝认证、回滚本地状态。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnTakeOver = _ => ValueTask.FromResult(
                TakeOverResult.Unavailable(new InvalidOperationException("simulated takeover failure")))
        };
        var tokenStore = new FakeResumeTokenStore();
        var options = new TcpGatewayOptions
        {
            EnableResume = true,
            ResumeTokenTtl = TimeSpan.FromSeconds(30),
            EnableEphemeralPresenceAndTyping = false,
            ReplaceSameDeviceSession = true, // 启用 TakeOver 路径
            IdleTimeout = TimeSpan.FromSeconds(90),
            AuthRedisFailMode = RedisFailMode.FailClosed
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore, options: options);
        await using var session = CreateSession(metrics);

        var authResult = new RealtimeAuthenticationResult
        {
            Succeeded = true,
            UserId = 2001,
            SessionId = "auth-session",
            DeviceIdHash = 0xBB,
            DeviceId = "dev-2"
        };

        var result = await coordinator.OnAuthenticatedAsync(session, authResult, ct);

        // FailClosed：认证被拒绝
        Assert.False(result.Success);
        Assert.Equal(AuthFailureKind.DependencyUnavailable, result.FailureKind);
        Assert.NotNull(result.RetryAfterMs);
        // 未颁发 ResumeToken
        Assert.Null(result.ResumeToken);
    }

    [Fact]
    public async Task OnAuthenticatedAsync_FailOpen_ProceedsWhenTakeOverUnavailable()
    {
        // P1-C：AuthRedisFailMode=FailOpen 时，TakeOver 依赖不可用应继续完成认证（旧行为）。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnTakeOver = _ => ValueTask.FromResult(
                TakeOverResult.Unavailable(new InvalidOperationException("simulated takeover failure")))
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnIssue = _ => Task.FromResult("auth-issued-token")
        };
        var options = new TcpGatewayOptions
        {
            EnableResume = true,
            ResumeTokenTtl = TimeSpan.FromSeconds(30),
            EnableEphemeralPresenceAndTyping = false,
            ReplaceSameDeviceSession = true,
            IdleTimeout = TimeSpan.FromSeconds(90),
            AuthRedisFailMode = RedisFailMode.FailOpen
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore, options: options);
        await using var session = CreateSession(metrics);

        var authResult = new RealtimeAuthenticationResult
        {
            Succeeded = true,
            UserId = 2002,
            SessionId = "auth-session-2",
            DeviceIdHash = 0xCC,
            DeviceId = "dev-3"
        };

        var result = await coordinator.OnAuthenticatedAsync(session, authResult, ct);

        // FailOpen：认证成功，TakeOver 失败被吞掉
        Assert.True(result.Success);
        Assert.Equal("auth-issued-token", result.ResumeToken);
    }

    [Fact]
    public async Task TryResumeAsync_AbortRollsBackLocalState_WhenTakeOverFails()
    {
        // P1-D：Commit 阶段 TakeOver 失败时，Abort 必须回滚已完成的本地状态变更
        // （UserSessionRegistry.Remove + Presence 下线）。
        // 验证：TakeOver 失败后，OnDisconnectedAsync 不应再次移除/广播（已 Abort）。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnGetCurrentSessionId = _ => ValueTask.FromResult<string?>(null),
            OnTakeOver = _ => ValueTask.FromResult(
                TakeOverResult.Unavailable(new InvalidOperationException("simulated takeover failure")))
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
        Assert.NotNull(result);
        Assert.False(result!.Success);
        // P1-D：Abort 应已回滚本地状态——session 不应在注册表中残留。
        // 验证：OnDisconnectedAsync 中 Remove 返回 false（已 Abort 移除），不再广播 Presence。
        var eventsBefore = bus.PublishedEvents.Count;
        await coordinator.OnDisconnectedAsync(session, ct);
        Assert.Equal(eventsBefore, bus.PublishedEvents.Count);
    }

    [Fact]
    public async Task TryResumeAsync_PrepareFailsOnInvalidToken_DoesNotCallCommit()
    {
        // P1-D：Prepare 阶段失败（InvalidToken）时不应进入 Commit，无副作用。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var takeOverCalled = false;
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnTakeOver = _ =>
            {
                takeOverCalled = true;
                return ValueTask.FromResult(TakeOverResult.NoPreviousLease());
            }
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(null) // Token 无效
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("invalid-token", session, ct);

        // Prepare 失败：InvalidToken
        Assert.NotNull(result);
        Assert.False(result!.Success);
        Assert.Equal(ResumeFailureKind.InvalidToken, result.FailureKind);
        // Commit 未执行：TakeOver 未被调用
        Assert.False(takeOverCalled);
        Assert.Empty(bus.PublishedEvents);
    }

    [Fact]
    public async Task TryResumeAsync_PrepareSucceeds_CommitExecutesAndSucceeds()
    {
        // P1-D：Prepare 成功 → Commit 执行 → 恢复成功（两阶段提交正常路径）。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus();
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnGetCurrentSessionId = _ => ValueTask.FromResult<string?>(null),
            OnTakeOver = _ => ValueTask.FromResult(TakeOverResult.NoPreviousLease())
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(
                new ResumeContext
                {
                    UserId = 3001,
                    SessionId = "prepare-commit-session",
                    ConnectionLeaseId = "old-lease-3001",
                    DeviceIdHash = 0xDD
                }),
            OnIssue = _ => Task.FromResult("new-token-3001")
        };
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore);
        await using var session = CreateSession(metrics);

        var result = await coordinator.TryResumeAsync("valid-token", session, ct);

        // Prepare + Commit 成功
        Assert.NotNull(result);
        Assert.True(result!.Success);
        Assert.NotNull(result.Result);
        Assert.Equal("new-token-3001", result.Result!.ResumeToken);
        Assert.Equal(3001, result.Result.UserId);
        Assert.Equal("prepare-commit-session", result.Result.SessionId);
    }

    [Fact]
    public async Task TryResumeAsync_ClosesLocalVictimEvenWhenNatsPublishFails()
    {
        // P1-H：NATS 吊销事件发布失败时，本机旧连接仍被立即关闭。
        // RevokeSessionAsync 顺序：RevokeResumeTokenSafeAsync → PublishSessionRevokedEventAsync（best-effort）
        // → victim.Close。NATS 发布是 try/catch 仅日志，不阻断后续 victim.Close。
        // 这保证 NATS 故障时本机 TakeOver 仍形成完整闭环——旧 Transport 无法继续通信。
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        var bus = new CapturingMessageBus { ThrowOnPublish = true };
        var leaseStore = new FakeDeviceSessionLeaseStore
        {
            OnGetCurrentSessionId = _ => ValueTask.FromResult<string?>(null),
            // 本机 victim 路径：TakeOver 无跨 Gateway 旧租约（victim 在本机注册表）。
            OnTakeOver = _ => ValueTask.FromResult(TakeOverResult.NoPreviousLease())
        };
        var tokenStore = new FakeResumeTokenStore
        {
            OnTryValidate = _ => Task.FromResult<ResumeContext?>(
                new ResumeContext
                {
                    UserId = 1001,
                    SessionId = "resuming-session",
                    ConnectionLeaseId = "old-lease",
                    DeviceIdHash = 0xAA,
                    DeviceId = "dev-1"
                }),
            OnIssue = _ => Task.FromResult("new-token")
        };

        // 共享注册表，预置本机 victim 会话（同 UserId + 同 DeviceIdHash）。
        var registry = new UserSessionRegistry();
        var coordinator = CreateCoordinator(metrics, bus, leaseStore, tokenStore, registry: registry);

        await using var victim = CreateSession(metrics, connectionId: 1);
        victim.Authenticate(userId: 1001, sessionId: "victim-session", deviceIdHash: 0xAA, deviceId: "dev-1");
        victim.CurrentResumeToken = "victim-old-token";
        Assert.True(registry.Add(victim));
        Assert.True(victim.IsConnected); // 前置条件：victim 未关闭

        // incoming：新的 Resume 请求（不同 ConnectionId，故不同 ConnectionLeaseId）。
        await using var incoming = CreateSession(metrics, connectionId: 2);

        var result = await coordinator.TryResumeAsync("valid-token", incoming, ct);

        // Resume 成功：NATS 故障不阻断恢复。
        Assert.NotNull(result);
        Assert.True(result!.Success);
        Assert.Equal("new-token", result.Result!.ResumeToken);

        // P1-H 核心：本机 victim 已被立即关闭，即使 NATS SessionRevoked 发布失败。
        Assert.False(victim.IsConnected, "victim must be closed even when NATS publish fails");
        Assert.Equal(SessionCloseReason.SessionRevoked, victim.CloseReason);
    }

    private static SessionLifecycleCoordinator CreateCoordinator(
        GatewayMetrics metrics,
        IRealtimeMessageBus bus,
        IDeviceSessionLeaseStore leaseStore,
        IResumeTokenStore tokenStore,
        IRedisCircuitBreaker? circuitBreaker = null,
        TcpGatewayOptions? options = null,
        UserSessionRegistry? registry = null,
        IGlobalPresenceStore? globalPresence = null)
    {
        options ??= new TcpGatewayOptions
        {
            EnableResume = true,
            ResumeTokenTtl = TimeSpan.FromSeconds(30),
            EnableEphemeralPresenceAndTyping = false,
            ReplaceSameDeviceSession = false,
            IdleTimeout = TimeSpan.FromSeconds(90)
        };

        return new SessionLifecycleCoordinator(
            leaseStore,
            globalPresence ?? new NoopGlobalPresenceStore(),
            tokenStore,
            registry ?? new UserSessionRegistry(),
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

    private static TcpClientSession CreateSession(GatewayMetrics metrics, uint connectionId = 1) =>
        new(
            new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp),
            connectionId,
            outboundQueueCapacity: 8,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            metrics,
            NullLogger<TcpClientSession>.Instance);

    /// <summary>
    /// 捕获 PublishEventAsync 与 QuerySyncBootstrapAsync 调用，验证 Resume 路径事件广播。
    /// <para>
    /// <see cref="ThrowOnPublish"/> = true 时 <see cref="PublishEventAsync"/> 抛异常，
    /// 用于验证 NATS/Realtime bus 故障时本机旧连接仍被立即关闭（best-effort 发布不阻断关闭）。
    /// </para>
    /// </summary>
    private sealed class CapturingMessageBus : IRealtimeMessageBus
    {
        public List<RealtimeEvent> PublishedEvents { get; } = [];
        public List<EphemeralPresenceEvent> PublishedPresenceEvents { get; } = [];

        /// <summary>
        /// 为 true 时 <see cref="PublishEventAsync"/> 抛 <see cref="InvalidOperationException"/>，
        /// 模拟 NATS/Realtime bus 发布失败。
        /// </summary>
        public bool ThrowOnPublish { get; set; }

        public Task PublishEventAsync(RealtimeEvent evt, CancellationToken ct = default)
        {
            if (ThrowOnPublish)
                throw new InvalidOperationException("simulated NATS publish failure");
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

        public Task<GroupConversationResult> QueryReadReceiptsAsync(
            GroupConversationCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(GroupConversationResult.Failed(
                command.RequestId, "not_used", "not used"));

        public Task<AttachmentFinalizeResult> FinalizeAttachmentUploadAsync(
            AttachmentFinalizeCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(AttachmentFinalizeResult.Failed(command.RequestId, "not_used", "not used"));

        public Task<AttachmentDownloadAuthorizeResult> AuthorizeAttachmentDownloadAsync(
            AttachmentDownloadAuthorizeCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(AttachmentDownloadAuthorizeResult.Failed(command.RequestId, "not_used", "not used"));

        public Task<RelationshipCommandResult> MutateRelationshipAsync(
            RelationshipCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(RelationshipCommandResult.Failed(command.RequestId, "x", "x"));

        public Task<RelationshipListResult> QueryRelationshipListAsync(
            RelationshipListQuery query,
            CancellationToken ct = default) =>
            Task.FromResult(RelationshipListResult.Failed(query.RequestId, "x", "x"));

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

        public Task<CallProcessResult> SendCallCommandAsync(
            CallCommand command,
            CancellationToken ct = default) =>
            Task.FromResult(CallProcessResult.Failed(CallErrorCode.StateStoreUnavailable, "unavailable"));

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
            CancellationToken ct = default)
        {
            PublishedPresenceEvents.Add(evt);
            return Task.CompletedTask;
        }

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

    private sealed class RecordingGlobalPresenceStore : IGlobalPresenceStore
    {
        public List<(long UserId, string InstanceId)> OnlineCalls { get; } = [];
        public List<(long UserId, string InstanceId)> OfflineCalls { get; } = [];

        public Task<PresenceTransition> SetOnlineAsync(
            long userId,
            string instanceId,
            CancellationToken ct = default)
        {
            OnlineCalls.Add((userId, instanceId));
            return Task.FromResult(PresenceTransition.WentOnline);
        }

        public Task<PresenceTransition> SetOfflineAsync(
            long userId,
            string instanceId,
            CancellationToken ct = default)
        {
            OfflineCalls.Add((userId, instanceId));
            return Task.FromResult(PresenceTransition.WentOffline);
        }

        public Task RefreshOnlineAsync(
            long userId,
            string instanceId,
            CancellationToken ct = default) => Task.CompletedTask;

        public Task<bool> IsOnlineAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(OnlineCalls.Any(call => call.UserId == userId));

        public Task<IReadOnlyDictionary<long, bool>> GetOnlineManyAsync(
            IReadOnlyList<long> userIds,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyDictionary<long, bool>>(
                userIds.ToDictionary(
                    static userId => userId,
                    userId => OnlineCalls.Any(call => call.UserId == userId)));

        public Task RunMaintenanceAsync(CancellationToken ct = default) => Task.CompletedTask;
    }

    private sealed class FakeDeviceSessionLeaseStore : IDeviceSessionLeaseStore
    {
        public Func<long, ValueTask<TakeOverResult>>? OnTakeOver { get; set; }
        public Func<long, ValueTask<string?>>? OnGetCurrentSessionId { get; set; }

        public ValueTask<TakeOverResult> TakeOverAsync(
            long userId,
            ulong deviceIdHash,
            string sessionId,
            string transportId,
            string leaseOwnerToken,
            TimeSpan ttl,
            CancellationToken cancellationToken) =>
            OnTakeOver?.Invoke(userId) ?? ValueTask.FromResult(TakeOverResult.NoPreviousLease());

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
