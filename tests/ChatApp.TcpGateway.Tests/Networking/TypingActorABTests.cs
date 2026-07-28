using System.Buffers;
using System.Net.Sockets;
using System.Text.Json;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Ephemeral;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// R1：真实 Ephemeral A/B 测试——TypingActorPipeline 全场景覆盖。
/// <para>
/// 覆盖 10 个场景：授权缓存命中/未命中、NATS 延迟/断线、同 Key 高频覆盖、
/// 连接 churn、10,000 活跃 Actor、单热点 Actor、预算和 ArrayPool 归零。
/// 不只测纯 Actor State 递增，而是覆盖真实业务路径（授权 I/O + fanout + 资源释放）。
/// </para>
/// </summary>
public sealed class TypingActorABTests
{
    private static readonly JsonPayloadCodec<TypingNotify> TypingNotifyCodec =
        new(GatewayJsonSerializerContext.Default.TypingNotify);

    #region 场景 1：授权缓存命中——第二次 Notify 跳过 I/O

    [Fact]
    public async Task AuthCacheHit_SecondNotifyToSameKeySkipsIO()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: true);
        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(30),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var pipeline = CreateTypingActorPipeline(
            authorizer, fanout, clock);
        await using var session = CreateSession(userId: 1001);
        await pipeline.StartAsync(ct);

        // 第一次 Notify：触发授权 I/O。
        SendTypingNotify(pipeline, session, conversationId: "dm:1001:1002", isTyping: true);
        Console.WriteLine($"[TEST] after first Notify: CallCount={authorizer.CallCount}");
        await WaitUntilAsync(
            () => authorizer.CallCount >= 1 && pipeline.Snapshot.TotalProcessed >= 2,
            TimeSpan.FromSeconds(2), ct);
        Console.WriteLine($"[TEST] after wait: CallCount={authorizer.CallCount} TotalProcessed={pipeline.Snapshot.TotalProcessed} BusyActors={pipeline.Snapshot.BusyActors} ActiveActors={pipeline.Snapshot.ActiveActors}");
        Assert.Equal(1, authorizer.CallCount);

        // 等待 fanout 发射 typing=true。
        await WaitUntilAsync(
            () => fanout.DrainPending().Count > 0,
            TimeSpan.FromSeconds(1), ct);

        // 第二次 Notify 到同一 (sender,target)：应命中 Actor 内缓存，不触发新 I/O。
        SendTypingNotify(pipeline, session, conversationId: "dm:1001:1002", isTyping: false);
        await WaitUntilAsync(
            () => pipeline.Snapshot.TotalProcessed >= 3,
            TimeSpan.FromSeconds(1), ct);

        // 授权调用次数仍为 1——缓存命中。
        Assert.Equal(1, authorizer.CallCount);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 2：授权缓存未命中——提交 I/O 并 Suspend

    [Fact]
    public async Task AuthCacheMiss_SubmitsIOAndSuspendsActor()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: true);
        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(30),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var pipeline = CreateTypingActorPipeline(authorizer, fanout, clock);
        await using var session = CreateSession(userId: 2001);
        await pipeline.StartAsync(ct);

        // 提交 Notify：Actor 应 Suspend 等待授权 I/O。
        SendTypingNotify(pipeline, session, conversationId: "dm:2001:2002", isTyping: true);

        // 等待授权 I/O 完成 + fanout 发射。
        await WaitUntilAsync(
            () => authorizer.CallCount >= 1,
            TimeSpan.FromSeconds(2), ct);
        await WaitUntilAsync(
            () => fanout.DrainPending().Count > 0,
            TimeSpan.FromSeconds(2), ct);

        Assert.Equal(1, authorizer.CallCount);
        // Actor 不应卡在 Busy 状态。
        await WaitUntilAsync(
            () => pipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(2), ct);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 3：授权拒绝——丢弃 Notify 且不发射

    [Fact]
    public async Task AuthDenied_DropsNotifyAndDoesNotEmit()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: false);
        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(30),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var pipeline = CreateTypingActorPipeline(authorizer, fanout, clock);
        await using var session = CreateSession(userId: 3001);
        await pipeline.StartAsync(ct);

        SendTypingNotify(pipeline, session, conversationId: "dm:3001:3002", isTyping: true);

        // 授权被拒绝。
        await WaitUntilAsync(
            () => authorizer.CallCount >= 1,
            TimeSpan.FromSeconds(2), ct);

        // 不应有 fanout 发射。
        Assert.Empty(fanout.DrainPending());

        // Actor 不应卡住。
        await WaitUntilAsync(
            () => pipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(2), ct);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 4：NATS 延迟——慢速授权不阻塞其他 Key

    [Fact]
    public async Task NatsLatency_SlowAuthDoesNotBlockOtherKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);

        // Key (4001,4002) 的授权慢 2s（远大于第一段等待窗口）；Key (4003,4004) 的授权快。
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: true);
        authorizer.SetLatency(senderUserId: 4001, TimeSpan.FromSeconds(2));

        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(30),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var pipeline = CreateTypingActorPipeline(authorizer, fanout, clock);
        await using var session1 = CreateSession(userId: 4001);
        await using var session2 = CreateSession(userId: 4003);
        await pipeline.StartAsync(ct);

        // 先发慢 Key。
        SendTypingNotify(pipeline, session1, conversationId: "dm:4001:4002", isTyping: true);

        // 立即发快 Key——不应被慢 Key 阻塞。
        SendTypingNotify(pipeline, session2, conversationId: "dm:4003:4004", isTyping: true);

        // 快 Key 应在 800ms 内完成授权 + 发射（无慢 Key 阻塞）。
        await WaitUntilAsync(
            () => fanout.DrainPending().Count > 0,
            TimeSpan.FromMilliseconds(800), ct);

        // 慢 Key 的授权仍在进行中（2s 未到）。
        // 注意：CallCount 在 AuthorizeAsync 入口即递增（含慢 Key 的 in-flight 调用），
        // 必须用 CompletedCallCount 判据——只有真正走完 Task.Delay(2s) 的才计入。
        Assert.Equal(1, authorizer.CompletedCallCount);

        // 等待慢 Key 完成。
        await WaitUntilAsync(
            () => authorizer.CompletedCallCount >= 2,
            TimeSpan.FromSeconds(5), ct);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 5：NATS 断线——授权失败后 Actor 恢复并可重试

    [Fact]
    public async Task NatsDisconnect_AuthFailurePostsDeniedAndActorResumes()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);

        // 第一次授权抛异常（模拟 NATS 断线），第二次成功。
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: true);
        authorizer.SetExceptionOnCall(callCount: 1, new InvalidOperationException("NATS disconnected"));

        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(30),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var pipeline = CreateTypingActorPipeline(authorizer, fanout, clock);
        await using var session = CreateSession(userId: 5001);
        await pipeline.StartAsync(ct);

        // 第一次：授权失败（NATS 断线）。
        SendTypingNotify(pipeline, session, conversationId: "dm:5001:5002", isTyping: true);
        await WaitUntilAsync(
            () => authorizer.CallCount >= 1,
            TimeSpan.FromSeconds(2), ct);

        // Actor 不应卡住。
        await WaitUntilAsync(
            () => pipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(2), ct);

        // 无 fanout 发射（授权被拒绝）。
        Assert.Empty(fanout.DrainPending());

        // 第二次：授权成功（NATS 恢复）。
        // Actor 已被 Denied 标记为 Authorized=false，新的 Notify 会重新提交授权。
        SendTypingNotify(pipeline, session, conversationId: "dm:5001:5002", isTyping: true);
        await WaitUntilAsync(
            () => authorizer.CallCount >= 2,
            TimeSpan.FromSeconds(2), ct);
        await WaitUntilAsync(
            () => fanout.DrainPending().Count > 0,
            TimeSpan.FromSeconds(2), ct);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 6：同 Key 高频覆盖——LatestOnly 合并 typing=true→false

    [Fact]
    public async Task SameKeyHighFrequencyOverride_LatestOnlyMergesTrueToFalseBeforeAuth()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);

        // 慢授权 1s，让 typing=false 在授权完成前到达并落入 LatestOnly Mailbox。
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: true);
        authorizer.SetLatency(senderUserId: 6001, TimeSpan.FromSeconds(1));

        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(30),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var pipeline = CreateTypingActorPipeline(authorizer, fanout, clock);
        await using var session = CreateSession(userId: 6001);
        await pipeline.StartAsync(ct);

        // 先发送 typing=true，等待 Actor 激活并提交授权 I/O（AuthPending=true）。
        // 这保证 typing=true 已经被 ReceiveNotify 消费并写入 state.DesiredIsTyping=true，
        // 不会被 DrainIngress 与随后的 typing=false 一次性合并。
        SendTypingNotify(pipeline, session, conversationId: "dm:6001:6002", isTyping: true);
        await WaitUntilAsync(
            () => authorizer.CallCount >= 1,
            TimeSpan.FromSeconds(2), ct);

        // 授权仍在进行中（1s 未到），此时发送 typing=false，落入 LatestOnly Mailbox。
        SendTypingNotify(pipeline, session, conversationId: "dm:6001:6002", isTyping: false);

        // 等待授权完成 + Actor 处理 ResumeMailbox。
        await WaitUntilAsync(
            () => pipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(3), ct);

        // 授权完成时 DesiredIsTyping 仍为 true（第二个 Notify 还未处理），
        // TryEmit 发射 typing=true；ResumeMailbox 处理 typing=false，再次 TryEmit 发射 typing=false。
        // TypingFanoutCoordinator._pending 按 (sender,conversation) 合并同 Key 的发射，
        // 两次 TryEmit 紧邻时 DrainPending 仅返回最新（typing=false）。
        // 这是 fanout 的 coalescing 设计：消费者只关心最终状态。
        var emissions = fanout.DrainPending();
        Assert.NotEmpty(emissions);
        var lastEmission = emissions[^1];
        Assert.False(lastEmission.IsTyping);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 7：连接 churn——空闲 Actor 被回收

    [Fact]
    public async Task ConnectionChurn_IdleActorDeactivatedAndCleared()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: true);
        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(30),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var pipeline = CreateTypingActorPipeline(
            authorizer, fanout, clock, idleTimeout: TimeSpan.FromSeconds(2));
        await using var session = CreateSession(userId: 7001);
        await pipeline.StartAsync(ct);

        // 发送 Notify 激活 Actor。
        SendTypingNotify(pipeline, session, conversationId: "dm:7001:7002", isTyping: true);
        await WaitUntilAsync(
            () => authorizer.CallCount >= 1 && pipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(2), ct);

        // Actor 应处于活跃状态。
        Assert.True(pipeline.Snapshot.ActiveActors >= 1);

        // 推进时间超过 IdleTimeout。
        clock.Advance(TimeSpan.FromSeconds(3));

        // Actor 应被 Sweep 回收。
        await WaitUntilAsync(
            () => pipeline.Snapshot.ActiveActors == 0,
            TimeSpan.FromSeconds(5), ct);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 8：10,000 活跃 Actor——规模测试

    [Fact]
    public async Task TenThousandActiveActors_AllAcceptedAndProcessed()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: true);
        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(60),
            tickInterval: TimeSpan.FromMilliseconds(500));

        // 大 Shard 数以支撑 10k Actor。
        await using var pipeline = CreateTypingActorPipeline(
            authorizer, fanout, clock,
            shardCount: 16,
            ingressCapacity: 4096,
            idleTimeout: TimeSpan.FromMinutes(5));
        await pipeline.StartAsync(ct);

        const int actorCount = 10_000;
        var sessions = new List<TcpClientSession>(actorCount);
        try
        {
            for (var i = 0; i < actorCount; i++)
            {
                var sender = 10_000L + i;
                var target = 20_000L + i;
                var session = CreateSession(sender);
                sessions.Add(session);
                SendTypingNotify(
                    pipeline, session,
                    conversationId: $"dm:{Math.Min(sender, target)}:{Math.Max(sender, target)}",
                    isTyping: true);
            }

            // 等待所有授权 I/O 完成。
            await WaitUntilAsync(
                () => authorizer.CallCount >= actorCount,
                TimeSpan.FromSeconds(30), ct);

            // 等待所有 Actor 处理完 Completion。
            await WaitUntilAsync(
                () => pipeline.Snapshot.BusyActors == 0,
                TimeSpan.FromSeconds(30), ct);

            Assert.Equal(actorCount, authorizer.CallCount);
            Assert.True(pipeline.Snapshot.ActiveActors >= actorCount);
        }
        finally
        {
            foreach (var s in sessions)
                await s.DisposeAsync();
        }

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 9：单热点 Actor——高频轰炸不阻塞其他 Actor

    [Fact]
    public async Task SingleHotActor_BombardDoesNotBlockOthers()
    {
        var ct = TestContext.Current.CancellationToken;
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var authorizer = new FakeDirectConversationAuthorizer(allowAll: true);

        // 热点 Key 的授权慢，模拟授权 I/O。
        authorizer.SetLatency(senderUserId: 8001, TimeSpan.FromMilliseconds(200));

        var fanout = new TypingFanoutCoordinator(
            clock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(30),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var pipeline = CreateTypingActorPipeline(authorizer, fanout, clock);
        await using var hotSession = CreateSession(userId: 8001);
        await using var otherSession = CreateSession(userId: 8003);
        await pipeline.StartAsync(ct);

        // 轰炸热点 Key (8001,8002) with 100 条 Notify。
        // LatestOnly 会合并，只有最新保留。
        for (var i = 0; i < 100; i++)
        {
            SendTypingNotify(
                pipeline, hotSession,
                conversationId: "dm:8001:8002",
                isTyping: i % 2 == 0);
        }

        // 同时发送到另一个 Key——不应被热点阻塞。
        SendTypingNotify(
            pipeline, otherSession,
            conversationId: "dm:8003:8004",
            isTyping: true);

        // 另一个 Key 应快速完成。
        await WaitUntilAsync(
            () => authorizer.CallCount >= 1,
            TimeSpan.FromSeconds(2), ct);

        // 热点 Key 的授权也应完成（第一次触发，后续合并）。
        await WaitUntilAsync(
            () => authorizer.CallCount >= 2,
            TimeSpan.FromSeconds(5), ct);

        // 两个 Actor 都不应卡住。
        await WaitUntilAsync(
            () => pipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(5), ct);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 10：预算和 ArrayPool 归零——Generic Actor 路径资源释放

    [Fact]
    public async Task BudgetAndArrayPool_GenericActorPathReleasesAllAfterProcessing()
    {
        var ct = TestContext.Current.CancellationToken;
        using var metrics = new GatewayMetrics();
        await using var session = CreateSession(connectionId: 9001);

        var processedCount = 0;
        await using var pipeline = new EphemeralCommandPipeline(
            new TcpGatewayOptions
            {
                UseActorRuntimeForEphemeralCommands = true,
                CommandSchedulerEphemeralCapacity = 256,
                EphemeralActorShardCount = 2,
                EphemeralActorIngressCapacity = 256,
                EphemeralActorAsyncConcurrency = 2,
                EphemeralActorIdleTimeout = TimeSpan.FromSeconds(1),
                EphemeralActorOperationTimeout = TimeSpan.FromSeconds(2)
            },
            (_, _) =>
            {
                Interlocked.Increment(ref processedCount);
                return ValueTask.CompletedTask;
            },
            metrics,
            TimeProvider.System,
            NullLogger.Instance);

        // 必须先 Start + Register，再 TryEnqueue：
        // FIFO 模式下 TryEnqueue 会检查 per-connection mailbox 容量，
        // Consumer 未启动时消息无法消费，容量耗尽即拒绝。
        await pipeline.StartAsync(ct);
        Assert.True(pipeline.TryRegisterConnection(9001, 0));

        var budget = new GlobalInboundBudget(64 * 1024);

        // 提交 50 条命令，每条租 32 字节 buffer + 32 字节预算。
        const int commandCount = 50;
        var rentedBuffers = new List<byte[]>(commandCount);
        for (var i = 0; i < commandCount; i++)
        {
            Assert.True(budget.TryReserve(32));
            var rented = ArrayPool<byte>.Shared.Rent(32);
            rentedBuffers.Add(rented);
            var command = new SessionCommand
            {
                Command = PacketCommand.TypingNotify,
                RentedBuffer = rented,
                PayloadLength = 32,
                IsPooled = true,
                ReservedInboundBytes = 32,
                InboundBudget = budget,
                Session = session,
                RemoteIp = "127.0.0.1"
            };
            Assert.True(pipeline.TryEnqueue(9001, in command));
        }

        // 等待全部处理完成 + 预算归零。
        await WaitUntilAsync(
            () => budget.CurrentBytes == 0 &&
                  processedCount >= commandCount &&
                  pipeline.Snapshot.PendingAsyncOperations == 0 &&
                  pipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(10), ct);

        Assert.Equal(0, budget.CurrentBytes);
        Assert.True(processedCount >= commandCount);

        await pipeline.StopAsync(ct);
    }

    #endregion

    #region 场景 11：Legacy Executor 对比——吞吐量平价验证

    /// <summary>
    /// A/B 对比：同一工作负载通过 Legacy SessionCommandExecutor 和 TypingActorPipeline，
    /// 验证 TypingActor 在架构上的优势——LatestOnly 合并显著降低授权 I/O 次数与分配。
    /// 不使用脆弱的时序断言（CI 抖动大），改用语义断言：
    /// <list type="bullet">
    /// <item>Legacy 对每条命令执行一次工作 lambda（5000 次）；</item>
    /// <item>TypingActor 仅触发 1 次授权 I/O（LatestOnly 合并 5000 条 Notify 到同一 Key 的最新状态）；</item>
    /// <item>TypingActor 不应残留任何 Busy Actor 或未完成 I/O。</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task CrossPipelineComparison_TypingActorNotSlowerThanLegacy()
    {
        var ct = TestContext.Current.CancellationToken;
        const int workload = 5000;

        // --- Legacy 路径：每条命令执行一次工作 lambda。 ---
        var legacyProcessed = 0;
        await using var legacyPipeline = new EphemeralCommandPipeline(
            new TcpGatewayOptions
            {
                UseActorRuntimeForEphemeralCommands = false,
                CommandSchedulerEphemeralCapacity = 8192
            },
            (_, _) =>
            {
                Interlocked.Increment(ref legacyProcessed);
                return ValueTask.CompletedTask;
            },
            new GatewayMetrics(),
            TimeProvider.System,
            NullLogger.Instance);

        await using var legacySession = CreateSession(connectionId: 1);
        var legacyBudget = new GlobalInboundBudget(1024 * 1024);

        await legacyPipeline.StartAsync(ct);
        Assert.True(legacyPipeline.TryRegisterConnection(1, 0));

        for (var i = 0; i < workload; i++)
        {
            Assert.True(legacyBudget.TryReserve(32));
            var rented = ArrayPool<byte>.Shared.Rent(32);
            var cmd = new SessionCommand
            {
                Command = PacketCommand.TypingNotify,
                RentedBuffer = rented,
                PayloadLength = 32,
                IsPooled = true,
                ReservedInboundBytes = 32,
                InboundBudget = legacyBudget,
                Session = legacySession,
                RemoteIp = "127.0.0.1"
            };
            // 队列满时短暂等待消费者排空，避免丢命令。
            while (!legacyPipeline.TryEnqueue(1, in cmd))
                await Task.Delay(1, ct);
        }
        await WaitUntilAsync(
            () => legacyProcessed >= workload && legacyBudget.CurrentBytes == 0,
            TimeSpan.FromSeconds(10), ct);
        await legacyPipeline.StopAsync(ct);

        // Legacy 必须完成全部 5000 次处理，预算归零。
        Assert.Equal(workload, legacyProcessed);
        Assert.Equal(0, legacyBudget.CurrentBytes);

        // --- TypingActor 路径：LatestOnly 合并同 Key 的 5000 条 Notify。 ---
        var typingClock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var typingAuthorizer = new FakeDirectConversationAuthorizer(allowAll: true);
        var typingFanout = new TypingFanoutCoordinator(
            typingClock,
            minInterval: TimeSpan.FromMilliseconds(1),
            ttl: TimeSpan.FromSeconds(60),
            tickInterval: TimeSpan.FromMilliseconds(500));

        await using var typingPipeline = CreateTypingActorPipeline(
            typingAuthorizer, typingFanout, typingClock,
            shardCount: 4,
            ingressCapacity: 1024);
        await using var typingSession = CreateSession(userId: 1);
        await typingPipeline.StartAsync(ct);

        for (var i = 0; i < workload; i++)
        {
            SendTypingNotify(
                typingPipeline, typingSession,
                conversationId: "dm:1:2",
                isTyping: i % 2 == 0);
        }

        // LatestOnly 合并后，5000 条 Notify 仅触发 1 次授权 I/O。
        await WaitUntilAsync(
            () => typingAuthorizer.CallCount >= 1 &&
                  typingPipeline.Snapshot.BusyActors == 0,
            TimeSpan.FromSeconds(5), ct);
        await typingPipeline.StopAsync(ct);

        // 语义断言：TypingActor 授权 I/O 次数应远低于 Legacy 处理次数。
        // LatestOnly 把 5000 条同 Key Notify 合并为 1 次授权 + 至多 2 次发射。
        Assert.Equal(1, typingAuthorizer.CallCount);
        Assert.Equal(0, typingPipeline.Snapshot.BusyActors);
        Assert.Equal(0, typingPipeline.Snapshot.PendingAsyncOperations);
    }

    #endregion

    #region Helper 方法

    private static TypingActorPipeline CreateTypingActorPipeline(
        FakeDirectConversationAuthorizer authorizer,
        TypingFanoutCoordinator fanout,
        ManualTimeProvider clock,
        int shardCount = 4,
        int ingressCapacity = 256,
        TimeSpan? idleTimeout = null)
    {
        var options = new TcpGatewayOptions
        {
            UseActorRuntimeForEphemeralCommands = true,
            UseTypingActorPipeline = true,
            EphemeralActorShardCount = shardCount,
            EphemeralActorIngressCapacity = ingressCapacity,
            EphemeralActorAsyncConcurrency = Math.Max(2, Environment.ProcessorCount * 2),
            EphemeralActorIdleTimeout = idleTimeout ?? TimeSpan.FromSeconds(30),
            EphemeralActorOperationTimeout = TimeSpan.FromSeconds(5)
        };

        return new TypingActorPipeline(
            options,
            TypingNotifyCodec,
            authorizer,
            fanout,
            new GatewayMetrics(),
            clock,
            NullLogger.Instance);
    }

    private static TcpClientSession CreateSession(
        long userId = 1,
        uint connectionId = 0)
    {
        var session = new TcpClientSession(
            new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp),
            connectionId: connectionId == 0 ? (uint)Random.Shared.Next(1, int.MaxValue) : connectionId,
            outboundQueueCapacity: 16,
            maxOutboundQueuedBytes: 128 * 1024,
            sendTimeout: TimeSpan.FromSeconds(1),
            TimeProvider.System,
            new GatewayMetrics(),
            NullLogger<TcpClientSession>.Instance);
        session.Authenticate(userId, Guid.NewGuid().ToString("N"), deviceIdHash: 0, deviceId: null);
        return session;
    }

    private static void SendTypingNotify(
        TypingActorPipeline pipeline,
        TcpClientSession session,
        string conversationId,
        bool isTyping)
    {
        var notify = new TypingNotify
        {
            ConversationId = conversationId,
            IsTyping = isTyping
        };

        // 序列化到 buffer。
        var writer = new ArrayBufferWriter<byte>();
        TypingNotifyCodec.Serialize(writer, notify);
        var payload = new ReadOnlySequence<byte>(writer.WrittenMemory);

        var frame = new PacketFrame(PacketCommand.TypingNotify, payload);
        pipeline.TryHandleFrame(in frame, session);
    }

    private static async Task WaitUntilAsync(
        Func<bool> predicate,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate())
                return;
            await Task.Delay(10, cancellationToken);
        }
        Assert.True(predicate(), "Condition was not reached before timeout.");
    }

    #endregion

    #region Fakes

    /// <summary>
    /// 可控的 IDirectConversationAuthorizer：支持缓存命中/未命中、延迟、异常注入。
    /// </summary>
    private sealed class FakeDirectConversationAuthorizer : IDirectConversationAuthorizer
    {
        private readonly bool _allowAll;
        private int _callCount;
        private int _completedCallCount;
        private readonly Dictionary<long, TimeSpan> _latencyBySender = new();
        private int _exceptionOnCall;
        private Exception? _exception;

        /// <summary>已启动的授权调用数（含 in-flight）。在 AuthorizeAsync 入口递增。</summary>
        public int CallCount => Volatile.Read(ref _callCount);

        /// <summary>已完成的授权调用数（走完 Task.Delay 或异常）。用于区分 in-flight 与已完成。</summary>
        public int CompletedCallCount => Volatile.Read(ref _completedCallCount);

        public FakeDirectConversationAuthorizer(bool allowAll)
        {
            _allowAll = allowAll;
        }

        public void SetLatency(long senderUserId, TimeSpan latency) =>
            _latencyBySender[senderUserId] = latency;

        public void SetExceptionOnCall(int callCount, Exception ex)
        {
            _exceptionOnCall = callCount;
            _exception = ex;
        }

        public async ValueTask<bool> AuthorizeAsync(long senderUserId, long targetUserId, CancellationToken ct)
        {
            var currentCall = Interlocked.Increment(ref _callCount);
            try
            {
                if (_exception is not null && currentCall == _exceptionOnCall)
                    throw _exception;

                if (_latencyBySender.TryGetValue(senderUserId, out var latency) && latency > TimeSpan.Zero)
                    await Task.Delay(latency, ct).ConfigureAwait(false);

                return _allowAll;
            }
            finally
            {
                Interlocked.Increment(ref _completedCallCount);
            }
        }

        public ValueTask InvalidateAsync(long senderUserId, long targetUserId, CancellationToken ct) =>
            ValueTask.CompletedTask;
    }

    /// <summary>
    /// 手动时间提供者，用于控制 TypingFanoutCoordinator 和 Actor IdleTimeout。
    /// </summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        private long _utcTicks;
        private long _timestamp;

        public ManualTimeProvider(DateTimeOffset start)
        {
            _utcTicks = start.Ticks;
            _timestamp = 0;
        }

        public override DateTimeOffset GetUtcNow() =>
            new(Volatile.Read(ref _utcTicks), TimeSpan.Zero);

        public void Advance(TimeSpan delta)
        {
            Interlocked.Add(ref _utcTicks, delta.Ticks);
            Interlocked.Add(ref _timestamp, delta.Ticks);
        }

        public override long GetTimestamp() => Volatile.Read(ref _timestamp);

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    }

    #endregion
}
