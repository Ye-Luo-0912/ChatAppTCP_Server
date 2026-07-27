using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Runtime;

namespace ChatApp.TcpGateway.Tests.ActorRuntime;

/// <summary>
/// <see cref="ActorRuntime{TKey,TState,TMessage}"/> 集成测试。
/// 验证 TryTell、消息处理、Mailbox 模式、Actor 生命周期、并发等核心行为。
/// </summary>
public sealed class ActorRuntimeTests
{
    /// <summary>
    /// 简单计数器 Behavior：每个消息累加 state.Count。
    /// </summary>
    private sealed class CounterBehavior : IActorBehavior<string, CounterState, CounterMessage>
    {
        public void Activate(in string key, ref CounterState state, ref ActorContext<string, CounterState, CounterMessage> context)
        {
            state = new CounterState { Count = 0, LastKey = key };
        }

        public ActorTurnResult Receive(in string key, ref CounterState state, in CounterMessage message, ref ActorContext<string, CounterState, CounterMessage> context)
        {
            state.Count += message.Delta;
            return ActorTurnResult.Continue;
        }

        public void Deactivate(in string key, ref CounterState state, ActorDeactivateReason reason, ref ActorContext<string, CounterState, CounterMessage> context)
        {
            // no-op
        }
    }

    private struct CounterState
    {
        public int Count;
        public string LastKey;
    }

    private readonly struct CounterMessage
    {
        public int Delta { get; init; }
    }

    [Fact]
    public async Task TryTellReturnsRuntimeStoppingAfterStopAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var runtime = new ActorRuntime<string, CounterState, CounterMessage>(
            new CounterBehavior(),
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 2,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 64,
                ShardBurstLimit = 16,
                ActorIdleTimeout = TimeSpan.FromSeconds(60)
            });

        await runtime.StartAsync(ct);

        var status = runtime.TryTell("a", new CounterMessage { Delta = 1 });
        Assert.Equal(ActorPostStatus.Accepted, status);

        // 给 Consumer Loop 一点时间处理
        await Task.Delay(50, ct);

        await runtime.StopAsync(ActorStopMode.Immediate, ct);

        var statusAfterStop = runtime.TryTell("b", new CounterMessage { Delta = 1 });
        Assert.Equal(ActorPostStatus.RuntimeStopping, statusAfterStop);
    }

    [Fact]
    public async Task MessagesToSameKeyAreSerializedInOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        var processed = new List<int>();
        var behavior = new CaptureBehavior(processed);
        await using var runtime = new ActorRuntime<string, CaptureState, CaptureMessage>(
            behavior,
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 1, // 单 Shard 强制同 key 严格串行
                DefaultMailboxCapacity = 64,
                ShardIngressCapacity = 256,
                ShardBurstLimit = 64,
                ActorIdleTimeout = TimeSpan.FromSeconds(60)
            });

        await runtime.StartAsync(ct);

        // 投递 20 条消息到同一 key
        for (var i = 0; i < 20; i++)
        {
            Assert.Equal(ActorPostStatus.Accepted, runtime.TryTell("a", new CaptureMessage { Seq = i }));
        }

        // 等待处理完成
        await WaitUntilAsync(() => processed.Count >= 20, TimeSpan.FromSeconds(2), ct);

        Assert.Equal(20, processed.Count);
        // 严格按入队顺序处理
        for (var i = 0; i < 20; i++)
            Assert.Equal(i, processed[i]);

        await runtime.StopAsync(ActorStopMode.Immediate, ct);
    }

    [Fact]
    public async Task LatestOnlyMailboxReplacesOlderMessage()
    {
        var ct = TestContext.Current.CancellationToken;
        var processed = new List<int>();
        var behavior = new CaptureBehavior(processed);
        await using var runtime = new ActorRuntime<string, CaptureState, CaptureMessage>(
            behavior,
            ActorMailboxMode.LatestOnly,
            new ActorRuntimeOptions
            {
                ShardCount = 1,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 256,
                ShardBurstLimit = 16,
                ActorIdleTimeout = TimeSpan.FromSeconds(60)
            });

        await runtime.StartAsync(ct);

        // 快速投递 10 条到同一 key，LatestOnly 只保留最后一条
        for (var i = 0; i < 10; i++)
        {
            runtime.TryTell("a", new CaptureMessage { Seq = i });
        }

        await Task.Delay(200, ct);
        await runtime.StopAsync(ActorStopMode.Immediate, ct);

        // 可能处理一条或多条，但最后处理的必定是 seq=9
        Assert.NotEmpty(processed);
        Assert.Equal(9, processed[^1]);
    }

    [Fact]
    public async Task SnapshotReflectsProcessedCount()
    {
        var ct = TestContext.Current.CancellationToken;
        var behavior = new CounterBehavior();
        // Ingress 容量必须 >= 投递总数，否则哈希分布不均时单 Shard Ingress 溢出，
        // TryTell 返回 ShardOverloaded，消息被静默丢弃（测试不重试）。
        await using var runtime = new ActorRuntime<string, CounterState, CounterMessage>(
            behavior,
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 2,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 256,
                ShardBurstLimit = 16,
                ActorIdleTimeout = TimeSpan.FromSeconds(60)
            });

        await runtime.StartAsync(ct);

        var accepted = 0;
        for (var i = 0; i < 100; i++)
        {
            if (runtime.TryTell($"key-{i % 10}", new CounterMessage { Delta = 1 }) == ActorPostStatus.Accepted)
                accepted++;
        }

        // 所有消息必须被接受（Ingress 容量足够，运行时未停止）。
        Assert.Equal(100, accepted);

        await WaitUntilAsync(() => runtime.GetSnapshot().TotalProcessed >= 100, TimeSpan.FromSeconds(2), ct);

        var snapshot = runtime.GetSnapshot();
        Assert.True(snapshot.TotalProcessed >= 100,
            $"TotalProcessed={snapshot.TotalProcessed}, Accepted={accepted}, " +
            $"PendingIngress={snapshot.PendingIngress}, PendingMailbox={snapshot.PendingMailbox}");
        Assert.True(snapshot.ActiveActors > 0);

        await runtime.StopAsync(ActorStopMode.Immediate, ct);

        var finalSnapshot = runtime.GetSnapshot();
        Assert.Equal(0, finalSnapshot.ActiveActors);
    }

    [Fact]
    public async Task MoreThan256ReadyActorsAreAllProcessed()
    {
        var ct = TestContext.Current.CancellationToken;
        var processed = 0;
        var behavior = new AtomicCountBehavior(() =>
            Interlocked.Increment(ref processed));
        await using var runtime =
            new ActorRuntime<int, AtomicCountState, AtomicCountMessage>(
                behavior,
                ActorMailboxMode.Fifo,
                new ActorRuntimeOptions
                {
                    ShardCount = 1,
                    DefaultMailboxCapacity = 2,
                    ShardIngressCapacity = 1024,
                    ShardBurstLimit = 32,
                    MaxMessagesPerActorTurn = 1,
                    ActorIdleTimeout = TimeSpan.FromMinutes(1)
                });

        // Start 前填充 512 个不同 key，确保一次 DrainIngress 产生超过旧 Ready 容量的 Actor。
        for (var i = 0; i < 512; i++)
        {
            Assert.Equal(
                ActorPostStatus.Accepted,
                runtime.TryTell(i, new AtomicCountMessage()));
        }

        await runtime.StartAsync(ct);
        await WaitUntilAsync(
            () => Volatile.Read(ref processed) == 512,
            TimeSpan.FromSeconds(3),
            ct);

        Assert.Equal(512, Volatile.Read(ref processed));
    }

    [Fact]
    public async Task FifoAdmissionReturnsMailboxFullBeforeAcceptingTooMuch()
    {
        await using var runtime =
            new ActorRuntime<int, AtomicCountState, AtomicCountMessage>(
                new AtomicCountBehavior(() => { }),
                ActorMailboxMode.Fifo,
                new ActorRuntimeOptions
                {
                    ShardCount = 1,
                    DefaultMailboxCapacity = 2,
                    ShardIngressCapacity = 16,
                    ActorIdleTimeout = TimeSpan.FromMinutes(1)
                });

        Assert.Equal(
            ActorPostStatus.Accepted,
            runtime.TryTell(1, new AtomicCountMessage()));
        Assert.Equal(
            ActorPostStatus.Accepted,
            runtime.TryTell(1, new AtomicCountMessage()));
        Assert.Equal(
            ActorPostStatus.MailboxFull,
            runtime.TryTell(1, new AtomicCountMessage()));
    }

    [Fact]
    public async Task CompletionResumesSuspendedActorWithQueuedMessages()
    {
        var ct = TestContext.Current.CancellationToken;
        var observed = new System.Collections.Concurrent.ConcurrentQueue<int>();
        await using var runtime =
            new ActorRuntime<int, SuspendState, SuspendMessage>(
                new SuspendBehavior(observed),
                ActorMailboxMode.Fifo,
                new ActorRuntimeOptions
                {
                    ShardCount = 1,
                    DefaultMailboxCapacity = 8,
                    ShardIngressCapacity = 32,
                    ShardBurstLimit = 8,
                    MaxMessagesPerActorTurn = 8,
                    ActorIdleTimeout = TimeSpan.FromMinutes(1)
                });

        await runtime.StartAsync(ct);
        Assert.Equal(
            ActorPostStatus.Accepted,
            runtime.TryTell(7, new SuspendMessage(1, false)));
        Assert.Equal(
            ActorPostStatus.Accepted,
            runtime.TryTell(7, new SuspendMessage(2, false)));

        await WaitUntilAsync(
            () => runtime.GetSnapshot().BusyActors == 1,
            TimeSpan.FromSeconds(2),
            ct);

        var completion = new SuspendMessage(-1, true);
        Assert.Equal(
            ActorPostStatus.Accepted,
            runtime.TryTellCompletion(7, 1, in completion));

        await WaitUntilAsync(
            () => observed.Count == 3,
            TimeSpan.FromSeconds(2),
            ct);
        Assert.Equal(new[] { 1, -1, 2 }, observed.ToArray());
        Assert.Equal(0, runtime.GetSnapshot().BusyActors);
    }

    [Fact]
    public async Task DeadlineFiresWithoutAdditionalTraffic()
    {
        var ct = TestContext.Current.CancellationToken;
        var deadlineObserved = 0;
        await using var runtime =
            new ActorRuntime<int, DeadlineState, DeadlineMessage>(
                new DeadlineBehavior(() =>
                    Interlocked.Exchange(ref deadlineObserved, 1)),
                ActorMailboxMode.Fifo,
                new ActorRuntimeOptions
                {
                    ShardCount = 1,
                    DefaultMailboxCapacity = 4,
                    ShardIngressCapacity = 32,
                    ShardTickInterval = TimeSpan.FromMilliseconds(10),
                    ActorIdleTimeout = TimeSpan.FromMinutes(1)
                });

        await runtime.StartAsync(ct);
        Assert.Equal(
            ActorPostStatus.Accepted,
            runtime.TryTell(1, new DeadlineMessage(false)));

        await WaitUntilAsync(
            () => Volatile.Read(ref deadlineObserved) == 1,
            TimeSpan.FromSeconds(2),
            ct);
        Assert.Equal(1, Volatile.Read(ref deadlineObserved));
    }

    [Fact]
    public async Task DrainProcessesAllAcceptedMessages()
    {
        var ct = TestContext.Current.CancellationToken;
        var processed = 0;
        await using var runtime =
            new ActorRuntime<int, AtomicCountState, AtomicCountMessage>(
                new AtomicCountBehavior(() =>
                    Interlocked.Increment(ref processed)),
                ActorMailboxMode.Fifo,
                new ActorRuntimeOptions
                {
                    ShardCount = 1,
                    DefaultMailboxCapacity = 256,
                    ShardIngressCapacity = 512,
                    ActorIdleTimeout = TimeSpan.FromMinutes(1)
                });

        await runtime.StartAsync(ct);
        for (var i = 0; i < 200; i++)
        {
            Assert.Equal(
                ActorPostStatus.Accepted,
                runtime.TryTell(1, new AtomicCountMessage()));
        }

        await runtime.StopAsync(ActorStopMode.Drain, ct);
        Assert.Equal(200, Volatile.Read(ref processed));
        Assert.Equal(0, runtime.GetSnapshot().PendingIngress);
        Assert.Equal(0, runtime.GetSnapshot().PendingMailbox);
    }

    [Fact]
    public async Task LatestOnlyReplacementInvokesDropHandlerOnce()
    {
        var ct = TestContext.Current.CancellationToken;
        var replaced = 0;
        var handler = new CountingDropHandler(reason =>
        {
            if (reason == ActorMessageDropReason.Replaced)
                Interlocked.Increment(ref replaced);
        });
        await using var runtime =
            new ActorRuntime<int, AtomicCountState, AtomicCountMessage>(
                new AtomicCountBehavior(() => { }),
                ActorMailboxMode.LatestOnly,
                new ActorRuntimeOptions
                {
                    ShardCount = 1,
                    ShardIngressCapacity = 16,
                    ActorIdleTimeout = TimeSpan.FromMinutes(1)
                },
                dropHandler: handler);

        Assert.Equal(
            ActorPostStatus.Accepted,
            runtime.TryTell(1, new AtomicCountMessage()));
        Assert.Equal(
            ActorPostStatus.Accepted,
            runtime.TryTell(1, new AtomicCountMessage()));
        await runtime.StartAsync(ct);

        await WaitUntilAsync(
            () => Volatile.Read(ref replaced) == 1,
            TimeSpan.FromSeconds(2),
            ct);
        Assert.Equal(1, Volatile.Read(ref replaced));
    }

    [Fact]
    public async Task CompletionOverflowFallbackDoesNotLoseMessagesOnStop()
    {
        var stopped = 0;
        var handler = new CountingDropHandler(reason =>
        {
            if (reason == ActorMessageDropReason.RuntimeStopping)
                Interlocked.Increment(ref stopped);
        });
        await using var runtime =
            new ActorRuntime<int, AtomicCountState, AtomicCountMessage>(
                new AtomicCountBehavior(() => { }),
                ActorMailboxMode.Fifo,
                new ActorRuntimeOptions
                {
                    ShardCount = 1,
                    ShardIngressCapacity = 64,
                    ActorIdleTimeout = TimeSpan.FromMinutes(1)
                },
                dropHandler: handler);

        // Completion Ring 容量为 64；其余消息必须进入惰性 overflow fallback。
        for (var i = 0; i < 100; i++)
        {
            var message = new AtomicCountMessage();
            Assert.Equal(
                ActorPostStatus.Accepted,
                runtime.TryTellCompletion(i, 1, in message));
        }

        Assert.Equal(100, runtime.GetSnapshot().PendingIngress);
        await runtime.StopAsync(
            ActorStopMode.Immediate,
            TestContext.Current.CancellationToken);

        Assert.Equal(100, Volatile.Read(ref stopped));
        Assert.Equal(0, runtime.GetSnapshot().PendingIngress);
    }

    private sealed class CaptureBehavior : IActorBehavior<string, CaptureState, CaptureMessage>
    {
        private readonly List<int> _processed;

        public CaptureBehavior(List<int> processed) => _processed = processed;

        public void Activate(in string key, ref CaptureState state, ref ActorContext<string, CaptureState, CaptureMessage> context)
        {
            state = new CaptureState { Key = key };
        }

        public ActorTurnResult Receive(in string key, ref CaptureState state, in CaptureMessage message, ref ActorContext<string, CaptureState, CaptureMessage> context)
        {
            _processed.Add(message.Seq);
            return ActorTurnResult.Continue;
        }

        public void Deactivate(in string key, ref CaptureState state, ActorDeactivateReason reason, ref ActorContext<string, CaptureState, CaptureMessage> context)
        {
        }
    }

    private struct CaptureState
    {
        public string Key;
    }

    private readonly struct CaptureMessage
    {
        public int Seq { get; init; }
    }

    private struct AtomicCountState
    {
    }

    private readonly struct AtomicCountMessage
    {
    }

    private sealed class AtomicCountBehavior :
        IActorBehavior<int, AtomicCountState, AtomicCountMessage>
    {
        private readonly Action _onReceive;

        public AtomicCountBehavior(Action onReceive)
        {
            _onReceive = onReceive;
        }

        public void Activate(
            in int key,
            ref AtomicCountState state,
            ref ActorContext<int, AtomicCountState, AtomicCountMessage> context)
        {
        }

        public ActorTurnResult Receive(
            in int key,
            ref AtomicCountState state,
            in AtomicCountMessage message,
            ref ActorContext<int, AtomicCountState, AtomicCountMessage> context)
        {
            _onReceive();
            return ActorTurnResult.Continue;
        }

        public void Deactivate(
            in int key,
            ref AtomicCountState state,
            ActorDeactivateReason reason,
            ref ActorContext<int, AtomicCountState, AtomicCountMessage> context)
        {
        }
    }

    private struct SuspendState
    {
    }

    private readonly record struct SuspendMessage(int Value, bool IsCompletion);

    private sealed class SuspendBehavior :
        IActorBehavior<int, SuspendState, SuspendMessage>
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<int> _observed;

        public SuspendBehavior(
            System.Collections.Concurrent.ConcurrentQueue<int> observed)
        {
            _observed = observed;
        }

        public void Activate(
            in int key,
            ref SuspendState state,
            ref ActorContext<int, SuspendState, SuspendMessage> context)
        {
        }

        public ActorTurnResult Receive(
            in int key,
            ref SuspendState state,
            in SuspendMessage message,
            ref ActorContext<int, SuspendState, SuspendMessage> context)
        {
            _observed.Enqueue(message.Value);
            if (message.IsCompletion)
                return ActorTurnResult.ResumeMailbox;
            return message.Value == 1
                ? ActorTurnResult.Suspend
                : ActorTurnResult.Continue;
        }

        public void Deactivate(
            in int key,
            ref SuspendState state,
            ActorDeactivateReason reason,
            ref ActorContext<int, SuspendState, SuspendMessage> context)
        {
        }
    }

    private struct DeadlineState
    {
    }

    private readonly record struct DeadlineMessage(bool IsDeadline);

    private sealed class DeadlineBehavior :
        IActorBehavior<int, DeadlineState, DeadlineMessage>
    {
        private readonly Action _onDeadline;

        public DeadlineBehavior(Action onDeadline)
        {
            _onDeadline = onDeadline;
        }

        public void Activate(
            in int key,
            ref DeadlineState state,
            ref ActorContext<int, DeadlineState, DeadlineMessage> context)
        {
        }

        public ActorTurnResult Receive(
            in int key,
            ref DeadlineState state,
            in DeadlineMessage message,
            ref ActorContext<int, DeadlineState, DeadlineMessage> context)
        {
            if (message.IsDeadline)
            {
                _onDeadline();
            }
            else
            {
                var deadline = new DeadlineMessage(true);
                Assert.True(context.TrySchedule(
                    TimeSpan.FromMilliseconds(40),
                    context.Generation,
                    in deadline));
            }

            return ActorTurnResult.Continue;
        }

        public void Deactivate(
            in int key,
            ref DeadlineState state,
            ActorDeactivateReason reason,
            ref ActorContext<int, DeadlineState, DeadlineMessage> context)
        {
        }
    }

    private sealed class CountingDropHandler :
        IActorMessageDropHandler<AtomicCountMessage>
    {
        private readonly Action<ActorMessageDropReason> _onDrop;

        public CountingDropHandler(
            Action<ActorMessageDropReason> onDrop)
        {
            _onDrop = onDrop;
        }

        public void OnDropped(
            in AtomicCountMessage message,
            ActorMessageDropReason reason)
        {
            _onDrop(reason);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10, ct);
        }
    }
}
