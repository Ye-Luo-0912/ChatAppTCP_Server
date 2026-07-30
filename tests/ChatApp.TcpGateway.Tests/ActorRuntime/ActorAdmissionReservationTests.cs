using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Runtime;

namespace ChatApp.TcpGateway.Tests.ActorRuntime;

/// <summary>
/// P1-7：Durable Actor Admission 预留系统测试。
/// 验证 TryTellDurable 在生产侧消耗式预留全局 Actor 配额，
/// 消除"生产侧接受、消费侧静默丢弃"竞态。
/// </summary>
public sealed class ActorAdmissionReservationTests
{
    /// <summary>
    /// 简单计数 Behavior：累加 state.Processed，不阻塞。
    /// </summary>
    private sealed class CounterBehavior : IActorBehavior<int, CounterState, CounterMessage>
    {
        public void Activate(
            in int key,
            ref CounterState state,
            ref ActorContext<int, CounterState, CounterMessage> context)
        {
            state = new CounterState { Processed = 0 };
        }

        public ActorTurnResult Receive(
            in int key,
            ref CounterState state,
            in CounterMessage message,
            ref ActorContext<int, CounterState, CounterMessage> context)
        {
            state.Processed += message.Delta;
            return ActorTurnResult.Continue;
        }

        public void Deactivate(
            in int key,
            ref CounterState state,
            ActorDeactivateReason reason,
            ref ActorContext<int, CounterState, CounterMessage> context)
        {
        }
    }

    private struct CounterState
    {
        public int Processed;
    }

    private readonly struct CounterMessage
    {
        public int Delta { get; init; }
    }

    /// <summary>
    /// 诊断测试：TryTellDurable 投递 2 条不同 Key 消息，检查是否都被处理。
    /// 用小配额（MaxActiveActors=2）复现 RejectedWhenGlobalQuotaFull 的场景。
    /// </summary>
    [Fact]
    public async Task Diag_TryTellDurable_TwoMessagesBothProcessed()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var runtime = new ActorRuntime<int, CounterState, CounterMessage>(
            new CounterBehavior(),
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 1,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 64,
                ActorIdleTimeout = TimeSpan.FromMinutes(5),
                MaxActiveActors = 2,
                MaxActiveActorsPerShard = 2
            });

        await runtime.StartAsync(ct);

        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(1, new CounterMessage { Delta = 1 }));
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(2, new CounterMessage { Delta = 1 }));

        await WaitUntilAsync(
            () => runtime.GetSnapshot().TotalProcessed >= 2,
            TimeSpan.FromSeconds(2), ct);

        var snap = runtime.GetSnapshot();
        Assert.True(snap.TotalProcessed >= 2,
            $"TotalProcessed={snap.TotalProcessed}, ActiveActors={snap.ActiveActors}, " +
            $"PendingIngress={snap.PendingIngress}, PendingMailbox={snap.PendingMailbox}, " +
            $"AdmissionRejected={snap.TotalActiveActorAdmissionRejected}");

        await runtime.StopAsync(ActorStopMode.Immediate, ct);
    }

    /// <summary>
    /// 全局配额满时，TryTellDurable 返回 AdmissionRejected 且不入队。
    /// </summary>
    [Fact]
    public async Task TryTellDurable_RejectedWhenGlobalQuotaFull()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var runtime = new ActorRuntime<int, CounterState, CounterMessage>(
            new CounterBehavior(),
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 1,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 64,
                ActorIdleTimeout = TimeSpan.FromMinutes(5),
                MaxActiveActors = 2,
                MaxActiveActorsPerShard = 2
            });

        await runtime.StartAsync(ct);

        // 占满 2 个 Actor 配额
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(1, new CounterMessage { Delta = 1 }));
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(2, new CounterMessage { Delta = 1 }));

        // 等待 Consumer 创建 2 个 Actor
        await WaitUntilAsync(
            () => runtime.GetSnapshot().ActiveActors == 2,
            TimeSpan.FromSeconds(2), ct);

        // 诊断：确认 Consumer 确实处理了消息
        var diag = runtime.GetSnapshot();
        Assert.True(diag.ActiveActors == 2,
            $"诊断失败: ActiveActors={diag.ActiveActors}, TotalProcessed={diag.TotalProcessed}, " +
            $"PendingIngress={diag.PendingIngress}, PendingMailbox={diag.PendingMailbox}");

        // 第 3 个不同 Key 应被拒绝（全局配额满）
        var status = runtime.TryTellDurable(3, new CounterMessage { Delta = 1 });
        Assert.Equal(ActorPostStatus.AdmissionRejected, status);

        await runtime.StopAsync(ActorStopMode.Immediate, ct);
    }

    /// <summary>
    /// 每 Shard 上限满时，TryTellDurable 返回 AdmissionRejected。
    /// </summary>
    [Fact]
    public async Task TryTellDurable_RejectedWhenShardLimitFull()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var runtime = new ActorRuntime<int, CounterState, CounterMessage>(
            new CounterBehavior(),
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 2,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 64,
                ActorIdleTimeout = TimeSpan.FromMinutes(5),
                MaxActiveActors = 100,
                MaxActiveActorsPerShard = 1
            });

        await runtime.StartAsync(ct);

        // Key 1 落到某 Shard，占满该 Shard 的 1 个 Actor 上限
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(1, new CounterMessage { Delta = 1 }));
        await WaitUntilAsync(
            () => runtime.GetSnapshot().ActiveActors == 1,
            TimeSpan.FromSeconds(2), ct);

        // Key 3 与 Key 1 同 Shard（StableHash(1) 和 StableHash(3) 的奇偶性可能不同，
        // 用足够多 Key 确保至少一个落到同 Shard）
        var anyRejected = false;
        for (var i = 3; i < 20; i++)
        {
            if (runtime.TryTellDurable(i, new CounterMessage { Delta = 1 }) == ActorPostStatus.AdmissionRejected)
            {
                anyRejected = true;
                break;
            }
        }
        Assert.True(anyRejected, "至少应有一个同 Shard Key 被拒绝");

        await runtime.StopAsync(ActorStopMode.Immediate, ct);
    }

    /// <summary>
    /// P1-7 核心不变量：TryTellDurable 返回 Accepted 后，
    /// 消费侧不会因配额超限而静默丢弃消息。
    /// 验证：Accepted 的消息全部被处理，TotalActiveActorAdmissionRejected == 0。
    /// </summary>
    [Fact]
    public async Task TryTellDurable_AcceptedGuaranteesNoConsumerSideSilentDrop()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var runtime = new ActorRuntime<int, CounterState, CounterMessage>(
            new CounterBehavior(),
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 1,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 64,
                ActorIdleTimeout = TimeSpan.FromMinutes(5),
                MaxActiveActors = 4,
                MaxActiveActorsPerShard = 4
            });

        await runtime.StartAsync(ct);

        // 投递 4 个不同 Key（正好占满配额），全部应 Accepted
        for (var i = 1; i <= 4; i++)
        {
            Assert.Equal(
                ActorPostStatus.Accepted,
                runtime.TryTellDurable(i, new CounterMessage { Delta = 1 }));
        }

        // 等待全部处理完成
        await WaitUntilAsync(
            () => runtime.GetSnapshot().TotalProcessed >= 4 &&
                  runtime.GetSnapshot().ActiveActors == 4,
            TimeSpan.FromSeconds(3), ct);

        var snapshot = runtime.GetSnapshot();
        Assert.Equal(4, snapshot.ActiveActors);
        Assert.True(snapshot.TotalProcessed >= 4);
        // 核心断言：无消费侧配额拒绝
        Assert.Equal(0, snapshot.TotalActiveActorAdmissionRejected);

        await runtime.StopAsync(ActorStopMode.Immediate, ct);
    }

    /// <summary>
    /// TryTellDurable 对已存在 Actor 投递时，预留配额立即释放（不泄漏）。
    /// 验证：对同一 Key 投递两次，ActiveActors == 1，配额只消耗 1。
    /// </summary>
    [Fact]
    public async Task TryTellDurable_ReleasesReservationWhenActorAlreadyExists()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var runtime = new ActorRuntime<int, CounterState, CounterMessage>(
            new CounterBehavior(),
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 1,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 64,
                ActorIdleTimeout = TimeSpan.FromMinutes(5),
                MaxActiveActors = 2,
                MaxActiveActorsPerShard = 2
            });

        await runtime.StartAsync(ct);

        // 第一次：创建 Actor，消耗 1 配额
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(1, new CounterMessage { Delta = 1 }));
        await WaitUntilAsync(
            () => runtime.GetSnapshot().ActiveActors == 1,
            TimeSpan.FromSeconds(2), ct);

        // 第二次：同一 Key，复用 Actor，预留应释放
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(1, new CounterMessage { Delta = 1 }));
        await WaitUntilAsync(
            () => runtime.GetSnapshot().TotalProcessed >= 2,
            TimeSpan.FromSeconds(2), ct);

        // 此时 ActiveActors 仍为 1，配额只消耗 1，还能再创建 1 个新 Actor
        Assert.Equal(1, runtime.GetSnapshot().ActiveActors);
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(2, new CounterMessage { Delta = 1 }));
        await WaitUntilAsync(
            () => runtime.GetSnapshot().ActiveActors == 2,
            TimeSpan.FromSeconds(2), ct);

        // 配额已满，第 3 个新 Key 应被拒绝
        Assert.Equal(
            ActorPostStatus.AdmissionRejected,
            runtime.TryTellDurable(3, new CounterMessage { Delta = 1 }));

        Assert.Equal(0, runtime.GetSnapshot().TotalActiveActorAdmissionRejected);

        await runtime.StopAsync(ActorStopMode.Immediate, ct);
    }

    /// <summary>
    /// Deactivate 后配额递减，新 Actor 可被创建。
    /// </summary>
    [Fact]
    public async Task TryTellDurable_DeactivationReleasesQuotaForNewActor()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var runtime = new ActorRuntime<int, CounterState, CounterMessage>(
            new CounterBehavior(),
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = 1,
                DefaultMailboxCapacity = 16,
                ShardIngressCapacity = 64,
                // 使用较长 idle timeout 避免在断言期间被意外回收；
                // 通过显式 TryDeactivate 触发释放。
                ActorIdleTimeout = TimeSpan.FromMinutes(5),
                MaxActiveActors = 1,
                MaxActiveActorsPerShard = 1
            });

        await runtime.StartAsync(ct);

        // 占满唯一配额
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(1, new CounterMessage { Delta = 1 }));
        await WaitUntilAsync(
            () => runtime.GetSnapshot().ActiveActors == 1,
            TimeSpan.FromSeconds(2), ct);

        // 新 Key 被拒绝（唯一配额被 Key 1 占用）
        Assert.Equal(
            ActorPostStatus.AdmissionRejected,
            runtime.TryTellDurable(2, new CounterMessage { Delta = 1 }));

        // 显式 Deactivate Key 1 释放配额
        Assert.True(runtime.TryDeactivate(1, ActorDeactivateReason.Explicit));
        await WaitUntilAsync(
            () => runtime.GetSnapshot().ActiveActors == 0,
            TimeSpan.FromSeconds(2), ct);

        // 配额释放后新 Key 可创建
        Assert.Equal(ActorPostStatus.Accepted, runtime.TryTellDurable(2, new CounterMessage { Delta = 1 }));
        await WaitUntilAsync(
            () => runtime.GetSnapshot().ActiveActors == 1,
            TimeSpan.FromSeconds(2), ct);

        await runtime.StopAsync(ActorStopMode.Immediate, ct);
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
