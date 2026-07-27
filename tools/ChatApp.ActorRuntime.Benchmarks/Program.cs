using System.Diagnostics;
using ChatApp.ActorRuntime.Abstractions;
using ChatApp.ActorRuntime.Runtime;

var messageCount = ReadInt(args, "--messages", 1_000_000);
var keyCount = ReadInt(args, "--keys", 16_384);
var producerCount = ReadInt(
    args,
    "--producers",
    Math.Max(2, Environment.ProcessorCount));
var shardCount = NextPowerOfTwo(
    ReadInt(args, "--shards", Math.Max(2, Environment.ProcessorCount)));

Console.WriteLine(
    $"ActorRuntime benchmark: messages={messageCount:N0}, keys={keyCount:N0}, " +
    $"producers={producerCount}, shards={shardCount}");

await RunPassAsync(
    Math.Min(100_000, messageCount),
    keyCount,
    producerCount,
    shardCount,
    report: false);
GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
await RunPassAsync(
    messageCount,
    keyCount,
    producerCount,
    shardCount,
    report: true);

static async Task RunPassAsync(
    int messageCount,
    int keyCount,
    int producerCount,
    int shardCount,
    bool report)
{
    await using var runtime =
        new ActorRuntime<int, BenchmarkState, BenchmarkMessage>(
            new BenchmarkBehavior(),
            ActorMailboxMode.Fifo,
            new ActorRuntimeOptions
            {
                ShardCount = shardCount,
                ShardIngressCapacity = 4096,
                DefaultMailboxCapacity = 64,
                ShardBurstLimit = 128,
                MaxMessagesPerActorTurn = 16,
                ShardTickInterval = TimeSpan.FromMilliseconds(50),
                AsyncOperationConcurrency = 2,
                AsyncOperationQueueCapacity = 16,
                ActorIdleTimeout = TimeSpan.FromMinutes(5)
            });

    await runtime.StartAsync(CancellationToken.None);

    // 先激活全部 Actor/Admission，使正式计量聚焦稳态热路径，而不是首次建表分配。
    var activationSpinner = new SpinWait();
    for (var key = 0; key < keyCount; key++)
    {
        var activation = new BenchmarkMessage(-1);
        while (runtime.TryTell(in key, in activation) !=
               ActorPostStatus.Accepted)
        {
            activationSpinner.SpinOnce();
        }

        activationSpinner.Reset();
    }

    while (runtime.GetSnapshot().TotalProcessed < keyCount)
        await Task.Delay(1);

    GC.Collect(
        2,
        GCCollectionMode.Aggressive,
        blocking: true,
        compacting: true);
    var processedBefore = runtime.GetSnapshot().TotalProcessed;
    var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var gen0Before = GC.CollectionCount(0);
    var gen1Before = GC.CollectionCount(1);
    var gen2Before = GC.CollectionCount(2);
    var next = -1;
    long retries = 0;
    var stopwatch = Stopwatch.StartNew();

    var producers = new Task[producerCount];
    for (var producer = 0; producer < producers.Length; producer++)
    {
        producers[producer] = Task.Run(() =>
        {
            var spinner = new SpinWait();
            while (true)
            {
                var sequence = Interlocked.Increment(ref next);
                if (sequence >= messageCount)
                    return;

                var key = sequence % keyCount;
                var message = new BenchmarkMessage(sequence);
                while (runtime.TryTell(in key, in message) !=
                       ActorPostStatus.Accepted)
                {
                    Interlocked.Increment(ref retries);
                    spinner.SpinOnce();
                }
                spinner.Reset();
            }
        });
    }

    await Task.WhenAll(producers);
    while (runtime.GetSnapshot().TotalProcessed < processedBefore + messageCount)
        await Task.Delay(1);

    stopwatch.Stop();
    var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
    var snapshot = runtime.GetSnapshot();
    await runtime.StopAsync(
        ActorStopMode.Drain,
        CancellationToken.None);

    if (!report)
        return;

    var perSecond = messageCount / stopwatch.Elapsed.TotalSeconds;
    Console.WriteLine($"elapsed_ms={stopwatch.Elapsed.TotalMilliseconds:F2}");
    Console.WriteLine($"throughput_msg_s={perSecond:F0}");
    Console.WriteLine($"allocated_bytes={allocated}");
    Console.WriteLine($"allocated_bytes_per_message={(double)allocated / messageCount:F3}");
    Console.WriteLine($"producer_retries={Volatile.Read(ref retries)}");
    Console.WriteLine($"gen0={GC.CollectionCount(0) - gen0Before}");
    Console.WriteLine($"gen1={GC.CollectionCount(1) - gen1Before}");
    Console.WriteLine($"gen2={GC.CollectionCount(2) - gen2Before}");
    Console.WriteLine($"preactivated_keys={keyCount}");
    Console.WriteLine($"active_actors={snapshot.ActiveActors}");
    Console.WriteLine($"mailbox_full={snapshot.TotalMailboxFull}");
    Console.WriteLine($"shard_overloaded={snapshot.TotalShardOverloaded}");
}

static int ReadInt(
    string[] args,
    string name,
    int fallback)
{
    for (var i = 0; i < args.Length - 1; i++)
    {
        if (string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase) &&
            int.TryParse(args[i + 1], out var value) &&
            value > 0)
        {
            return value;
        }
    }

    return fallback;
}

static int NextPowerOfTwo(int value)
{
    value--;
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    return value + 1;
}

internal struct BenchmarkState
{
    public long LastSequence;
}

internal readonly record struct BenchmarkMessage(long Sequence);

internal sealed class BenchmarkBehavior :
    IActorBehavior<int, BenchmarkState, BenchmarkMessage>
{
    public void Activate(
        in int key,
        ref BenchmarkState state,
        ref ActorContext<int, BenchmarkState, BenchmarkMessage> context)
    {
        state.LastSequence = -1;
    }

    public ActorTurnResult Receive(
        in int key,
        ref BenchmarkState state,
        in BenchmarkMessage message,
        ref ActorContext<int, BenchmarkState, BenchmarkMessage> context)
    {
        state.LastSequence = message.Sequence;
        return ActorTurnResult.Continue;
    }

    public void Deactivate(
        in int key,
        ref BenchmarkState state,
        ActorDeactivateReason reason,
        ref ActorContext<int, BenchmarkState, BenchmarkMessage> context)
    {
    }
}
