using System.Diagnostics;
using System.Threading.Channels;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;

// 五-3：Outbound Channel 每连接内存成本微基准。
// 测量三种方案的 per-connection 内存成本，决定是否实施 Lazy Segmented MPSC Outbound Queue。
//
// 方案 A（当前）：每连接固定 Channel<OutboundWrite>(256) + Lifetime CTS + OutboundQueueBudget + 状态字段
// 方案 B（空连接 Lazy）：空闲连接不分配 Channel；首次入队才分配 8-16 Slot segment
// 方案 C（无 Channel）：仅状态字段，无 Channel（理论下界）
//
// 运行：dotnet run -c Release -- --connections 10000

var connectionCount = ReadInt(args, "--connections", 10_000);
var warmupConnections = Math.Min(1000, connectionCount);

Console.WriteLine(
    $"OutboundChannel memory benchmark: connections={connectionCount:N0}, " +
    $"warmup={warmupConnections:N0}");

// 预热
RunPass(warmupConnections, "warmup", report: false);
GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);

// 正式测量
RunPass(connectionCount, "channel-bounded-256", report: true, useChannel: true, useCts: true, useBudget: true);
GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);

RunPass(connectionCount, "state-only-no-channel", report: true, useChannel: false, useCts: false, useBudget: false);
GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);

RunPass(connectionCount, "lazy-segmented-mpsc", report: true, useLazy: true);
GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);

static void RunPass(
    int connectionCount,
    string label,
    bool report,
    bool useChannel = false,
    bool useCts = false,
    bool useBudget = false,
    bool useLazy = false)
{
    var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
    var gen0Before = GC.CollectionCount(0);
    var gen1Before = GC.CollectionCount(1);
    var gen2Before = GC.CollectionCount(2);

    GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);
    GC.WaitForPendingFinalizers();
    GC.Collect(2, GCCollectionMode.Aggressive, blocking: true, compacting: true);

    var memoryBefore = GC.GetTotalMemory(forceFullCollection: false);
    var stopwatch = Stopwatch.StartNew();

    // 模拟 N 个连接的 per-session 状态分配
    var sessions = new SessionState[connectionCount];
    for (var i = 0; i < connectionCount; i++)
    {
        sessions[i] = new SessionState(
            useChannel,
            useCts,
            useBudget,
            useLazy);
    }

    stopwatch.Stop();
    var memoryAfter = GC.GetTotalMemory(forceFullCollection: false);
    var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
    var managedMemoryDelta = memoryAfter - memoryBefore;

    // 防止 GC 在测量后回收
    GC.KeepAlive(sessions);

    var gen0 = GC.CollectionCount(0) - gen0Before;
    var gen1 = GC.CollectionCount(1) - gen1Before;
    var gen2 = GC.CollectionCount(2) - gen2Before;

    if (!report)
        return;

    var bytesPerConn = managedMemoryDelta / (double)connectionCount;
    var allocatedPerConn = allocated / (double)connectionCount;

    Console.WriteLine($"--- {label} ---");
    Console.WriteLine($"  elapsed_ms={stopwatch.Elapsed.TotalMilliseconds:F2}");
    Console.WriteLine($"  managed_memory_delta_bytes={managedMemoryDelta:N0}");
    Console.WriteLine($"  bytes_per_connection={bytesPerConn:F1}");
    Console.WriteLine($"  allocated_bytes={allocated:N0}");
    Console.WriteLine($"  allocated_per_connection={allocatedPerConn:F1}");
    Console.WriteLine($"  gen0={gen0} gen1={gen1} gen2={gen2}");
    Console.WriteLine();
}

static int ReadInt(string[] args, string name, int fallback)
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

/// <summary>
/// 模拟单个连接的 per-session 状态分配。
/// 根据 useChannel/useCts/useBudget/useLazy 控制哪些组件被分配。
/// </summary>
internal sealed class SessionState : IDisposable
{
    // 方案 A：当前实现的 per-session 组件
    private readonly Channel<OutboundWrite>? _outbound;
    private readonly CancellationTokenSource? _lifetimeCts;
    private readonly OutboundQueueBudget? _budget;

    // 方案 B：Lazy Segmented MPSC（首次入队才分配）
    private readonly LazyMpscQueue<OutboundWrite>? _lazyQueue;

    // 公共状态字段（所有方案都有）——模拟真实 Session 的固定开销
    internal int _sendState;             // Idle/Queued/Running 状态机
    internal long _sendStartedAt;        // 发送开始单调时间戳
    internal int _sendInProgress;        // 发送中标志（CAS 1→0 关闭）
    internal long _connectionId;         // 连接唯一标识
    internal long _userId;               // 认证后用户 ID
    internal long _deviceIdHash;         // 设备指纹哈希
    internal long _authenticatedAtMs;    // 认证时间戳
    internal int _frameAssemblyState;    // 帧装配状态

    public SessionState(bool useChannel, bool useCts, bool useBudget, bool useLazy)
    {
        // 初始化状态字段，避免编译器警告并模拟真实分配
        _sendState = 0;
        _sendStartedAt = 0;
        _sendInProgress = 0;
        _connectionId = 0;
        _userId = 0;
        _deviceIdHash = 0;
        _authenticatedAtMs = 0;
        _frameAssemblyState = 0;

        if (useChannel)
        {
            _outbound = Channel.CreateBounded<OutboundWrite>(
                new BoundedChannelOptions(256)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait
                });
        }

        if (useCts)
        {
            _lifetimeCts = new CancellationTokenSource();
        }

        if (useBudget)
        {
            _budget = new OutboundQueueBudget(256 * 1024);
        }

        if (useLazy)
        {
            _lazyQueue = new LazyMpscQueue<OutboundWrite>(initialSegmentSlots: 16);
        }
    }

    public void Dispose()
    {
        _lifetimeCts?.Dispose();
        _lazyQueue?.Dispose();
    }
}

/// <summary>
/// 方案 B：Lazy Segmented MPSC Queue。
/// 空闲连接不分配 Segment；首次入队才分配 8-16 Slot segment。
/// 单消费者（SendLoop/drain/pump），多生产者（业务线程入队）。
/// FIFO Durable，字节预算独立。
/// </summary>
internal sealed class LazyMpscQueue<T> : IDisposable where T : struct
{
    private Segment? _head;
    private Segment? _tail;
    private readonly int _segmentSlots;

    public LazyMpscQueue(int initialSegmentSlots)
    {
        _segmentSlots = initialSegmentSlots;
    }

    public bool IsAllocated => _head is not null;

    public void Enqueue(in T item)
    {
        if (_head is null)
        {
            // 首次入队才分配
            var segment = new Segment(_segmentSlots);
            _head = segment;
            _tail = segment;
        }

        if (!_head.TryEnqueue(in item))
        {
            // 当前 segment 满，分配新 segment
            var newSegment = new Segment(_segmentSlots);
            _head.Next = newSegment;
            _head = newSegment;
            _head.TryEnqueue(in item);
        }
    }

    public bool TryDequeue(out T item)
    {
        if (_tail is null)
        {
            item = default;
            return false;
        }

        if (_tail.TryDequeue(out item))
            return true;

        // 当前 segment 空，前进到下一个
        if (_tail.Next is not null)
        {
            _tail = _tail.Next;
            return _tail.TryDequeue(out item);
        }

        item = default;
        return false;
    }

    public void Dispose()
    {
        _head = null;
        _tail = null;
    }

    private sealed class Segment(int slots)
    {
        private readonly T[] _items = new T[slots];
        private int _head;
        private int _tail;
        private int _count;

        public Segment? Next;

        public bool TryEnqueue(in T item)
        {
            if (_count >= slots)
                return false;
            _items[_tail] = item;
            _tail = (_tail + 1) % slots;
            Interlocked.Increment(ref _count);
            return true;
        }

        public bool TryDequeue(out T item)
        {
            if (_count == 0)
            {
                item = default;
                return false;
            }
            item = _items[_head];
            _items[_head] = default;
            _head = (_head + 1) % slots;
            Interlocked.Decrement(ref _count);
            return true;
        }
    }
}
