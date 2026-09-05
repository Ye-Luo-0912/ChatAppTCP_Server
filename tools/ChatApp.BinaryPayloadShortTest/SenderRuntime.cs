using System.Buffers;
using System.Net.Sockets;
using System.Collections.Concurrent;
using System.Diagnostics;
using ChatApp.Binary.Core;
using ChatApp.Shared.Protocol.Tcp.Binary.Schemas;
using ChatApp.TcpGateway.Core.Protocol;
using LocalChatMessage = ChatApp.TcpGateway.Core.Messaging.ChatMessage;
using SharedChatMessage = ChatApp.Shared.Protocol.Tcp.ChatMessage;
using SharedMessageAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageAcknowledgement;

namespace ChatApp.BinaryPayloadShortTest;

/// <summary>phase 级共享时钟：全部延迟基于同一 <see cref="Stopwatch"/>。</summary>
internal static class PhaseClock
{
    public static readonly Stopwatch Instance = Stopwatch.StartNew();

    public static long GetTimestamp() => Stopwatch.GetTimestamp();

    public static double ToMilliseconds(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;
}

/// <summary>单个测量 phase 的参数。</summary>
internal sealed record PhaseSpec(int Id, int Rate, int Seconds, int Senders);

/// <summary>
/// 一条发送者连接的运行时： pacing 发送 ChatMessage（含 1 附件引用 + ~100B 正文）、
/// 持续读取 MessageAcknowledgement 并统计端到端 ack 延迟 / 漏投 / 重复。
/// </summary>
internal sealed class SenderRuntime : IAsyncDisposable
{
    /// <summary>消息正文：~100 B ASCII（正文 99 字符，两端两格式同一条）。</summary>
    public const string MessageBody = "body:" + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    /// <summary>追赶调度相邻两次检查之间的休眠（受系统计时器粒度合并，追赶逻辑保证速率）。</summary>
    private const int PacingSleepMilliseconds = 10;

    private readonly ProtocolClient _client;
    private readonly int _index;
    private readonly WireFormat _format;
    private readonly FixedBufferWriter _jsonWriter;
    private readonly byte[] _binaryBuffer;
    private readonly ConcurrentDictionary<string, long> _inflight = new();
    private readonly object _latencyGate = new();
    private readonly List<long> _latencyTicks = [];

    private long _nextSeq;
    private long _sentCount;
    private long _sentFrameBytes;
    private long _sentPayloadBytes;
    private long _ackedCount;
    private long _duplicateAcks;
    private long _unknownAcks;
    private long _rejectedAcks;
    private long _decodeErrors;
    private long _errorFrames;
    private Task? _readerTask;

    public SenderRuntime(ProtocolClient client, int index, WireFormat format)
    {
        _client = client;
        _index = index;
        _format = format;
        _jsonWriter = new FixedBufferWriter(new byte[16 * 1024]);
        _binaryBuffer = new byte[BinaryLimits.Default.MaxMessageBytes];
    }

    public long SentCount => Interlocked.Read(ref _sentCount);
    public long AckedCount => Interlocked.Read(ref _ackedCount);
    public long DuplicateAcks => Interlocked.Read(ref _duplicateAcks);
    public long UnknownAcks => Interlocked.Read(ref _unknownAcks);
    public long RejectedAcks => Interlocked.Read(ref _rejectedAcks);
    public long DecodeErrors => Interlocked.Read(ref _decodeErrors);
    public long ErrorFrames => Interlocked.Read(ref _errorFrames);
    public long SentFrameBytes => Interlocked.Read(ref _sentFrameBytes);
    public long SentPayloadBytes => Interlocked.Read(ref _sentPayloadBytes);
    public long InflightCount => _inflight.Count;

    public void StartReader(CancellationToken cancellationToken)
    {
        Guard.Ensure(_readerTask is null, "reader already started.");
        _readerTask = Task.Run(
            async () =>
            {
                try
                {
                    while (true)
                    {
                        var frame = await _client.ReadFrameAsync(cancellationToken);
                        switch (frame.Command)
                        {
                            case PacketCommand.MessageAcknowledgement:
                                HandleAcknowledgement(frame.Payload);
                                break;
                            case PacketCommand.Error:
                                Interlocked.Increment(ref _errorFrames);
                                break;
                            default:
                                // 预热/测量窗口内不预期其他帧；忽略并继续。
                                break;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // phase 结束的正常退出路径。
                }
                catch (Exception exception)
                    when (exception is IOException or SocketException or ObjectDisposedException)
                {
                    // 服务端停机 / 连接关闭：reader 终止。
                }
            },
            CancellationToken.None);
    }

    /// <summary>预热：小流量 JIT 预热并等待 ack 全部返回，然后清零全部计数（不进入测量数据）。</summary>
    public async Task RunWarmupAsync(int phaseId, int warmupMessages, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
        for (var i = 0; i < warmupMessages; i++)
        {
            if (i > 0 && !await timer.WaitForNextTickAsync(cancellationToken))
            {
                break;
            }

            await SendOneAsync(FormattableString.Invariant($"w{phaseId}-{_index}-{i}"), counted: false, cancellationToken);
        }

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!_inflight.IsEmpty && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10, cancellationToken);
        }

        Guard.Ensure(_inflight.IsEmpty, $"warmup acks incomplete: {_inflight.Count} still in flight.");
        await Task.Delay(50, CancellationToken.None);
        ResetMeasurements();
    }

    /// <summary>
    /// 测量发送循环：精确发送 <paramref name="perSenderRate"/> × <c>phase.Seconds</c> 条。
    /// 采用"墙钟追赶"调度：按已流逝时间计算目标累计发送数，不足部分补发（突发上限 1s 配额）。
    /// Windows 下 PeriodicTimer/Task.Delay 受系统计时器粒度（~15.6ms）钳制，
    /// 固定间隔 pacing 会把速率压到 ~64/s；追赶调度对粒度不敏感，平均速率精确。
    /// </summary>
    public async Task RunMeasuredPhaseAsync(PhaseSpec phase, int perSenderRate, CancellationToken cancellationToken)
    {
        var totalMessages = perSenderRate * phase.Seconds;
        var startTimestamp = PhaseClock.GetTimestamp();
        var sent = 0;
        while (sent < totalMessages)
        {
            var elapsedSeconds =
                PhaseClock.ToMilliseconds(PhaseClock.GetTimestamp() - startTimestamp) / 1000.0;
            var target = (int)Math.Min(totalMessages, Math.Ceiling(elapsedSeconds * perSenderRate));
            var burst = Math.Min(target - sent, perSenderRate);
            for (var i = 0; i < burst; i++)
            {
                var seq = Interlocked.Increment(ref _nextSeq);
                await SendOneAsync(
                    FormattableString.Invariant($"p{phase.Id}-s{_index}-{seq}"),
                    counted: true,
                    cancellationToken);
                sent++;
            }

            if (sent < totalMessages)
            {
                await Task.Delay(PacingSleepMilliseconds, cancellationToken);
            }
        }
    }

    public IReadOnlyList<long> SnapshotLatencyTicks()
    {
        lock (_latencyGate)
        {
            return [.. _latencyTicks];
        }
    }

    public async ValueTask DisposeAsync()
    {
        // 先关闭连接（解除阻塞中的 ReadAsync），再等待 reader 退出；可重入。
        await _client.DisposeAsync();
        if (_readerTask is { } task)
        {
            await task;
        }
    }

    private void ResetMeasurements()
    {
        _inflight.Clear();
        Interlocked.Exchange(ref _nextSeq, 0);
        Interlocked.Exchange(ref _sentCount, 0);
        Interlocked.Exchange(ref _sentFrameBytes, 0);
        Interlocked.Exchange(ref _sentPayloadBytes, 0);
        Interlocked.Exchange(ref _ackedCount, 0);
        Interlocked.Exchange(ref _duplicateAcks, 0);
        Interlocked.Exchange(ref _unknownAcks, 0);
        Interlocked.Exchange(ref _rejectedAcks, 0);
        Interlocked.Exchange(ref _decodeErrors, 0);
        Interlocked.Exchange(ref _errorFrames, 0);
        lock (_latencyGate)
        {
            _latencyTicks.Clear();
        }
    }

    private async Task SendOneAsync(string clientMessageId, bool counted, CancellationToken cancellationToken)
    {
        int payloadLength;
        long sentAt;
        if (_format == WireFormat.Json)
        {
            _jsonWriter.Reset();
            WireCodecs.ChatMessage.Serialize(_jsonWriter, BuildJsonMessage(clientMessageId));
            payloadLength = _jsonWriter.WrittenCount;
            sentAt = PhaseClock.GetTimestamp();
            _inflight[clientMessageId] = sentAt;
            await _client.WriteFrameAsync(
                PacketCommand.ChatMessage,
                _jsonWriter.Buffer.AsMemory(0, payloadLength),
                cancellationToken);
        }
        else
        {
            var encode = TcpBinaryWireEncoder.TryEncode(
                BuildBinaryMessage(clientMessageId),
                _binaryBuffer,
                BinaryLimits.Default);
            Guard.Ensure(
                encode.Status == TcpBinaryWireEncodeStatus.Encoded,
                $"binary chat encode failed: {encode.Status}");
            payloadLength = encode.Written;
            sentAt = PhaseClock.GetTimestamp();
            _inflight[clientMessageId] = sentAt;
            await _client.WriteFrameAsync(
                PacketCommand.ChatMessage,
                _binaryBuffer.AsMemory(0, payloadLength),
                cancellationToken);
        }

        if (counted)
        {
            Interlocked.Increment(ref _sentCount);
            Interlocked.Add(ref _sentFrameBytes, PacketProtocol.HeaderSize + payloadLength);
            Interlocked.Add(ref _sentPayloadBytes, payloadLength);
        }
    }

    private static LocalChatMessage BuildJsonMessage(string clientMessageId) => new()
    {
        ClientMessageId = clientMessageId,
        MessageId = clientMessageId,
        TargetUserId = ProtocolClient.ReceiverUserId,
        SenderUserId = ProtocolClient.SenderUserId,
        Content = MessageBody,
        SentUtc = DateTime.UtcNow,
        AttachmentIds = [$"att-{clientMessageId}"]
    };

    private static SharedChatMessage BuildBinaryMessage(string clientMessageId) => new()
    {
        ClientMessageId = clientMessageId,
        MessageId = clientMessageId,
        TargetUserId = ProtocolClient.ReceiverUserId,
        SenderUserId = ProtocolClient.SenderUserId,
        Content = MessageBody,
        SentAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        AttachmentIds = [$"att-{clientMessageId}"]
    };

    private void HandleAcknowledgement(byte[] payload)
    {
        string? clientMessageId;
        bool accepted;
        if (_format == WireFormat.Json)
        {
            var acknowledgement = WireCodecs.MessageAcknowledgement.Deserialize(new ReadOnlySequence<byte>(payload));
            if (acknowledgement is null)
            {
                Interlocked.Increment(ref _decodeErrors);
                return;
            }

            clientMessageId = acknowledgement.ClientMessageId;
            accepted = acknowledgement.Accepted;
        }
        else
        {
            var decode = TcpBinaryWireCodec.TryDecode(
                PacketCommand.MessageAcknowledgement,
                new ReadOnlySequence<byte>(payload),
                BinaryLimits.Default);
            if (decode.Status != TcpBinaryWireStatus.Decoded
                || decode.Value is not SharedMessageAcknowledgement shared)
            {
                Interlocked.Increment(ref _decodeErrors);
                return;
            }

            clientMessageId = shared.ClientMessageId;
            accepted = shared.Accepted;
        }

        if (!accepted)
        {
            Interlocked.Increment(ref _rejectedAcks);
        }

        if (clientMessageId is null)
        {
            Interlocked.Increment(ref _unknownAcks);
            return;
        }

        var now = PhaseClock.GetTimestamp();
        if (_inflight.TryRemove(clientMessageId, out var sentAt))
        {
            // 首次 ack：唯一计数 + 延迟样本。重复 ack 走 else 分支。
            lock (_latencyGate)
            {
                _latencyTicks.Add(now - sentAt);
            }

            Interlocked.Increment(ref _ackedCount);
        }
        else
        {
            Interlocked.Increment(ref _duplicateAcks);
        }
    }
}
