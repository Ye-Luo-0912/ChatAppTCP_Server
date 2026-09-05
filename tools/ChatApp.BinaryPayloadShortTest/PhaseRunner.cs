using System.Diagnostics;
using System.Globalization;
using System.Text.Json;

namespace ChatApp.BinaryPayloadShortTest;

/// <summary>进程级资源快照（in-proc：客户端 + 网关合计，两轮同质可差分）。</summary>
internal sealed record MetricsSnapshot(
    long AllocatedBytes,
    long Gen0,
    long Gen1,
    long Gen2,
    TimeSpan GcTotalPause,
    TimeSpan CpuTime)
{
    public static MetricsSnapshot Take() => new(
        GC.GetTotalAllocatedBytes(precise: true),
        GC.CollectionCount(0),
        GC.CollectionCount(1),
        GC.CollectionCount(2),
        GC.GetTotalPauseDuration(),
        Process.GetCurrentProcess().TotalProcessorTime);
}

/// <summary>单个 phase 的汇总结果（同时作为 JSONL 报告行序列化）。</summary>
internal sealed record PhaseReport(
    string Label,
    string Format,
    int PhaseId,
    int Rate,
    int Seconds,
    int Senders,
    int IdleConnections,
    long Sent,
    long Acked,
    long Missing,
    long InflightRemaining,
    long DuplicateAcks,
    long UnknownAcks,
    long RejectedAcks,
    long DecodeErrors,
    long ErrorFrames,
    long GatewayIncomingMessages,
    long SentFrameBytes,
    long SentPayloadBytes,
    double AvgPayloadBytes,
    double AvgFrameBytes,
    long LatencySamples,
    double LatencyP50Ms,
    double LatencyP95Ms,
    double LatencyP99Ms,
    double LatencyMaxMs,
    long AllocBytesDelta,
    long Gen0Delta,
    long Gen1Delta,
    long Gen2Delta,
    double GcPauseTotalMs,
    double GcPauseDeltaMs,
    double CpuSecondsDelta,
    double WallSeconds);

/// <summary>执行单个 phase：连接 → 预热 → 快照 → 定速测量 → 排水 → 快照 → 汇总。</summary>
internal static class PhaseRunner
{
    public static async Task<PhaseReport> RunAsync(
        int port,
        HarnessOptions options,
        PhaseSpec phase,
        Func<long> gatewayIncomingCount)
    {
        using var phaseCts = new CancellationTokenSource(
            TimeSpan.FromSeconds(phase.Seconds + options.DrainSeconds + 120));

        var senders = new List<SenderRuntime>(phase.Senders);
        try
        {
            for (var i = 0; i < phase.Senders; i++)
            {
                var client = await ProtocolClient.ConnectAndAuthenticateAsync(
                    port,
                    options.Format,
                    deviceIdHash: (ulong)(100 + i),
                    phaseCts.Token);
                senders.Add(new SenderRuntime(client, i, options.Format));
            }

            // 读循环必须先于预热启动，预热 ack 才能被消费。
            foreach (var sender in senders)
            {
                sender.StartReader(phaseCts.Token);
            }

            foreach (var sender in senders)
            {
                await sender.RunWarmupAsync(phase.Id, options.WarmupPerSender, phaseCts.Token);
            }

            var incomingBefore = gatewayIncomingCount();
            var before = MetricsSnapshot.Take();
            var startTimestamp = PhaseClock.GetTimestamp();

            var perSenderRate = phase.Rate / phase.Senders;
            var monitor = StartMonitor(phase, senders);

            var sendTasks = senders
                .Select(sender => sender.RunMeasuredPhaseAsync(phase, perSenderRate, phaseCts.Token))
                .ToArray();
            await Task.WhenAll(sendTasks);

            // 排水：等待全部 ack 返回（零漏投判定窗口）。
            var drainDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(options.DrainSeconds);
            while (senders.Any(sender => sender.InflightCount > 0) && DateTime.UtcNow < drainDeadline)
            {
                await Task.Delay(20, CancellationToken.None);
            }

            var after = MetricsSnapshot.Take();
            var incomingDelta = gatewayIncomingCount() - incomingBefore;
            var wallSeconds = PhaseClock.ToMilliseconds(PhaseClock.GetTimestamp() - startTimestamp) / 1000.0;
            var report = BuildReport(options, phase, senders, before, after, incomingDelta, wallSeconds);

            await monitor.StopAsync();
            monitor.Dispose();
            return report;
        }
        finally
        {
            // 成功与失败路径统一收口：取消读循环 → 关闭连接（解除阻塞的 ReadAsync）。
            // using 的 phaseCts 在 finally 体执行后才释放，此处仍可取消。
            await phaseCts.CancelAsync();
            foreach (var sender in senders)
            {
                await sender.DisposeAsync();
            }
        }
    }

    public static string Summarize(PhaseReport report) =>
        $"phase={report.PhaseId} format={report.Format} rate={report.Rate}/s seconds={report.Seconds} " +
        $"sent={report.Sent} acked={report.Acked} missing={report.Missing} dup={report.DuplicateAcks} " +
        $"rejected={report.RejectedAcks} decodeErr={report.DecodeErrors} errFrames={report.ErrorFrames} " +
        $"incoming={report.GatewayIncomingMessages} " +
        $"payloadAvg={report.AvgPayloadBytes.ToString("F1", CultureInfo.InvariantCulture)}B " +
        $"frameAvg={report.AvgFrameBytes.ToString("F1", CultureInfo.InvariantCulture)}B " +
        $"frames={report.SentFrameBytes} " +
        $"p50={report.LatencyP50Ms.ToString("F2", CultureInfo.InvariantCulture)}ms " +
        $"p95={report.LatencyP95Ms.ToString("F2", CultureInfo.InvariantCulture)}ms " +
        $"p99={report.LatencyP99Ms.ToString("F2", CultureInfo.InvariantCulture)}ms " +
        $"max={report.LatencyMaxMs.ToString("F2", CultureInfo.InvariantCulture)}ms " +
        $"alloc={report.AllocBytesDelta}B gen0={report.Gen0Delta} gen1={report.Gen1Delta} gen2={report.Gen2Delta} " +
        $"gcPause={report.GcPauseDeltaMs.ToString("F0", CultureInfo.InvariantCulture)}ms " +
        $"cpu={report.CpuSecondsDelta.ToString("F2", CultureInfo.InvariantCulture)}s " +
        $"wall={report.WallSeconds.ToString("F1", CultureInfo.InvariantCulture)}s";

    private static MonitorLoop StartMonitor(PhaseSpec phase, IReadOnlyList<SenderRuntime> senders)
    {
        var monitor = new MonitorLoop(phase, senders);
        monitor.Start();
        return monitor;
    }

    private sealed class MonitorLoop(PhaseSpec phase, IReadOnlyList<SenderRuntime> senders) : IDisposable
    {
        private readonly CancellationTokenSource _cancellation = new();
        private Task _loop = Task.CompletedTask;

        public void Start()
        {
            _loop = Task.Run(async () =>
            {
                try
                {
                    while (!_cancellation.Token.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), _cancellation.Token);
                        var sent = senders.Sum(static sender => sender.SentCount);
                        var acked = senders.Sum(static sender => sender.AckedCount);
                        var inflight = senders.Sum(static sender => sender.InflightCount);
                        Console.WriteLine(
                            $"  [phase {phase.Id}] sent={sent} acked={acked} inflight={inflight}");
                    }
                }
                catch (OperationCanceledException)
                {
                    // 正常停止。
                }
            }, CancellationToken.None);
        }

        public async Task StopAsync()
        {
            _cancellation.Cancel();
            await _loop;
        }

        public void Dispose() => _cancellation.Dispose();
    }

    private static PhaseReport BuildReport(
        HarnessOptions options,
        PhaseSpec phase,
        IReadOnlyList<SenderRuntime> senders,
        MetricsSnapshot before,
        MetricsSnapshot after,
        long gatewayIncomingMessages,
        double wallSeconds)
    {
        var sent = senders.Sum(static sender => sender.SentCount);
        var acked = senders.Sum(static sender => sender.AckedCount);
        var inflightRemaining = senders.Sum(static sender => sender.InflightCount);

        var latency = senders
            .SelectMany(static sender => sender.SnapshotLatencyTicks())
            .OrderBy(static tick => tick)
            .ToArray();

        return new PhaseReport(
            Label: options.Label,
            Format: options.Format == WireFormat.Binary ? "binary" : "json",
            PhaseId: phase.Id,
            Rate: phase.Rate,
            Seconds: phase.Seconds,
            Senders: phase.Senders,
            IdleConnections: options.IdleConnections,
            Sent: sent,
            Acked: acked,
            Missing: sent - acked,
            InflightRemaining: inflightRemaining,
            DuplicateAcks: senders.Sum(static sender => sender.DuplicateAcks),
            UnknownAcks: senders.Sum(static sender => sender.UnknownAcks),
            RejectedAcks: senders.Sum(static sender => sender.RejectedAcks),
            DecodeErrors: senders.Sum(static sender => sender.DecodeErrors),
            ErrorFrames: senders.Sum(static sender => sender.ErrorFrames),
            GatewayIncomingMessages: gatewayIncomingMessages,
            SentFrameBytes: senders.Sum(static sender => sender.SentFrameBytes),
            SentPayloadBytes: senders.Sum(static sender => sender.SentPayloadBytes),
            AvgPayloadBytes: sent > 0 ? (double)senders.Sum(static s => s.SentPayloadBytes) / sent : 0,
            AvgFrameBytes: sent > 0 ? (double)senders.Sum(static s => s.SentFrameBytes) / sent : 0,
            LatencySamples: latency.Length,
            LatencyP50Ms: Percentile(latency, 50),
            LatencyP95Ms: Percentile(latency, 95),
            LatencyP99Ms: Percentile(latency, 99),
            LatencyMaxMs: latency.Length > 0 ? PhaseClock.ToMilliseconds(latency[^1]) : 0,
            AllocBytesDelta: after.AllocatedBytes - before.AllocatedBytes,
            Gen0Delta: after.Gen0 - before.Gen0,
            Gen1Delta: after.Gen1 - before.Gen1,
            Gen2Delta: after.Gen2 - before.Gen2,
            GcPauseTotalMs: after.GcTotalPause.TotalMilliseconds,
            GcPauseDeltaMs: (after.GcTotalPause - before.GcTotalPause).TotalMilliseconds,
            CpuSecondsDelta: (after.CpuTime - before.CpuTime).TotalSeconds,
            WallSeconds: wallSeconds);
    }

    /// <summary>最近邻秩百分位（升序样本，毫秒）。</summary>
    private static double Percentile(long[] sortedTicks, double percentile)
    {
        if (sortedTicks.Length == 0)
        {
            return 0;
        }

        var rank = (int)Math.Ceiling(percentile / 100.0 * sortedTicks.Length);
        if (rank < 1)
        {
            rank = 1;
        }

        return PhaseClock.ToMilliseconds(sortedTicks[rank - 1]);
    }
}

/// <summary>把 phase 汇总以 JSONL 追加到结果文件。</summary>
internal static class ReportWriter
{
    public static void Append(string path, PhaseReport report)
    {
        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var line = JsonSerializer.Serialize(report);
        File.AppendAllText(path, line + Environment.NewLine);
    }
}
