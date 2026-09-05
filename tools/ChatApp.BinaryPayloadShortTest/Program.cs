using System.Globalization;
using System.Net;
using System.Net.Sockets;

namespace ChatApp.BinaryPayloadShortTest;

/// <summary>
/// BIN-INTEGRATION-3 收益与稳定性短测 harness 入口：
/// in-proc 组装真 TcpGatewayService（回环监听），同一拓扑分别以 JSON / binary
/// 完成 80/320/640 msg/s 的 5–20 分钟短测，phase 汇总逐行追加到 JSONL 结果文件。
/// <para>
/// 局限：CPU / alloc / GC 为 harness 客户端 + 网关进程合计（in-proc），
/// 两轮 harness 形态相同，差值可归因于 codec；跨进程归因需后续 soak。
/// </para>
/// </summary>
internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        HarnessOptions options;
        try
        {
            options = HarnessOptions.Parse(args);
        }
        catch (ArgumentException exception)
        {
            Console.Error.WriteLine($"argument error: {exception.Message}");
            Console.Error.WriteLine(HarnessOptions.Usage);
            return 2;
        }

        Console.WriteLine($"=== ChatApp.BinaryPayloadShortTest round start: {options.Describe()} ===");
        try
        {
            await RunRoundAsync(options);
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine("=== ROUND FAILED ===");
            Console.Error.WriteLine(exception);
            return 1;
        }

        Console.WriteLine("=== ROUND OK ===");
        return 0;
    }

    private static async Task RunRoundAsync(HarnessOptions options)
    {
        var port = ReserveLoopbackPort();
        using var node = GatewayNode.Create(port, options.EnableBinary);
        await node.Service.StartAsync(CancellationToken.None);
        try
        {
            // 拓扑：N 条纯保活连接（deviceIdHash 1..N）+ 每 phase 独立的发送者连接。
            var idleClients = new List<ProtocolClient>(options.IdleConnections);
            for (var i = 0; i < options.IdleConnections; i++)
            {
                idleClients.Add(await ProtocolClient.ConnectAndAuthenticateAsync(
                    port,
                    options.Format,
                    deviceIdHash: (ulong)(i + 1),
                    CancellationToken.None));
            }

            Console.WriteLine(
                $"[round] gateway listening on {port}; {idleClients.Count} idle sessions authenticated " +
                $"({options.Format} negotiation)");

            for (var index = 0; index < options.Rates.Count; index++)
            {
                var phase = new PhaseSpec(
                    Id: index + 1,
                    Rate: options.Rates[index],
                    Seconds: options.SecondsPerRate[index],
                    Senders: options.SendersPerRate[index]);
                Console.WriteLine(
                    $"[round] phase {phase.Id}: {phase.Rate}/s x {phase.Seconds}s, " +
                    $"{phase.Senders} sender(s), per-sender {phase.Rate / phase.Senders}/s");
                var report = await PhaseRunner.RunAsync(
                    port,
                    options,
                    phase,
                    () => node.Bus.PublishedIncomingCount);
                ReportWriter.Append(options.OutputPath, report);
                Console.WriteLine($"[phase {phase.Id}] {Summarize(report)}");
            }

            foreach (var idle in idleClients)
            {
                await idle.DisposeAsync();
            }
        }
        finally
        {
            await node.Service.StopAsync(CancellationToken.None);
        }
    }

    private static string Summarize(PhaseReport report) =>
        $"sent={report.Sent} acked={report.Acked} missing={report.Missing} dup={report.DuplicateAcks} " +
        $"payloadAvg={report.AvgPayloadBytes.ToString("F1", CultureInfo.InvariantCulture)}B " +
        $"frameAvg={report.AvgFrameBytes.ToString("F1", CultureInfo.InvariantCulture)}B " +
        $"p50={report.LatencyP50Ms.ToString("F2", CultureInfo.InvariantCulture)}ms " +
        $"p95={report.LatencyP95Ms.ToString("F2", CultureInfo.InvariantCulture)}ms " +
        $"p99={report.LatencyP99Ms.ToString("F2", CultureInfo.InvariantCulture)}ms " +
        $"alloc={report.AllocBytesDelta}B cpu={report.CpuSecondsDelta.ToString("F2", CultureInfo.InvariantCulture)}s";

    private static int ReserveLoopbackPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }
}
