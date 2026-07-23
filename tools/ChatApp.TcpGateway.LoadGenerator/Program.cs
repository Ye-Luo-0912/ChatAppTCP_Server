using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.LoadGenerator;
using ChatApp.TcpGateway.LoadGenerator.Diagnostics;

LoadOptions options;
try
{
    options = LoadOptions.Parse(args);
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception.Message);
    Console.Error.WriteLine(
        "Usage: --mode connection|heartbeat|chat|invalid-packet " +
        "--host 127.0.0.1 --port 8888 --connections 100 " +
        "--duration-seconds 10 [--token TOKEN] [--target-user-id ID] " +
        "[--messages-per-second 10] [--payload-bytes 128] " +
        "[--slow-readers 0] [--report-directory PATH]");
    return 2;
}

using var duration = new CancellationTokenSource(options.Duration);
var runState = new LoadRunState();
var clients = Enumerable.Range(0, options.Connections)
    .Select(index => RunClientAsync(
        options,
        index,
        runState,
        duration.Token))
    .ToArray();

var startedAt = Stopwatch.GetTimestamp();
var results = await Task.WhenAll(clients).ConfigureAwait(false);
var elapsed = Stopwatch.GetElapsedTime(startedAt);

var successfulConnections = results.Count(
    static result => result.Connected);
var failedConnections = results.Length - successfulConnections;
var sent = results.Sum(static result => result.Sent);
var received = results.Sum(static result => result.Received);
var acknowledged = results.Sum(static result => result.Acknowledged);
var rejected = results.Sum(static result => result.Rejected);
var latencies = results
    .SelectMany(static result => result.Latencies)
    .Order()
    .ToArray();

Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Mode: {options.Mode}"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Connections: {successfulConnections} succeeded, {failedConnections} failed"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Elapsed: {elapsed.TotalSeconds:F2}s"));

if (options.Mode == LoadMode.Heartbeat)
{
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Heartbeat round trips: {latencies.Length}"));
}
else if (options.Mode == LoadMode.Chat)
{
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Chat messages: {sent} sent, {acknowledged} MQ-accepted, " +
            $"{rejected} rejected, {received} deliveries received"));
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Throughput: {sent / elapsed.TotalSeconds:F0} sent/s, " +
            $"{received / elapsed.TotalSeconds:F0} delivered/s"));
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Slow readers: {options.SlowReaders}"));
}

if (latencies.Length != 0)
{
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Latency ms p50={Percentile(latencies, 0.50):F3}, " +
            $"p95={Percentile(latencies, 0.95):F3}, " +
            $"p99={Percentile(latencies, 0.99):F3}"));
    Console.WriteLine(
        string.Create(
            CultureInfo.InvariantCulture,
            $"Measured operations/sec: {latencies.Length / elapsed.TotalSeconds:F0}"));
}

var errorSamples = results
    .Where(static result => result.Error is not null)
    .Select(static result => result.Error!)
    .Distinct(StringComparer.Ordinal)
    .Take(5)
    .ToArray();
foreach (var error in errorSamples)
    Console.Error.WriteLine(error);

var report = TcpLoadReport.Create(
    options,
    elapsed,
    successfulConnections,
    failedConnections,
    sent,
    received,
    acknowledged,
    rejected,
    latencies,
    errorSamples);
var reportPaths = TcpLoadReportWriter.WriteFiles(
    report,
    options.ReportDirectory);
if (reportPaths is not null)
{
    Console.WriteLine($"JSON report: {reportPaths.JsonPath}");
    Console.WriteLine($"Markdown report: {reportPaths.MarkdownPath}");
}

return failedConnections == 0 ? 0 : 1;

static async Task<ClientResult> RunClientAsync(
    LoadOptions options,
    int clientIndex,
    LoadRunState runState,
    CancellationToken cancellationToken)
{
    var result = new ClientResult();
    var isSlowReader = options.Mode == LoadMode.Chat &&
                       clientIndex >= options.Connections - options.SlowReaders;

    try
    {
        await using var client = new ProtocolClient();
        await client.ConnectAsync(
                options.Host,
                options.Port,
                isSlowReader,
                cancellationToken)
            .ConfigureAwait(false);
        result.Connected = true;

        if (options.Mode == LoadMode.Connection)
        {
            await WaitForDurationAsync(cancellationToken)
                .ConfigureAwait(false);
            return result;
        }

        if (options.Mode == LoadMode.InvalidPacket)
        {
            await client.SendInvalidPacketAndWaitForCloseAsync(
                    cancellationToken)
                .ConfigureAwait(false);
            return result;
        }

        var token = options.AccessTokens[
            clientIndex % options.AccessTokens.Count];
        var identity = await client.AuthenticateAsync(
                token,
                AddDeviceOffset(options.DeviceIdHash, clientIndex),
                cancellationToken)
            .ConfigureAwait(false);

        if (options.Mode == LoadMode.Heartbeat)
        {
            await RunHeartbeatAsync(
                    client,
                    options.MessagesPerSecond,
                    result,
                    cancellationToken)
                .ConfigureAwait(false);
            return result;
        }

        if (isSlowReader)
        {
            await WaitForDurationAsync(cancellationToken)
                .ConfigureAwait(false);
            return result;
        }

        var targetUserId = options.TargetUserId ?? identity.UserId;
        var content = new string('x', options.PayloadBytes);
        var sendTask = RunChatSenderAsync(
            client,
            targetUserId,
            content,
            options.MessagesPerSecond,
            result,
            runState,
            cancellationToken);
        var receiveTask = RunChatReceiverAsync(
            client,
            result,
            runState,
            cancellationToken);

        await Task.WhenAll(sendTask, receiveTask)
            .ConfigureAwait(false);
        return result;
    }
    catch (OperationCanceledException)
        when (cancellationToken.IsCancellationRequested)
    {
        return result;
    }
    catch (Exception exception)
    {
        result.Error = exception.Message;
        result.Connected = false;
        return result;
    }
}

static async Task RunHeartbeatAsync(
    ProtocolClient client,
    int messagesPerSecond,
    ClientResult result,
    CancellationToken cancellationToken)
{
    using var timer = new PeriodicTimer(
        TimeSpan.FromSeconds(1d / messagesPerSecond));

    while (!cancellationToken.IsCancellationRequested)
    {
        var startedAt = Stopwatch.GetTimestamp();
        await client.SendHeartbeatAsync(cancellationToken)
            .ConfigureAwait(false);
        await client.ReceiveHeartbeatAsync(cancellationToken)
            .ConfigureAwait(false);
        result.Latencies.Add(
            Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds);

        if (!await timer.WaitForNextTickAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            break;
        }
    }
}

static async Task RunChatSenderAsync(
    ProtocolClient client,
    long targetUserId,
    string content,
    int messagesPerSecond,
    ClientResult result,
    LoadRunState runState,
    CancellationToken cancellationToken)
{
    using var timer = new PeriodicTimer(
        TimeSpan.FromSeconds(1d / messagesPerSecond));

    while (!cancellationToken.IsCancellationRequested)
    {
        var messageId = Guid.CreateVersion7().ToString("N");
        runState.StartedAt.TryAdd(
            messageId,
            Stopwatch.GetTimestamp());

        await client.SendChatMessageAsync(
                new ChatMessage
                {
                    MessageId = messageId,
                    TargetUserId = targetUserId,
                    Content = content
                },
                cancellationToken)
            .ConfigureAwait(false);
        result.Sent++;

        if (!await timer.WaitForNextTickAsync(cancellationToken)
                .ConfigureAwait(false))
        {
            break;
        }
    }
}

static async Task RunChatReceiverAsync(
    ProtocolClient client,
    ClientResult result,
    LoadRunState runState,
    CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        var inbound = await client
            .ReceiveChatInboundAsync(cancellationToken)
            .ConfigureAwait(false);

        if (inbound.Acknowledgement is not null)
        {
            if (inbound.Acknowledgement.Accepted)
            {
                result.Acknowledged++;
            }
            else
            {
                result.Rejected++;
            }

            continue;
        }

        var message = inbound.Message
            ?? throw new InvalidDataException(
                "Chat inbound frame contained no payload.");
        result.Received++;

        if (message.MessageId is not null &&
            runState.StartedAt.TryGetValue(
                message.MessageId,
                out var startedAt))
        {
            result.Latencies.Add(
                Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds);
        }
    }
}

static async Task WaitForDurationAsync(
    CancellationToken cancellationToken)
{
    await Task.Delay(
            Timeout.InfiniteTimeSpan,
            cancellationToken)
        .ConfigureAwait(false);
}

static ulong? AddDeviceOffset(
    ulong? deviceIdHash,
    int clientIndex)
{
    if (deviceIdHash is null)
    {
        return null;
    }

    return unchecked(deviceIdHash.Value + (ulong)clientIndex);
}

static double Percentile(
    double[] sortedValues,
    double percentile)
{
    var index = (int)Math.Ceiling(
        percentile * sortedValues.Length) - 1;
    return sortedValues[Math.Clamp(
        index,
        0,
        sortedValues.Length - 1)];
}

internal sealed class LoadRunState
{
    public ConcurrentDictionary<string, long> StartedAt { get; } = new();
}

internal sealed class ClientResult
{
    public bool Connected { get; set; }
    public long Sent { get; set; }
    public long Received { get; set; }
    public long Acknowledged { get; set; }
    public long Rejected { get; set; }
    public List<double> Latencies { get; } = [];
    public string? Error { get; set; }
}
