using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.PipelineLoadGenerator.Configuration;
using ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;
using ChatApp.Realtime.PipelineLoadGenerator.Runtime;
using Microsoft.Extensions.Logging.Abstractions;

PipelineLoadOptions options;
try
{
    options = PipelineLoadOptions.Parse(args);
}
catch (HelpRequestedException)
{
    Console.WriteLine(PipelineLoadOptions.Usage);
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine(ex.Message);
    Console.Error.WriteLine(PipelineLoadOptions.Usage);
    return 2;
}

using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.Cancel();
};

await using var messageBus = new NatsRealtimeMessageBus(
    new RealtimeIntegrationOptions
    {
        Url = options.NatsUrl,
        ClientName = "chatapp-realtime-pipeline-load",
        // Reuse one durable consumer per load-generator host. A random instance ID
        // would leave a new durable consumer behind after every benchmark run.
        InstanceId = $"pipeline-load-{Environment.MachineName}",
        GatewayConsumerPrefix = "chatapp-pipeline-load",
        ManageStreams = false,
        ReplayRetainedEventsOnConsumerCreation = true,
        HistoryRequestTimeoutMs = (int)Math.Min(
            int.MaxValue,
            options.OperationTimeout.TotalMilliseconds)
    },
    NullLogger<NatsRealtimeMessageBus>.Instance);
await using var eventRouter = new PipelineEventRouter(messageBus);

try
{
    var runner = new PipelineLoadRunner(messageBus, eventRouter, options);
    var report = await runner.RunAsync(shutdown.Token).ConfigureAwait(false);
    PipelineReportWriter.WriteConsole(report);
    var paths = PipelineReportWriter.WriteFiles(
        report,
        options.ReportDirectory);
    if (paths is not null)
    {
        Console.WriteLine($"JSON report: {paths.JsonPath}");
        Console.WriteLine($"Markdown report: {paths.MarkdownPath}");
    }

    return report.Failed == 0 ? 0 : 1;
}
catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
{
    Console.Error.WriteLine("Load run cancelled.");
    return 130;
}
catch (Exception ex)
{
    Console.Error.WriteLine(ex);
    return 1;
}
