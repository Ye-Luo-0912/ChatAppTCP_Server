using ChatApp.Performance.Orchestrator.Configuration;
using ChatApp.Performance.Orchestrator.Diagnostics;
using ChatApp.Performance.Orchestrator.Runtime;

BenchmarkOptions options;
try
{
    options = BenchmarkOptions.Parse(args);
}
catch (HelpRequestedException)
{
    Console.WriteLine(BenchmarkOptions.Usage);
    return 0;
}
catch (Exception exception)
{
    Console.Error.WriteLine(exception.Message);
    Console.Error.WriteLine(BenchmarkOptions.Usage);
    return 2;
}

using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.Cancel();
};

var result = await new BenchmarkRunner(options)
    .RunAsync(shutdown.Token)
    .ConfigureAwait(false);
var paths = BenchmarkReportWriter.Write(
    result.Report,
    result.SessionDirectory);

Console.WriteLine($"Result: {(result.Report.Succeeded ? "PASSED" : "FAILED")}");
Console.WriteLine($"JSON report: {paths.JsonPath}");
Console.WriteLine($"Markdown report: {paths.MarkdownPath}");
foreach (var error in result.Report.Errors)
    Console.Error.WriteLine(error);

return result.Report.Succeeded ? 0 : 1;
