using System.Diagnostics;
using System.Text;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal static class CommandRunner
{
    public static async Task<CommandResult> RunAsync(
        string fileName,
        IEnumerable<string> arguments,
        string workingDirectory,
        CancellationToken ct)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = fileName,
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
            CreateNoWindow = true
        };
        foreach (var argument in arguments)
            startInfo.ArgumentList.Add(argument);

        using var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException($"Failed to start {fileName}.");
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct).ConfigureAwait(false);
        return new CommandResult(
            process.ExitCode,
            await stdoutTask.ConfigureAwait(false),
            await stderrTask.ConfigureAwait(false));
    }

    public static async Task EnsureSuccessAsync(
        string fileName,
        IEnumerable<string> arguments,
        string workingDirectory,
        CancellationToken ct)
    {
        var result = await RunAsync(fileName, arguments, workingDirectory, ct)
            .ConfigureAwait(false);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Command failed ({fileName}, exit {result.ExitCode}): " +
                result.StandardError.Trim());
        }
    }
}

internal sealed record CommandResult(
    int ExitCode,
    string StandardOutput,
    string StandardError);
