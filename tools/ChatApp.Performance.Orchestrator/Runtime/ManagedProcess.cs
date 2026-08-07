using System.Diagnostics;
using System.Text;
using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal sealed class ManagedProcess : IAsyncDisposable
{
    private readonly StreamWriter _stdoutWriter;
    private readonly StreamWriter _stderrWriter;
    private readonly Task _stdoutPump;
    private readonly Task _stderrPump;
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private bool _stopRequested;
    private bool _disposed;

    private ManagedProcess(
        string label,
        string kind,
        Process process,
        string stdoutPath,
        string stderrPath,
        StreamWriter stdoutWriter,
        StreamWriter stderrWriter)
    {
        Label = label;
        Kind = kind;
        Process = process;
        StandardOutputPath = stdoutPath;
        StandardErrorPath = stderrPath;
        _stdoutWriter = stdoutWriter;
        _stderrWriter = stderrWriter;
        _stdoutPump = PumpAsync(process.StandardOutput, stdoutWriter);
        _stderrPump = PumpAsync(process.StandardError, stderrWriter);
    }

    public string Label { get; }
    public string Kind { get; }
    public Process Process { get; }
    public string StandardOutputPath { get; }
    public string StandardErrorPath { get; }

    public bool HasExited
    {
        get
        {
            try
            {
                return Process.HasExited;
            }
            catch (InvalidOperationException)
            {
                return true;
            }
        }
    }

    public static ManagedProcess Start(
        string label,
        string kind,
        string fileName,
        IEnumerable<string> arguments,
        string workingDirectory,
        string logDirectory,
        IReadOnlyDictionary<string, string?>? environment = null)
    {
        Directory.CreateDirectory(logDirectory);
        var safeLabel = string.Concat(label.Select(character =>
            char.IsLetterOrDigit(character) || character is '-' or '_'
                ? character
                : '-'));
        var stdoutPath = Path.GetFullPath(Path.Combine(logDirectory, $"{safeLabel}.stdout.log"));
        var stderrPath = Path.GetFullPath(Path.Combine(logDirectory, $"{safeLabel}.stderr.log"));
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
        if (environment is not null)
        {
            foreach (var pair in environment)
                startInfo.Environment[pair.Key] = pair.Value;
        }

        var stdoutWriter = new StreamWriter(stdoutPath, append: false) { AutoFlush = true };
        var stderrWriter = new StreamWriter(stderrPath, append: false) { AutoFlush = true };
        try
        {
            var process = Process.Start(startInfo)
                ?? throw new InvalidOperationException($"Failed to start process {label}.");
            return new ManagedProcess(
                label,
                kind,
                process,
                stdoutPath,
                stderrPath,
                stdoutWriter,
                stderrWriter);
        }
        catch
        {
            stdoutWriter.Dispose();
            stderrWriter.Dispose();
            throw;
        }
    }

    public async Task<int> WaitForExitAsync(CancellationToken ct)
    {
        await Process.WaitForExitAsync(ct).ConfigureAwait(false);
        await Task.WhenAll(_stdoutPump, _stderrPump).ConfigureAwait(false);
        return Process.ExitCode;
    }

    public async Task StopAsync()
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed)
                return;

            if (!HasExited)
            {
                _stopRequested = true;
                try
                {
                    Process.Kill(entireProcessTree: true);
                }
                catch (InvalidOperationException)
                {
                }
                catch (System.ComponentModel.Win32Exception) when (HasExited)
                {
                }
            }

            try
            {
                await Process.WaitForExitAsync().ConfigureAwait(false);
                await Task.WhenAll(_stdoutPump, _stderrPump).ConfigureAwait(false);
            }
            catch (InvalidOperationException)
            {
            }
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public BenchmarkProcessResult CreateResult(
        long cgroupOomEvents = 0,
        long cgroupOomKillEvents = 0)
    {
        int? exitCode = null;
        if (HasExited)
        {
            try
            {
                exitCode = Process.ExitCode;
            }
            catch (InvalidOperationException)
            {
            }
        }

        var standardOutputTail = ReadTail(StandardOutputPath, 20);
        var standardErrorTail = ReadTail(StandardErrorPath, 20);
        // item 八：退出归因分类，避免将 exit 137 一律判成服务内存泄漏。
        var oomClassification = OomClassifier.Classify(
            exitCode,
            _stopRequested,
            standardOutputTail,
            standardErrorTail,
            cgroupOomKillEvents);
        var oomEvidence = OomClassifier.BuildEvidence(
            exitCode,
            _stopRequested,
            standardOutputTail,
            standardErrorTail,
            cgroupOomEvents,
            cgroupOomKillEvents);

        return new BenchmarkProcessResult
        {
            Label = Label,
            Kind = Kind,
            ProcessId = Process.Id,
            ExitCode = exitCode,
            StoppedByOrchestrator = _stopRequested,
            OomClassification = oomClassification,
            OomEvidence = oomEvidence,
            StandardOutputPath = StandardOutputPath,
            StandardErrorPath = StandardErrorPath,
            StandardOutputTail = standardOutputTail,
            StandardErrorTail = standardErrorTail
        };
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed)
                return;

            if (!HasExited)
            {
                _stopRequested = true;
                try
                {
                    Process.Kill(entireProcessTree: true);
                }
                catch (InvalidOperationException)
                {
                }
                catch (System.ComponentModel.Win32Exception) when (HasExited)
                {
                }
            }

            try
            {
                await Process.WaitForExitAsync().ConfigureAwait(false);
                await Task.WhenAll(_stdoutPump, _stderrPump).ConfigureAwait(false);
            }
            catch (InvalidOperationException)
            {
            }

            await _stdoutWriter.DisposeAsync().ConfigureAwait(false);
            await _stderrWriter.DisposeAsync().ConfigureAwait(false);
            Process.Dispose();
            _disposed = true;
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    private static async Task PumpAsync(StreamReader reader, StreamWriter writer)
    {
        var buffer = new char[4_096];
        while (true)
        {
            var read = await reader.ReadAsync(buffer).ConfigureAwait(false);
            if (read == 0)
                return;
            await writer.WriteAsync(buffer.AsMemory(0, read)).ConfigureAwait(false);
        }
    }

    private static string[] ReadTail(string path, int lineCount)
    {
        try
        {
            return File.ReadLines(path).TakeLast(lineCount).ToArray();
        }
        catch (IOException)
        {
            return [];
        }
    }
}
