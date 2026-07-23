using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal sealed class ResourceSampler
{
    private readonly ConcurrentDictionary<string, ProcessAccumulator> _processes =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DockerAccumulator> _containers =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _errors =
        new(StringComparer.Ordinal);

    public IReadOnlyList<string> Errors => _errors.Keys.Order(StringComparer.Ordinal).ToArray();

    public void AddProcess(string label, Process process)
    {
        if (!_processes.TryAdd(label, new ProcessAccumulator(label, process)))
            throw new InvalidOperationException($"Process label is already registered: {label}");
    }

    public async Task RunAsync(
        TimeSpan interval,
        IReadOnlyList<string> dockerContainers,
        CancellationToken ct)
    {
        using var timer = new PeriodicTimer(interval);
        do
        {
            SampleProcesses();
            if (dockerContainers.Count != 0)
                await SampleDockerAsync(dockerContainers, ct).ConfigureAwait(false);
        }
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false));
    }

    public IReadOnlyList<ProcessResourceSummary> GetProcessSummaries() =>
        _processes.Values
            .OrderBy(static accumulator => accumulator.Label, StringComparer.Ordinal)
            .Select(static accumulator => accumulator.CreateSummary())
            .ToArray();

    public IReadOnlyList<DockerResourceSummary> GetDockerSummaries() =>
        _containers.Values
            .OrderBy(static accumulator => accumulator.Name, StringComparer.Ordinal)
            .Select(static accumulator => accumulator.CreateSummary())
            .ToArray();

    private void SampleProcesses()
    {
        foreach (var accumulator in _processes.Values)
        {
            try
            {
                accumulator.Sample();
            }
            catch (Exception exception)
                when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
            {
                _errors.TryAdd(
                    $"Process sample failed for {accumulator.Label}: {exception.Message}",
                    0);
            }
        }
    }

    private async Task SampleDockerAsync(
        IReadOnlyList<string> containers,
        CancellationToken ct)
    {
        try
        {
            var arguments = new List<string>
            {
                "stats",
                "--no-stream",
                "--format",
                "{{json .}}"
            };
            arguments.AddRange(containers);
            var result = await CommandRunner.RunAsync(
                    "docker",
                    arguments,
                    Environment.CurrentDirectory,
                    ct)
                .ConfigureAwait(false);
            if (result.ExitCode != 0)
            {
                _errors.TryAdd(
                    $"Docker stats failed: {result.StandardError.Trim()}",
                    0);
            }

            foreach (var line in result.StandardOutput.Split(
                         ['\r', '\n'],
                         StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                using var document = JsonDocument.Parse(line);
                var root = document.RootElement;
                var name = GetString(root, "Name");
                if (string.IsNullOrWhiteSpace(name))
                    continue;
                var cpu = ParsePercent(GetString(root, "CPUPerc"));
                var memory = ParseBytes(GetString(root, "MemUsage")?.Split('/')[0]);
                var accumulator = _containers.GetOrAdd(
                    name,
                    static container => new DockerAccumulator(container));
                accumulator.Sample(
                    cpu,
                    memory,
                    GetString(root, "NetIO"),
                    GetString(root, "BlockIO"));
            }
        }
        catch (Exception exception)
            when (exception is InvalidOperationException
                  or System.ComponentModel.Win32Exception
                  or JsonException)
        {
            _errors.TryAdd($"Docker stats sampling failed: {exception.Message}", 0);
        }
    }

    private static string? GetString(JsonElement element, string propertyName) =>
        element.TryGetProperty(propertyName, out var property)
            ? property.GetString()
            : null;

    private static double ParsePercent(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return 0;
        var normalized = value.Trim().TrimEnd('%');
        return double.TryParse(
            normalized,
            NumberStyles.Float,
            CultureInfo.InvariantCulture,
            out var parsed)
            ? parsed
            : 0;
    }

    private static long ParseBytes(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return 0;
        var normalized = value.Trim();
        var unitStart = 0;
        while (unitStart < normalized.Length &&
               (char.IsDigit(normalized[unitStart]) || normalized[unitStart] is '.' or ','))
        {
            unitStart++;
        }

        if (!double.TryParse(
                normalized[..unitStart].Replace(',', '.'),
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out var amount))
        {
            return 0;
        }

        var unit = normalized[unitStart..].Trim().ToUpperInvariant();
        var multiplier = unit switch
        {
            "B" => 1d,
            "KB" => 1_000d,
            "KIB" => 1_024d,
            "MB" => 1_000_000d,
            "MIB" => 1_048_576d,
            "GB" => 1_000_000_000d,
            "GIB" => 1_073_741_824d,
            _ => 1d
        };
        return (long)Math.Clamp(amount * multiplier, 0, long.MaxValue);
    }

    private sealed class ProcessAccumulator(string label, Process process)
    {
        private long _lastTimestamp;
        private TimeSpan _lastCpu;
        private int _samples;
        private int _cpuSamples;
        private double _cpuSum;
        private double _cpuMax;
        private long _workingSetFirst;
        private long _workingSetLast;
        private long _workingSetMin = long.MaxValue;
        private long _workingSetSum;
        private long _workingSetMax;
        private long _privateMemoryFirst;
        private long _privateMemoryLast;
        private long _privateMemoryMin = long.MaxValue;
        private long _privateMemorySum;
        private long _privateMemoryMax;
        private int _threadMax;
        private int _handleMax;
        private double _totalCpuSeconds;

        public string Label { get; } = label;

        public void Sample()
        {
            if (process.HasExited)
                return;

            process.Refresh();
            var now = Stopwatch.GetTimestamp();
            var cpu = process.TotalProcessorTime;
            if (_lastTimestamp != 0)
            {
                var elapsed = Stopwatch.GetElapsedTime(_lastTimestamp, now);
                if (elapsed > TimeSpan.Zero)
                {
                    var cpuPercent = Math.Max(
                        0,
                        (cpu - _lastCpu).TotalSeconds /
                        elapsed.TotalSeconds /
                        Environment.ProcessorCount * 100d);
                    _cpuSum += cpuPercent;
                    _cpuMax = Math.Max(_cpuMax, cpuPercent);
                    _cpuSamples++;
                }
            }

            _lastTimestamp = now;
            _lastCpu = cpu;
            _totalCpuSeconds = cpu.TotalSeconds;
            var workingSet = process.WorkingSet64;
            var privateMemory = process.PrivateMemorySize64;
            if (_samples == 0)
            {
                _workingSetFirst = workingSet;
                _privateMemoryFirst = privateMemory;
            }
            _workingSetLast = workingSet;
            _workingSetMin = Math.Min(_workingSetMin, workingSet);
            _workingSetSum += workingSet;
            _workingSetMax = Math.Max(_workingSetMax, workingSet);
            _privateMemoryLast = privateMemory;
            _privateMemoryMin = Math.Min(_privateMemoryMin, privateMemory);
            _privateMemorySum += privateMemory;
            _privateMemoryMax = Math.Max(_privateMemoryMax, privateMemory);
            _threadMax = Math.Max(_threadMax, process.Threads.Count);
            try
            {
                _handleMax = Math.Max(_handleMax, process.HandleCount);
            }
            catch (PlatformNotSupportedException)
            {
            }
            _samples++;
        }

        public ProcessResourceSummary CreateSummary() => new()
        {
            Label = Label,
            ProcessId = process.Id,
            Samples = _samples,
            AverageCpuPercent = _cpuSamples == 0 ? 0 : _cpuSum / _cpuSamples,
            MaximumCpuPercent = _cpuMax,
            TotalCpuSeconds = _totalCpuSeconds,
            FirstWorkingSetBytes = _workingSetFirst,
            LastWorkingSetBytes = _workingSetLast,
            MinimumWorkingSetBytes = _samples == 0 ? 0 : _workingSetMin,
            AverageWorkingSetBytes = _samples == 0 ? 0 : _workingSetSum / _samples,
            MaximumWorkingSetBytes = _workingSetMax,
            FirstPrivateMemoryBytes = _privateMemoryFirst,
            LastPrivateMemoryBytes = _privateMemoryLast,
            MinimumPrivateMemoryBytes = _samples == 0 ? 0 : _privateMemoryMin,
            AveragePrivateMemoryBytes = _samples == 0 ? 0 : _privateMemorySum / _samples,
            MaximumPrivateMemoryBytes = _privateMemoryMax,
            MaximumThreadCount = _threadMax,
            MaximumHandleCount = _handleMax
        };
    }

    private sealed class DockerAccumulator(string name)
    {
        private int _samples;
        private double _cpuSum;
        private double _cpuMax;
        private long _memoryFirst;
        private long _memoryLast;
        private long _memoryMin = long.MaxValue;
        private long _memorySum;
        private long _memoryMax;
        private string? _lastNetworkIo;
        private string? _lastBlockIo;

        public string Name { get; } = name;

        public void Sample(double cpu, long memory, string? networkIo, string? blockIo)
        {
            if (_samples == 0)
                _memoryFirst = memory;
            _samples++;
            _cpuSum += cpu;
            _cpuMax = Math.Max(_cpuMax, cpu);
            _memoryLast = memory;
            _memoryMin = Math.Min(_memoryMin, memory);
            _memorySum += memory;
            _memoryMax = Math.Max(_memoryMax, memory);
            _lastNetworkIo = networkIo;
            _lastBlockIo = blockIo;
        }

        public DockerResourceSummary CreateSummary() => new()
        {
            Container = Name,
            Samples = _samples,
            AverageCpuPercent = _samples == 0 ? 0 : _cpuSum / _samples,
            MaximumCpuPercent = _cpuMax,
            FirstMemoryBytes = _memoryFirst,
            LastMemoryBytes = _memoryLast,
            MinimumMemoryBytes = _samples == 0 ? 0 : _memoryMin,
            AverageMemoryBytes = _samples == 0 ? 0 : _memorySum / _samples,
            MaximumMemoryBytes = _memoryMax,
            LastNetworkIo = _lastNetworkIo,
            LastBlockIo = _lastBlockIo
        };
    }
}
