using System.Globalization;

namespace ChatApp.Performance.Orchestrator.Runtime;

/// <summary>
/// Best-effort capturer of Linux process memory and cgroup-v2 memory pressure
/// signals. Used to attribute OOM kills without assuming the process that died
/// was the one that leaked (item 八): kernel <c>/proc/&lt;pid&gt;/status</c>
/// gives the real committed RSS high-water mark, while cgroup
/// <c>memory.events</c> gives authoritative oom/oom_kill counters.
/// </summary>
internal static class LinuxProcessMetrics
{
    public static bool IsLinux => OperatingSystem.IsLinux();

    /// <summary>
    /// Reads a single sample of Linux process memory and, when the process
    /// belongs to a v2 memory cgroup, the cgroup current/peak usage and event
    /// counters. Returns <c>null</c> when unavailable or unreadable so sampling
    /// degrades gracefully on non-Linux hosts.
    /// </summary>
    public static LinuxProcessSample? SampleBestEffort(int processId)
    {
        if (!IsLinux)
            return null;

        try
        {
            var statusPath = $"/proc/{processId}/status";
            if (!File.Exists(statusPath))
                return null;

            long? vmRssKb = null;
            long? vmHwmKb = null;
            foreach (var line in File.ReadLines(statusPath))
            {
                if (vmRssKb is not null && vmHwmKb is not null)
                    break;
                if (line.StartsWith("VmRSS:", StringComparison.Ordinal))
                {
                    vmRssKb = ParseKb(line);
                }
                else if (line.StartsWith("VmHWM:", StringComparison.Ordinal))
                {
                    vmHwmKb = ParseKb(line);
                }
            }

            long? pssKb = null;
            var smapsRollupPath = $"/proc/{processId}/smaps_rollup";
            if (File.Exists(smapsRollupPath))
            {
                foreach (var line in File.ReadLines(smapsRollupPath))
                {
                    if (line.StartsWith("Pss:", StringComparison.Ordinal))
                    {
                        pssKb = ParseKb(line);
                        break;
                    }
                }
            }

            var cgroup = ReadCgroupMemory(processId);
            return new LinuxProcessSample(
                VmRssBytes: vmRssKb is null ? null : vmRssKb * 1_024,
                VmHwmBytes: vmHwmKb is null ? null : vmHwmKb * 1_024,
                PssBytes: pssKb is null ? null : pssKb * 1_024,
                FileDescriptorCount: ReadFileDescriptorCount(processId),
                CgroupMemoryCurrentBytes: cgroup?.MemoryCurrentBytes,
                CgroupMemoryPeakBytes: cgroup?.MemoryPeakBytes,
                CgroupOomEvents: cgroup?.OomEvents,
                CgroupOomKillEvents: cgroup?.OomKillEvents);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static long? ParseKb(string statusLine)
    {
        var separator = statusLine.IndexOf(':');
        if (separator < 0)
            return null;
        var valueText = statusLine[(separator + 1)..].Trim();
        var space = valueText.IndexOf(' ');
        if (space < 0)
            return null;
        return long.TryParse(
            valueText[..space],
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out var kb)
            ? kb
            : null;
    }

    private static CgroupMemorySnapshot? ReadCgroupMemory(int processId)
    {
        string? memoryPath = null;
        try
        {
            var cgroupFilePath = $"/proc/{processId}/cgroup";
            if (!File.Exists(cgroupFilePath))
                return null;

            foreach (var line in File.ReadLines(cgroupFilePath))
            {
                // v2 line format: "0::/path/to/cgroup"
                var separator = line.IndexOf("::", StringComparison.Ordinal);
                if (separator < 0)
                    continue;
                var path = line[(separator + 2)..].Trim();
                if (path.Length == 0)
                    continue;
                memoryPath = $"/sys/fs/cgroup{path}";
                break;
            }

            if (memoryPath is null)
                return null;

            var current = ReadUlong(Path.Combine(memoryPath, "memory.current"));
            var peak = ReadUlong(Path.Combine(memoryPath, "memory.peak"));
            long? oom = null;
            long? oomKill = null;

            var eventsPath = Path.Combine(memoryPath, "memory.events");
            if (File.Exists(eventsPath))
            {
                foreach (var line in File.ReadLines(eventsPath))
                {
                    if (line.StartsWith("oom ", StringComparison.Ordinal))
                    {
                        oom = ParseCounter(line);
                    }
                    else if (line.StartsWith("oom_kill ", StringComparison.Ordinal))
                    {
                        oomKill = ParseCounter(line);
                    }
                }
            }

            if (current is null && peak is null && oom is null && oomKill is null)
                return null;

            return new CgroupMemorySnapshot(current, peak, oom, oomKill);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static long? ReadUlong(string path)
    {
        if (!File.Exists(path))
            return null;
        return long.TryParse(
            File.ReadAllText(path).Trim(),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out var value)
            ? value
            : null;
    }

    private static long? ParseCounter(string line)
    {
        var space = line.IndexOf(' ');
        if (space < 0)
            return null;
        return long.TryParse(
            line[(space + 1)..].Trim(),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out var value)
            ? value
            : null;
    }

    private sealed record CgroupMemorySnapshot(
        long? MemoryCurrentBytes,
        long? MemoryPeakBytes,
        long? OomEvents,
        long? OomKillEvents);

    /// <summary>
    /// TCP-MEM-1：统计进程打开的文件描述符数量。Linux 上 socket 与
    /// epoll/eventfd/pipe 都占 fd，是区分内核 socket 归属与 managed 对象
    /// retained 的直接证据之一。枚举 /proc/&lt;pid&gt;/fd 目录条目计数。
    /// </summary>
    private static int? ReadFileDescriptorCount(int processId)
    {
        var fdPath = $"/proc/{processId}/fd";
        if (!Directory.Exists(fdPath))
            return null;
        try
        {
            return Directory.EnumerateFileSystemEntries(fdPath)
                .Count();
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }
}

/// <summary>
/// TCP-MEM-1：单次 Linux 进程内存归因样本。PssBytes 来自 smaps_rollup，
/// FileDescriptorCount 来自 /proc/&lt;pid&gt;/fd；配合 cgroup 信号一起把
/// managed retained、GC committed、native cache 与内核 socket 区分开。
/// </summary>
internal sealed record LinuxProcessSample(
    long? VmRssBytes,
    long? VmHwmBytes,
    long? PssBytes,
    int? FileDescriptorCount,
    long? CgroupMemoryCurrentBytes,
    long? CgroupMemoryPeakBytes,
    long? CgroupOomEvents,
    long? CgroupOomKillEvents);