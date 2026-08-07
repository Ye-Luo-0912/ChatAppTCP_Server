using System.Globalization;
using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.Performance.Orchestrator.Runtime;

/// <summary>
/// item 八：把进程退出归因到具体 OOM 来源，而不是把 exit 137 一律判成
/// “Gateway 内存泄漏”。Linux 上 SIGKILL 的退出码是 128+9=137；判据优先级：
/// 托管 OOM 日志 &gt; cgroup oom_kill 计数 &gt; 无证据的 SIGKILLUnknown。
/// </summary>
internal static class OomClassifier
{
    private const int LinuxSigKillExitCode = 137;

    private static readonly string[] ManagedOomMarkers =
    [
        "OutOfMemoryException",
        "Out of memory",
        "insufficient memory",
        "Failed to allocate memory",
        "Unable to allocate memory",
        "System.OutOfMemoryException"
    ];

    public static OomClassification Classify(
        int? exitCode,
        bool stoppedByOrchestrator,
        IReadOnlyList<string> standardOutputTail,
        IReadOnlyList<string> standardErrorTail,
        long cgroupOomKillEvents)
    {
        if (exitCode is null || stoppedByOrchestrator)
            return OomClassification.None;

        if (LinuxProcessMetrics.IsLinux && exitCode == LinuxSigKillExitCode)
        {
            if (HasManagedOomEvidence(standardOutputTail, standardErrorTail))
                return OomClassification.ManagedOOM;

            if (cgroupOomKillEvents > 0)
                return OomClassification.KilledByCgroupOOM;

            return OomClassification.SIGKILLUnknown;
        }

        return OomClassification.None;
    }

    /// <summary>构造人工可读的归因证据片段，供报告人工复核。</summary>
    public static string? BuildEvidence(
        int? exitCode,
        bool stoppedByOrchestrator,
        IReadOnlyList<string> standardOutputTail,
        IReadOnlyList<string> standardErrorTail,
        long cgroupOomEvents,
        long cgroupOomKillEvents)
    {
        if (exitCode is null || stoppedByOrchestrator)
            return null;

        var markers = standardOutputTail
            .Concat(standardErrorTail)
            .Where(static line => ManagedOomMarkers.Any(marker =>
                line.Contains(marker, StringComparison.OrdinalIgnoreCase)))
            .Take(3)
            .Select(static line => line.Trim().Length > 200 ? line.Trim()[..200] : line.Trim())
            .ToArray();

        var parts = new List<string>();
        if (OperatingSystem.IsLinux())
        {
            parts.Add($"exit={exitCode.Value}");
            if (cgroupOomKillEvents > 0)
                parts.Add($"cgroup oom_kill={cgroupOomKillEvents.ToString(CultureInfo.InvariantCulture)}");
            if (cgroupOomEvents > 0)
                parts.Add($"cgroup oom={cgroupOomEvents.ToString(CultureInfo.InvariantCulture)}");
        }

        if (markers.Length != 0)
            parts.Add($"log markers: {string.Join(" | ", markers)}");

        return parts.Count == 0 ? null : string.Join("; ", parts);
    }

    private static bool HasManagedOomEvidence(
        IReadOnlyList<string> standardOutputTail,
        IReadOnlyList<string> standardErrorTail) =>
        standardOutputTail.Concat(standardErrorTail).Any(static line =>
            ManagedOomMarkers.Any(marker =>
                line.Contains(marker, StringComparison.OrdinalIgnoreCase)));
}