using System.Globalization;
using System.Text;
using System.Text.Json;

namespace ChatApp.ResumeVerification.Diagnostics;

/// <summary>
/// 将 <see cref="ResumeVerificationReport"/> 写入 JSON + Markdown 文件。
/// </summary>
internal static class ResumeVerificationReportWriter
{
    /// <summary>写入报告文件，返回 JSON 与 Markdown 路径。</summary>
    public static ResumeVerificationReportPaths Write(
        ResumeVerificationReport report,
        string reportDirectory)
    {
        Directory.CreateDirectory(reportDirectory);
        var timestamp = report.CompletedAtUtc.ToString(
            "yyyyMMdd-HHmmss'Z'",
            CultureInfo.InvariantCulture);
        var jsonPath = Path.GetFullPath(
            Path.Combine(reportDirectory, $"resume-verification-{timestamp}.json"));
        var markdownPath = Path.GetFullPath(
            Path.Combine(reportDirectory, $"resume-verification-{timestamp}.md"));

        File.WriteAllText(
            jsonPath,
            JsonSerializer.Serialize(
                report,
                ResumeVerificationReportJsonContext.Default.ResumeVerificationReport));
        File.WriteAllText(markdownPath, CreateMarkdown(report));
        return new ResumeVerificationReportPaths(jsonPath, markdownPath);
    }

    private static string CreateMarkdown(ResumeVerificationReport report)
    {
        var text = new StringBuilder();
        text.AppendLine("# ChatApp Resume fault stress verification report");
        text.AppendLine();
        text.AppendLine(FormattableString.Invariant(
            $"Result: **{(report.AllPassed ? "PASSED" : "FAILED")}**"));
        text.AppendLine();
        text.AppendLine(FormattableString.Invariant(
            $"Window: {report.StartedAtUtc:O} - {report.CompletedAtUtc:O}"));
        text.AppendLine();
        text.AppendLine("## Configuration");
        text.AppendLine();
        text.AppendLine("| Item | Value |");
        text.AppendLine("|---|---:|");
        text.AppendLine(FormattableString.Invariant(
            $"| Gateways | {report.Configuration.GatewayEndpoints.Count} |"));
        foreach (var endpoint in report.Configuration.GatewayEndpoints)
        {
            text.AppendLine(FormattableString.Invariant($"| Endpoint | {endpoint} |"));
        }
        text.AppendLine(FormattableString.Invariant(
            $"| User count | {report.Configuration.UserCount} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Storm size | {report.Configuration.StormSize} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Redis down delay (s) | {report.Configuration.RedisDownDelaySeconds} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Redis recovery delay (s) | {report.Configuration.RedisRecoveryDelaySeconds} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Bootstrap user id start | {report.Configuration.BootstrapUserIdStart} |"));
        text.AppendLine(FormattableString.Invariant(
            $"| Warmup (s) | {report.Configuration.WarmupSeconds} |"));

        text.AppendLine();
        text.AppendLine("## Scenarios");
        text.AppendLine();
        text.AppendLine("| Scenario | Passed | Duration (s) | Summary |");
        text.AppendLine("|---|---|---:|---|");
        foreach (var scenario in report.Scenarios)
        {
            text.AppendLine(string.Format(
                CultureInfo.InvariantCulture,
                "| {0} | {1} | {2:F2} | {3} |",
                scenario.Name,
                scenario.Passed ? "yes" : "no",
                scenario.DurationSeconds,
                scenario.Summary));
        }

        foreach (var scenario in report.Scenarios)
        {
            text.AppendLine();
            text.AppendLine(FormattableString.Invariant($"### {scenario.Name}"));
            text.AppendLine();
            if (scenario.Metrics.Count != 0)
            {
                text.AppendLine("| Metric | Value | Sampled at (UTC) |");
                text.AppendLine("|---|---:|---|");
                foreach (var metric in scenario.Metrics)
                {
                    text.AppendLine(string.Format(
                        CultureInfo.InvariantCulture,
                        "| `{0}` | {1:G8} | {2:O} |",
                        metric.Name,
                        metric.Value,
                        metric.SampledAtUtc));
                }
            }

            if (scenario.Errors.Count != 0)
            {
                text.AppendLine();
                text.AppendLine("Errors:");
                foreach (var error in scenario.Errors)
                    text.Append("- ").AppendLine(error);
            }
        }

        return text.ToString();
    }
}

/// <summary>报告文件输出路径。</summary>
internal sealed record ResumeVerificationReportPaths(string JsonPath, string MarkdownPath);
