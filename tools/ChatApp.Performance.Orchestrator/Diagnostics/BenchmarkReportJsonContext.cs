using System.Text.Json.Serialization;

namespace ChatApp.Performance.Orchestrator.Diagnostics;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(BenchmarkReport))]
internal sealed partial class BenchmarkReportJsonContext : JsonSerializerContext;
