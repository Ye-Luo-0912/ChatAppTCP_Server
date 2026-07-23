using System.Text.Json.Serialization;

namespace ChatApp.Realtime.PipelineLoadGenerator.Diagnostics;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(PipelineLoadReport))]
[JsonSerializable(typeof(PipelineLoadConfiguration))]
[JsonSerializable(typeof(PipelineLoadEnvironment))]
[JsonSerializable(typeof(LatencySnapshot))]
[JsonSerializable(typeof(Dictionary<string, LatencySnapshot>))]
internal sealed partial class PipelineReportJsonContext : JsonSerializerContext;
