using System.Text.Json.Serialization;

namespace ChatApp.TcpGateway.LoadGenerator.Diagnostics;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(TcpLoadReport))]
internal sealed partial class TcpLoadReportJsonContext : JsonSerializerContext;
