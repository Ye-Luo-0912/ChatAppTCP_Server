using System.Text.Json.Serialization;

namespace ChatApp.ResumeVerification.Diagnostics;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(ResumeVerificationReport))]
internal sealed partial class ResumeVerificationReportJsonContext : JsonSerializerContext;
