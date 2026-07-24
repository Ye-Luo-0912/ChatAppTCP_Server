namespace ChatApp.TcpGateway.Gateway.Diagnostics;

public sealed class ObservabilityOptions
{
    public const string SectionName = "Observability";

    public string ServiceName { get; init; } = "ChatApp.TcpGateway";
    public bool PrometheusEnabled { get; init; }
    public string PrometheusHost { get; init; } = "127.0.0.1";
    public int PrometheusPort { get; init; } = 9464;
    public bool OtlpEnabled { get; init; }
    public string OtlpEndpoint { get; init; } = "http://127.0.0.1:4317";
    public double TraceSampleRatio { get; init; } = 0.05;

    public bool IsValid() =>
        !string.IsNullOrWhiteSpace(ServiceName)
        && TraceSampleRatio is >= 0 and <= 1
        && (!PrometheusEnabled
            || (!string.IsNullOrWhiteSpace(PrometheusHost)
                && PrometheusPort is > 0 and <= 65_535))
        && (!OtlpEnabled
            || Uri.TryCreate(OtlpEndpoint, UriKind.Absolute, out _));
}
