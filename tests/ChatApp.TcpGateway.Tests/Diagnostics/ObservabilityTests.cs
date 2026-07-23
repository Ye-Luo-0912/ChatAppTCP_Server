using System.Diagnostics;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Diagnostics;

namespace ChatApp.TcpGateway.Tests.Diagnostics;

public sealed class ObservabilityTests
{
    [Fact]
    public void OptionsRejectInvalidPortsAndSampleRatios()
    {
        Assert.True(new ObservabilityOptions().IsValid());
        Assert.False(new ObservabilityOptions
        {
            PrometheusEnabled = true,
            PrometheusPort = 0
        }.IsValid());
        Assert.False(new ObservabilityOptions
        {
            TraceSampleRatio = 1.01
        }.IsValid());
        Assert.False(new ObservabilityOptions
        {
            OtlpEnabled = true,
            OtlpEndpoint = "not-a-uri"
        }.IsValid());
    }

    [Fact]
    public void NatsHeadersRoundTripCurrentW3CContext()
    {
        using var activity = new Activity("test")
            .SetIdFormat(ActivityIdFormat.W3C)
            .Start();

        var headers = RealtimeIntegrationTelemetry.CreatePropagationHeaders();
        var restored = RealtimeIntegrationTelemetry.ExtractParentContext(headers);

        Assert.NotNull(headers);
        Assert.Equal(activity.TraceId, restored.TraceId);
        Assert.Equal(activity.SpanId, restored.SpanId);
        Assert.True(restored.IsRemote);
    }
}
