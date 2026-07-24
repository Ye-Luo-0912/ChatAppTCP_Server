using ChatApp.Realtime.Integration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace ChatApp.TcpGateway.Gateway.Diagnostics;

public static class GatewayObservabilityRegistration
{
    public static ObservabilityOptions AddGatewayObservability(
        this IServiceCollection services,
        IConfiguration configuration,
        string instanceId)
    {
        var options = configuration
            .GetSection(ObservabilityOptions.SectionName)
            .Get<ObservabilityOptions>()
            ?? new ObservabilityOptions();
        if (!options.IsValid())
            throw new InvalidOperationException("Observability configuration is invalid.");

        services.AddSingleton(Options.Create(options));
        services
            .AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(
                options.ServiceName,
                serviceInstanceId: instanceId))
            .WithMetrics(metrics =>
            {
                metrics.AddMeter(
                    GatewayMetrics.MeterName,
                    RealtimeIntegrationTelemetry.ActivitySourceName,
                    "System.Runtime");
                if (options.PrometheusEnabled)
                {
                    metrics.AddPrometheusHttpListener(prometheus =>
                    {
                        prometheus.Host = options.PrometheusHost;
                        prometheus.Port = options.PrometheusPort;
                    });
                }

                if (options.OtlpEnabled)
                {
                    metrics.AddOtlpExporter(exporter =>
                        exporter.Endpoint = new Uri(options.OtlpEndpoint));
                }
            })
            .WithTracing(tracing =>
            {
                tracing
                    .AddSource(
                        GatewayTelemetry.ActivitySourceName,
                        RealtimeIntegrationTelemetry.ActivitySourceName)
                    .SetSampler(new ParentBasedSampler(
                        new TraceIdRatioBasedSampler(options.TraceSampleRatio)));
                if (options.OtlpEnabled)
                {
                    tracing.AddOtlpExporter(exporter =>
                        exporter.Endpoint = new Uri(options.OtlpEndpoint));
                }
            });

        return options;
    }
}
