using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using ChatApp.TcpGateway.Observability.Tracing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging;

internal sealed class RealtimeEventConsumerService : BackgroundService
{
    private static readonly TimeSpan DeliveryRetryDelay =
        TimeSpan.FromSeconds(1);

    private readonly IRealtimeMessageBus _messageBus;
    private readonly RealtimeEventDispatcher _dispatcher;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<RealtimeEventConsumerService> _logger;

    public RealtimeEventConsumerService(
        IRealtimeMessageBus messageBus,
        RealtimeEventDispatcher dispatcher,
        GatewayMetrics metrics,
        ILogger<RealtimeEventConsumerService> logger)
    {
        _messageBus = messageBus;
        _dispatcher = dispatcher;
        _metrics = metrics;
        _logger = logger;
    }

    public override async Task StartAsync(
        CancellationToken cancellationToken)
    {
        var latency = await _messageBus
            .PingAsync(cancellationToken)
            .ConfigureAwait(false);
        _logger.RealtimeBusReady(latency.TotalMilliseconds);

        await base.StartAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var retryAttempt = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeAsync(stoppingToken)
                    .ConfigureAwait(false);
                throw new InvalidOperationException(
                    "Realtime event subscription ended unexpectedly.");
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                retryAttempt++;
                var retryDelay = TimeSpan.FromSeconds(
                    Math.Min(30, 1 << Math.Min(retryAttempt - 1, 5)));
                _logger.RealtimeSubscriptionFailed(
                    RealtimeSubscriptionKind.DurableEvents,
                    retryDelay,
                    exception);
                await Task.Delay(retryDelay, stoppingToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private async Task ConsumeAsync(CancellationToken stoppingToken)
    {
        await foreach (var delivery in _messageBus
                           .ConsumeEventsAsync(stoppingToken)
                           .ConfigureAwait(false))
        {
            using var activity = GatewayTelemetry.StartEventConsumer(
                delivery.ParentContext);
            activity?.SetTag(
                "chat.event.type",
                delivery.Event.Type.ToString());
            _metrics.RealtimeEventReceived();

            try
            {
                _dispatcher.Dispatch(delivery.Event);
                await delivery.AckAsync(stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                GatewayTelemetry.RecordException(activity, exception);
                _logger.RealtimeDeliveryFailed(
                    delivery.Event.EventId,
                    delivery.DeliveryCount,
                    exception);
                await TryNakAsync(delivery, stoppingToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private async Task TryNakAsync(
        RealtimeEventDelivery delivery,
        CancellationToken cancellationToken)
    {
        try
        {
            await delivery.NakAsync(
                    DeliveryRetryDelay,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!cancellationToken.IsCancellationRequested)
        {
            _logger.RealtimeNakFailed(
                delivery.Event.EventId,
                exception);
        }
    }
}
