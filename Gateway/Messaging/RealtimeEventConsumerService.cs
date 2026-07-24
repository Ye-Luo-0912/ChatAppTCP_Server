using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Gateway.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging;

internal sealed partial class RealtimeEventConsumerService : BackgroundService
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
        LogMessageBusReady(_logger, latency.TotalMilliseconds);

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
                LogSubscriptionFailed(
                    _logger,
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
                LogDeliveryFailed(
                    _logger,
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
            LogNakFailed(
                _logger,
                delivery.Event.EventId,
                exception);
        }
    }

    [LoggerMessage(
        EventId = 30,
        Level = LogLevel.Information,
        Message = "Realtime message bus is ready; ping latency: {LatencyMilliseconds:F2} ms.")]
    private static partial void LogMessageBusReady(
        ILogger logger,
        double latencyMilliseconds);

    [LoggerMessage(
        EventId = 31,
        Level = LogLevel.Warning,
        Message = "Realtime event subscription failed; retrying after {RetryDelay}.")]
    private static partial void LogSubscriptionFailed(
        ILogger logger,
        TimeSpan retryDelay,
        Exception exception);

    [LoggerMessage(
        EventId = 32,
        Level = LogLevel.Error,
        Message = "Realtime event {EventId} delivery failed at attempt {DeliveryCount}; requesting redelivery.")]
    private static partial void LogDeliveryFailed(
        ILogger logger,
        string eventId,
        ulong? deliveryCount,
        Exception exception);

    [LoggerMessage(
        EventId = 33,
        Level = LogLevel.Error,
        Message = "Realtime event {EventId} NAK failed; JetStream AckWait will control redelivery.")]
    private static partial void LogNakFailed(
        ILogger logger,
        string eventId,
        Exception exception);
}
