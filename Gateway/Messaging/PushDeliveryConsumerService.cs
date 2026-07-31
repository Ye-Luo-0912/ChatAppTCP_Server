using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Messaging;

/// <summary>
/// 离线推送消费者：从 JetStream 拉取 <see cref="PushDeliveryCommand"/> 后调用本地 <see cref="IPushDispatcher"/>。
/// <para>
/// RealtimeServices 检测目标用户离线时发布推送命令到 NATS，本服务消费后执行实际推送
/// （令牌拉取 + Provider 调用 + 无效令牌注销）。
/// </para>
/// <para>
/// ACK 语义：DispatchAsync 成功（无论是否有令牌）后 ACK；异常时 NAK 延迟重投。
/// JetStream MaxDeliver 控制最终放弃，超出后消息进入 DLQ。
/// </para>
/// </summary>
internal sealed class PushDeliveryConsumerService : BackgroundService
{
    private static readonly TimeSpan DeliveryRetryDelay =
        TimeSpan.FromSeconds(1);

    private readonly IRealtimeMessageBus _messageBus;
    private readonly IPushDispatcher _pushDispatcher;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<PushDeliveryConsumerService> _logger;

    public PushDeliveryConsumerService(
        IRealtimeMessageBus messageBus,
        IPushDispatcher pushDispatcher,
        GatewayMetrics metrics,
        ILogger<PushDeliveryConsumerService> logger)
    {
        _messageBus = messageBus;
        _pushDispatcher = pushDispatcher;
        _metrics = metrics;
        _logger = logger;
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
                    "Push delivery subscription ended unexpectedly.");
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
                    RealtimeSubscriptionKind.PushDeliveries,
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
                           .ConsumePushDeliveriesAsync(stoppingToken)
                           .ConfigureAwait(false))
        {
            _metrics.PushDeliveryReceived();

            try
            {
                var result = await _pushDispatcher
                    .DispatchAsync(delivery.Command, stoppingToken)
                    .ConfigureAwait(false);

                _logger.PushDeliveryDispatched(
                    delivery.Command.TargetUserId,
                    result.AttemptedCount,
                    result.SucceededCount);

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
                _logger.PushDeliveryFailed(
                    delivery.Command.TargetUserId,
                    exception);
                _metrics.PushDeliveryFailed();
                await TryNakAsync(delivery, stoppingToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private static async Task TryNakAsync(
        PushDelivery delivery,
        CancellationToken ct)
    {
        try
        {
            await delivery.NakAsync(DeliveryRetryDelay, ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            // NAK 失败由 JetStream AckWait 兜底重投。
        }
    }
}
