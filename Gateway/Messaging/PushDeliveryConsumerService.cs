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
/// P0-2 ACK 语义（基于 <see cref="PushDispatchDisposition"/>）：
/// <list type="bullet">
/// <item><see cref="PushDispatchDisposition.NoTargets"/> / <see cref="PushDispatchDisposition.FullySucceeded"/> /
///   <see cref="PushDispatchDisposition.PermanentlyCompleted"/> → ACK（重投不会改变结果）。</item>
/// <item><see cref="PushDispatchDisposition.Retryable"/> → NAK 延迟重投
///   （FCM 503 / APNs 429 / Provider 超时等可重试失败必须重投，避免永久丢失）。</item>
/// </list>
/// 异常时 NAK 延迟重投。JetStream MaxDeliver 控制最终放弃，超出后消息进入 DLQ。
/// </para>
/// <para>
/// <b>已知权衡</b>：Retryable 场景下 NAK 整条命令会导致已成功 token 在重投后重复收到推送。
/// 长期应拆分为每 token 独立工作项以支持 PartiallyRetryable（仅重试失败 token）。
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

                // P0-2：基于 Disposition 决定 ACK / NAK，而非无条件 ACK。
                // 可重试失败（provider_unavailable / rate_limited）必须 NAK 重投，
                // 否则 FCM 503 / APNs 429 / Provider 超时等场景会永久丢失推送。
                var disposition = ClassifyDisposition(result);
                if (disposition == PushDispatchDisposition.Retryable)
                {
                    await delivery
                        .NakAsync(DeliveryRetryDelay, stoppingToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    await delivery.AckAsync(stoppingToken)
                        .ConfigureAwait(false);
                }
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

    /// <summary>
    /// P0-2：基于 <see cref="PushDeliveryResult"/> 现有字段推导处置分类。
    /// <para>
    /// 不修改 RealtimeServices 共享契约——处置是 Consumer 侧决策，使用本地字段即可推导：
    /// <list type="bullet">
    /// <item>AttemptedCount=0 → <see cref="PushDispatchDisposition.NoTargets"/></item>
    /// <item>SucceededCount=AttemptedCount → <see cref="PushDispatchDisposition.FullySucceeded"/></item>
    /// <item>RetryableFailureCount=0 → <see cref="PushDispatchDisposition.PermanentlyCompleted"/>
    ///   （仅有 invalid_token / payload_too_large 等永久失败，重投无意义）</item>
    /// <item>RetryableFailureCount&gt;0 → <see cref="PushDispatchDisposition.Retryable"/></item>
    /// </list>
    /// </para>
    /// </summary>
    private static PushDispatchDisposition ClassifyDisposition(
        PushDeliveryResult result)
    {
        if (result.AttemptedCount == 0)
            return PushDispatchDisposition.NoTargets;
        if (result.SucceededCount == result.AttemptedCount)
            return PushDispatchDisposition.FullySucceeded;
        if (result.RetryableFailureCount == 0)
            return PushDispatchDisposition.PermanentlyCompleted;
        return PushDispatchDisposition.Retryable;
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

/// <summary>
/// P0-2：推送投递处置分类。Consumer 据此决定 JetStream ACK / NAK。
/// <para>
/// 替代"DispatchAsync 正常返回即 ACK"的旧行为——可重试失败必须 NAK 重投，
/// 否则 FCM 503 / APNs 429 / Provider 超时等场景会永久丢失推送。
/// </para>
/// </summary>
internal enum PushDispatchDisposition
{
    /// <summary>
    /// 用户无注册令牌（AttemptedCount=0）。ACK——重投也不会有令牌可投。
    /// </summary>
    NoTargets = 0,

    /// <summary>
    /// 全部令牌投递成功。ACK。
    /// </summary>
    FullySucceeded = 1,

    /// <summary>
    /// 无可重试失败，仅有永久失败（invalid_token / payload_too_large）。
    /// ACK——重投不会改变结果，无效令牌已被注销。
    /// </summary>
    PermanentlyCompleted = 2,

    /// <summary>
    /// 存在可重试失败（provider_unavailable / rate_limited）。
    /// NAK 并延迟重投，让 Provider 恢复后再次尝试。
    /// <para>
    /// 已成功 token 在重投后会重复收到推送——这是当前命令粒度 NAK 的已知权衡。
    /// 长期应拆分为每 token 独立工作项，支持 PartiallyRetryable 仅重试失败 token。
    /// </para>
    /// </summary>
    Retryable = 3
}
