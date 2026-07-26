namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime;

/// <summary>
/// 将 Realtime 事件的毫秒时间戳转换为 UTC <see cref="DateTime"/>。
/// 溢出（无效时间戳）时回退到 <see cref="TimeProvider"/> 当前 UTC 时间。
/// 从 <c>RealtimeEventDispatcher.GetSentUtc</c> 抽取，消除 handler 对 TimeProvider 的直接依赖。
/// </summary>
internal sealed class RealtimeTimestampConverter
{
    private readonly TimeProvider _timeProvider;

    public RealtimeTimestampConverter(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
    }

    public DateTime ToUtc(long unixMs)
    {
        try
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(unixMs).UtcDateTime;
        }
        catch (ArgumentOutOfRangeException)
        {
            return _timeProvider.GetUtcNow().UtcDateTime;
        }
    }
}
