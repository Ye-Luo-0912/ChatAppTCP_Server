namespace ChatApp.Performance.Orchestrator.Runtime;

internal static class BenchmarkTiming
{
    internal static readonly TimeSpan ShutdownAndReportGrace =
        TimeSpan.FromSeconds(30);

    public static TimeSpan CalculateLoadTimeout(
        TimeSpan ramp,
        TimeSpan stabilization,
        TimeSpan measurement,
        bool isTcpChat,
        TimeSpan tcpDeliveryDrain,
        bool pipelineEnabled,
        TimeSpan pipelineOperationTimeout) =>
        ramp
        + stabilization
        + measurement
        + (isTcpChat ? tcpDeliveryDrain : TimeSpan.Zero)
        + (pipelineEnabled ? pipelineOperationTimeout : TimeSpan.Zero)
        + ShutdownAndReportGrace;
}
