namespace ChatApp.TcpGateway.LoadGenerator;

internal static class LoadGateEvaluator
{
    public static LoadGateEvaluation Evaluate(
        LoadOptions options,
        TargetPlan targetPlan,
        bool measurementStarted,
        int successfulConnections,
        long sent,
        long expectedDeliveries,
        long received,
        long acknowledged,
        long rejected,
        long duplicateAcknowledgements,
        long duplicateDeliveries,
        long latencySamples,
        long trackingExpired,
        long trackingDropped,
        bool deliveryDrainCompleted,
        string? runtimeFailure)
    {
        var failures = new List<string>();
        if (targetPlan.Error is not null)
            failures.Add(targetPlan.Error);

        if (successfulConnections != options.Connections)
        {
            failures.Add(
                $"Only {successfulConnections}/{options.Connections} clients completed successfully.");
        }

        if (runtimeFailure is not null)
            failures.Add($"Measurement aborted early: {runtimeFailure}");

        if (!measurementStarted)
        {
            failures.Add("Measurement did not start because the ready gate failed.");
            return new LoadGateEvaluation(false, failures);
        }

        if (options.Mode is LoadMode.Heartbeat or LoadMode.Chat)
        {
            if (sent == 0)
                failures.Add("No operations were sent during measurement.");

            var acknowledgementRatio = sent == 0
                ? 0d
                : acknowledged / (double)sent;
            if (acknowledgementRatio < options.MinimumAcknowledgementRatio)
            {
                failures.Add(
                    $"Acknowledgement ratio {acknowledgementRatio:F6} is below " +
                    $"the required {options.MinimumAcknowledgementRatio:F6}.");
            }

            if (latencySamples == 0)
                failures.Add("No terminal latency samples were recorded.");
        }

        if (options.Mode == LoadMode.Chat)
        {
            if (options.DeliveryDrain > TimeSpan.Zero &&
                !deliveryDrainCompleted)
            {
                failures.Add(
                    $"Delivery drain did not observe ACK and every expected recipient " +
                    $"delivery for each sent " +
                    $"message within {options.DeliveryDrain.TotalSeconds:F0} seconds.");
            }

            // item 五：跨 Gateway 配对时，目标投递落在另一 Gateway 上，本地
            // LoadGenerator 无法观测目标投递，因此投递比与“未跟踪投递”校验
            // 不再适用（改为 ack 校验 + 接收侧 LoadGenerator 观测实际投递）。
            var crossGateway = options.TargetRingFilePath is not null;
            if (!crossGateway)
            {
                var deliveryRatio = expectedDeliveries == 0
                    ? 0d
                    : received / (double)expectedDeliveries;
                if (deliveryRatio < options.MinimumDeliveryRatio)
                {
                    failures.Add(
                        $"Delivery ratio {deliveryRatio:F6} is below " +
                        $"the required {options.MinimumDeliveryRatio:F6}.");
                }
            }

            if (rejected != 0)
                failures.Add($"Gateway rejected {rejected} chat operations.");
            if (duplicateAcknowledgements != 0)
            {
                failures.Add(
                    $"Observed {duplicateAcknowledgements} duplicate or untracked " +
                    "message acknowledgements.");
            }
            if (!crossGateway && duplicateDeliveries != 0)
            {
                failures.Add(
                    $"Observed {duplicateDeliveries} duplicate or untracked peer deliveries.");
            }
            if (trackingDropped != 0)
            {
                failures.Add(
                    $"Latency tracking dropped {trackingDropped} operations because " +
                    "the bounded in-flight limit was reached.");
            }
            if (trackingExpired != 0)
            {
                failures.Add(
                    $"Latency tracking expired {trackingExpired} operations before a terminal frame arrived.");
            }
        }

        if (options.Mode == LoadMode.Slowloris &&
            acknowledged != options.Connections)
        {
            failures.Add(
                $"Gateway closed only {acknowledged}/{options.Connections} slowloris connections.");
        }

        return new LoadGateEvaluation(failures.Count == 0, failures);
    }
}

internal sealed record LoadGateEvaluation(
    bool Passed,
    IReadOnlyList<string> Failures);
