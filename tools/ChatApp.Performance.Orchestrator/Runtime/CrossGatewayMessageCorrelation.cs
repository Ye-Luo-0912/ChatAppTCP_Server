using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal static class CrossGatewayMessageCorrelation
{
    public static CrossGatewayMessageCorrelationResult Evaluate(
        IReadOnlyList<LoadResultSummary> loads)
    {
        if (loads.Count < 2)
        {
            return CrossGatewayMessageCorrelationResult.Failed(
                "Cross-Gateway message-id correlation requires at least two TCP child reports.");
        }

        var orderedLoads = new (int GatewayOrdinal, LoadResultSummary Load)[loads.Count];

        for (var index = 0; index < loads.Count; index++)
        {
            var load = loads[index];
            if (load.AcknowledgementIdFingerprint is not { } acknowledgements ||
                load.DeliveryIdFingerprint is not { } deliveries)
            {
                return CrossGatewayMessageCorrelationResult.Failed(
                    $"{load.Name} did not report message-id fingerprints; " +
                    "cross-child ACK/delivery correlation is inconclusive.");
            }

            if (acknowledgements.Count != load.MessagesAcknowledged)
            {
                return CrossGatewayMessageCorrelationResult.Failed(
                    $"{load.Name} fingerprinted {acknowledgements.Count} ACK ids but " +
                    $"reported {load.MessagesAcknowledged} acknowledgements.");
            }

            if (deliveries.Count != load.MessagesReceived)
            {
                return CrossGatewayMessageCorrelationResult.Failed(
                    $"{load.Name} fingerprinted {deliveries.Count} delivery ids but " +
                    $"reported {load.MessagesReceived} deliveries.");
            }

            if (!TryGetGatewayOrdinal(load.SourceReport, out var gatewayOrdinal))
            {
                return CrossGatewayMessageCorrelationResult.Failed(
                    $"{load.Name} source report is not under a tcp-gateway-N directory; " +
                    "cross-child route correlation is inconclusive.");
            }

            orderedLoads[index] = (gatewayOrdinal, load);
        }

        Array.Sort(
            orderedLoads,
            static (left, right) => left.GatewayOrdinal.CompareTo(right.GatewayOrdinal));
        for (var index = 0; index < orderedLoads.Length; index++)
        {
            if (orderedLoads[index].GatewayOrdinal != index + 1)
            {
                return CrossGatewayMessageCorrelationResult.Failed(
                    "Cross-Gateway child reports must contain one contiguous " +
                    "tcp-gateway-N directory for every Gateway.");
            }
        }

        long acknowledgementCount = 0;
        long deliveryCount = 0;
        for (var index = 0; index < orderedLoads.Length; index++)
        {
            var sender = orderedLoads[index];
            var receiver = orderedLoads[(index + 1) % orderedLoads.Length];
            var acknowledgements = sender.Load.AcknowledgementIdFingerprint!;
            var deliveries = receiver.Load.DeliveryIdFingerprint!;

            acknowledgementCount += acknowledgements.Count;
            deliveryCount += deliveries.Count;
            if (acknowledgements != deliveries)
            {
                return CrossGatewayMessageCorrelationResult.Failed(
                    $"Gateway {sender.GatewayOrdinal} ACK and next-ring delivery id sets differ: " +
                    "ACK ids do not match deliveries " +
                    $"observed by the next-ring Gateway {receiver.GatewayOrdinal} " +
                    $"(ACK={acknowledgements.Count}, delivery={deliveries.Count}).",
                    acknowledgementCount,
                    deliveryCount);
            }
        }

        if (acknowledgementCount == 0 || deliveryCount == 0)
        {
            return CrossGatewayMessageCorrelationResult.Failed(
                "Cross-Gateway message-id correlation had no ACK/delivery samples.");
        }

        return new CrossGatewayMessageCorrelationResult(
            true,
            acknowledgementCount,
            deliveryCount,
            $"Matched {acknowledgementCount} ACK ids to the next Gateway child's delivery ids " +
            "using a probabilistic count + 64-bit sum/xor fingerprint; this is not " +
            "a per-id log or cryptographic proof.");
    }

    private static bool TryGetGatewayOrdinal(string sourceReport, out int gatewayOrdinal)
    {
        const string prefix = "tcp-gateway-";
        gatewayOrdinal = 0;
        var directoryName = Path.GetFileName(Path.GetDirectoryName(sourceReport));
        return directoryName is not null &&
               directoryName.StartsWith(prefix, StringComparison.Ordinal) &&
               int.TryParse(
                   directoryName.AsSpan(prefix.Length),
                   System.Globalization.NumberStyles.None,
                   System.Globalization.CultureInfo.InvariantCulture,
                   out gatewayOrdinal) &&
               gatewayOrdinal > 0;
    }
}

internal sealed record CrossGatewayMessageCorrelationResult(
    bool Passed,
    long AcknowledgementCount,
    long DeliveryCount,
    string Detail)
{
    public static CrossGatewayMessageCorrelationResult Failed(
        string detail,
        long acknowledgementCount = 0,
        long deliveryCount = 0) =>
        new(false, acknowledgementCount, deliveryCount, detail);
}
