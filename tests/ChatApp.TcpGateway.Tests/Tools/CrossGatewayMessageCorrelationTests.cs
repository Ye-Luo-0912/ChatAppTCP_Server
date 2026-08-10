using ChatApp.Performance.Orchestrator.Diagnostics;
using ChatApp.Performance.Orchestrator.Runtime;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class CrossGatewayMessageCorrelationTests
{
    [Fact]
    public void EvaluateMatchesTheSameIdsAcrossOppositeChildren()
    {
        var result = CrossGatewayMessageCorrelation.Evaluate(
        [
            CreateLoad(
                "tcp:gateway-1",
                1,
                acknowledgement: new MessageIdFingerprintSummary(1, 10, 10),
                delivery: new MessageIdFingerprintSummary(1, 20, 20)),
            CreateLoad(
                "tcp:gateway-2",
                2,
                acknowledgement: new MessageIdFingerprintSummary(1, 20, 20),
                delivery: new MessageIdFingerprintSummary(1, 10, 10))
        ]);

        Assert.True(result.Passed);
        Assert.Equal(2, result.AcknowledgementCount);
        Assert.Equal(2, result.DeliveryCount);
        Assert.Contains("probabilistic", result.Detail, StringComparison.Ordinal);
        Assert.Contains("not a per-id log", result.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void EvaluateRejectsEqualCountsWithDifferentMessageIds()
    {
        var result = CrossGatewayMessageCorrelation.Evaluate(
        [
            CreateLoad(
                "tcp:gateway-1",
                1,
                acknowledgement: new MessageIdFingerprintSummary(1, 10, 10),
                delivery: new MessageIdFingerprintSummary(1, 30, 30)),
            CreateLoad(
                "tcp:gateway-2",
                2,
                acknowledgement: new MessageIdFingerprintSummary(1, 20, 20),
                delivery: new MessageIdFingerprintSummary(1, 10, 10))
        ]);

        Assert.False(result.Passed);
        Assert.Contains("sets differ", result.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void EvaluateIsInconclusiveWhenLegacyChildOmitsFingerprints()
    {
        var first = CreateLoad(
            "tcp:gateway-1",
            1,
            acknowledgement: new MessageIdFingerprintSummary(1, 10, 10),
            delivery: new MessageIdFingerprintSummary(1, 20, 20));
        var legacy = CreateLoad(
            "tcp:gateway-2",
            2,
            acknowledgement: new MessageIdFingerprintSummary(1, 20, 20),
            delivery: null);

        var result = CrossGatewayMessageCorrelation.Evaluate([first, legacy]);

        Assert.False(result.Passed);
        Assert.Contains("inconclusive", result.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void EvaluateRejectsSameGatewayLoopbackEvenWhenGlobalSetsMatch()
    {
        var result = CrossGatewayMessageCorrelation.Evaluate(
        [
            CreateLoad(
                "tcp:gateway-1",
                1,
                acknowledgement: new MessageIdFingerprintSummary(1, 10, 10),
                delivery: new MessageIdFingerprintSummary(1, 10, 10)),
            CreateLoad(
                "tcp:gateway-2",
                2,
                acknowledgement: new MessageIdFingerprintSummary(1, 20, 20),
                delivery: new MessageIdFingerprintSummary(1, 20, 20))
        ]);

        Assert.False(result.Passed);
        Assert.Contains("next-ring Gateway", result.Detail, StringComparison.Ordinal);
    }

    private static LoadResultSummary CreateLoad(
        string name,
        int gatewayOrdinal,
        MessageIdFingerprintSummary? acknowledgement,
        MessageIdFingerprintSummary? delivery) =>
        new()
        {
            Name = name,
            Kind = "tcp-chat",
            MessagesSent = 1,
            MessagesAcknowledged = 1,
            MessagesReceived = 1,
            AcknowledgementIdFingerprint = acknowledgement,
            DeliveryIdFingerprint = delivery,
            SourceReport = Path.Combine(
                "run",
                $"tcp-gateway-{gatewayOrdinal}",
                "tcp-load.json")
        };
}
