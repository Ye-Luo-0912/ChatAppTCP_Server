using ChatApp.Performance.Orchestrator.Runtime;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class TcpBootstrapIdentityPlannerTests
{
    [Fact]
    public void SlowSlotsReuseUsersTargetedByActiveSenders()
    {
        var plan = TcpBootstrapIdentityPlanner.Create(
            CreateIdentities(firstUserId: 1, count: 4),
            [new TcpBootstrapPartitionShape(
                Connections: 6,
                SlowReaders: 2,
                ActiveSenders: 2)]);

        Assert.Equal([1L, 2L, 3L, 4L], UserIds(plan.HealthyPartitions[0]));
        Assert.Equal(
            [1L, 2L, 3L, 4L, 2L, 3L],
            UserIds(plan.ConnectionPartitions[0]));
        Assert.Equal(
            plan.ConnectionPartitions[0][1].Token,
            plan.ConnectionPartitions[0][4].Token);
        Assert.Equal(
            plan.ConnectionPartitions[0][2].Token,
            plan.ConnectionPartitions[0][5].Token);
    }

    [Fact]
    public void MultipleGatewayPartitionsPreserveHealthyPrefixAndSlowSuffix()
    {
        var plan = TcpBootstrapIdentityPlanner.Create(
            CreateIdentities(firstUserId: 100, count: 6),
            [
                new TcpBootstrapPartitionShape(
                    Connections: 5,
                    SlowReaders: 2,
                    ActiveSenders: 2),
                new TcpBootstrapPartitionShape(
                    Connections: 4,
                    SlowReaders: 1,
                    ActiveSenders: 1)
            ]);

        Assert.Equal([100L, 101L, 102L], UserIds(plan.HealthyPartitions[0]));
        Assert.Equal([103L, 104L, 105L], UserIds(plan.HealthyPartitions[1]));
        Assert.Equal(
            [100L, 101L, 102L, 101L, 102L],
            UserIds(plan.ConnectionPartitions[0]));
        Assert.Equal(
            [103L, 104L, 105L, 104L],
            UserIds(plan.ConnectionPartitions[1]));
    }

    [Fact]
    public void ZeroSlowReadersPreservesOneIdentityPerConnection()
    {
        var plan = TcpBootstrapIdentityPlanner.Create(
            CreateIdentities(firstUserId: 200, count: 5),
            [
                new TcpBootstrapPartitionShape(
                    Connections: 3,
                    SlowReaders: 0,
                    ActiveSenders: 2),
                new TcpBootstrapPartitionShape(
                    Connections: 2,
                    SlowReaders: 0,
                    ActiveSenders: 1)
            ]);

        Assert.Equal([200L, 201L, 202L], UserIds(plan.ConnectionPartitions[0]));
        Assert.Equal([203L, 204L], UserIds(plan.ConnectionPartitions[1]));
        Assert.Equal(
            UserIds(plan.HealthyPartitions[0]),
            UserIds(plan.ConnectionPartitions[0]));
        Assert.Equal(
            UserIds(plan.HealthyPartitions[1]),
            UserIds(plan.ConnectionPartitions[1]));
    }

    [Fact]
    public void MoreSlowReadersThanTargetUsersReuseTheTargetRoundRobin()
    {
        var plan = TcpBootstrapIdentityPlanner.Create(
            CreateIdentities(firstUserId: 1_000, count: 2),
            [new TcpBootstrapPartitionShape(
                Connections: 5,
                SlowReaders: 3,
                ActiveSenders: 1)]);

        Assert.Equal(
            [1_000L, 1_001L, 1_001L, 1_001L, 1_001L],
            UserIds(plan.ConnectionPartitions[0]));
    }

    [Fact]
    public void SlowReaderPartitionRequiresTwoHealthyUsers()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            TcpBootstrapIdentityPlanner.Create(
                CreateIdentities(firstUserId: 1, count: 1),
                [new TcpBootstrapPartitionShape(
                    Connections: 2,
                    SlowReaders: 1,
                    ActiveSenders: 1)]));

        Assert.Contains("at least two healthy identities", exception.Message);
    }

    [Fact]
    public void SlowReaderPartitionRequiresAnActiveSender()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            TcpBootstrapIdentityPlanner.Create(
                CreateIdentities(firstUserId: 1, count: 2),
                [new TcpBootstrapPartitionShape(
                    Connections: 3,
                    SlowReaders: 1,
                    ActiveSenders: 0)]));

        Assert.Contains("requires an active sender", exception.Message);
    }

    [Fact]
    public void GlobalHealthyIdentityCountMustMatchAllPartitions()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            TcpBootstrapIdentityPlanner.Create(
                CreateIdentities(firstUserId: 1, count: 4),
                [
                    new TcpBootstrapPartitionShape(
                        Connections: 4,
                        SlowReaders: 1,
                        ActiveSenders: 1),
                    new TcpBootstrapPartitionShape(
                        Connections: 3,
                        SlowReaders: 0,
                        ActiveSenders: 1)
                ]));

        Assert.Contains("6 were required", exception.Message);
    }

    private static TcpBootstrapIdentity[] CreateIdentities(
        long firstUserId,
        int count) =>
        Enumerable.Range(0, count)
            .Select(index => new TcpBootstrapIdentity(
                firstUserId + index,
                $"token-{firstUserId + index}"))
            .ToArray();

    private static long[] UserIds(
        IReadOnlyList<TcpBootstrapIdentity> identities) =>
        identities.Select(static identity => identity.UserId).ToArray();
}
