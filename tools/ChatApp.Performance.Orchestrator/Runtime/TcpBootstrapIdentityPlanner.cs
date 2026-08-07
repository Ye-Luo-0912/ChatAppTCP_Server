namespace ChatApp.Performance.Orchestrator.Runtime;

/// <summary>
/// Maps the globally bootstrapped healthy identities to per-Gateway connection
/// slots. Slow-reader slots deliberately reuse a locally targetable healthy
/// identity so they receive real chat fan-out without removing the readable
/// device that makes delivery observable.
/// </summary>
internal static class TcpBootstrapIdentityPlanner
{
    public static TcpBootstrapIdentityPlan Create(
        IReadOnlyList<TcpBootstrapIdentity> healthyIdentities,
        IReadOnlyList<TcpBootstrapPartitionShape> partitionShapes)
    {
        ArgumentNullException.ThrowIfNull(healthyIdentities);
        ArgumentNullException.ThrowIfNull(partitionShapes);
        if (partitionShapes.Count == 0)
            throw new ArgumentException("At least one TCP partition is required.", nameof(partitionShapes));

        var expectedHealthyIdentities = 0;
        for (var index = 0; index < partitionShapes.Count; index++)
        {
            var shape = partitionShapes[index];
            if (shape.Connections <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(partitionShapes),
                    $"TCP partition {index} must contain at least one connection.");
            }

            if (shape.SlowReaders < 0 || shape.SlowReaders > shape.Connections)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(partitionShapes),
                    $"TCP partition {index} has an invalid slow-reader count.");
            }

            var healthyCount = shape.Connections - shape.SlowReaders;
            if (shape.ActiveSenders < 0 || shape.ActiveSenders > healthyCount)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(partitionShapes),
                    $"TCP partition {index} has an invalid active-sender count.");
            }

            if (shape.SlowReaders > 0 && healthyCount < 2)
            {
                throw new ArgumentException(
                    $"TCP partition {index} requires at least two healthy identities " +
                    "when slow readers are enabled.",
                    nameof(partitionShapes));
            }

            if (shape.SlowReaders > 0 && shape.ActiveSenders == 0)
            {
                throw new ArgumentException(
                    $"TCP partition {index} requires an active sender so every slow " +
                    "reader receives real chat traffic.",
                    nameof(partitionShapes));
            }

            expectedHealthyIdentities = checked(expectedHealthyIdentities + healthyCount);
        }

        if (healthyIdentities.Count != expectedHealthyIdentities)
        {
            throw new ArgumentException(
                $"TCP bootstrap supplied {healthyIdentities.Count} healthy identities; " +
                $"{expectedHealthyIdentities} were required by the partition plan.",
                nameof(healthyIdentities));
        }

        if (healthyIdentities.Any(static identity =>
                identity.UserId <= 0 || string.IsNullOrWhiteSpace(identity.Token)))
        {
            throw new ArgumentException(
                "TCP bootstrap identities require a positive user id and a non-empty token.",
                nameof(healthyIdentities));
        }

        if (healthyIdentities.Select(static identity => identity.UserId).Distinct().Count() !=
            healthyIdentities.Count)
        {
            throw new ArgumentException(
                "TCP bootstrap healthy identities must have distinct user ids.",
                nameof(healthyIdentities));
        }

        if (healthyIdentities.Select(static identity => identity.Token)
                .Distinct(StringComparer.Ordinal).Count() != healthyIdentities.Count)
        {
            throw new ArgumentException(
                "TCP bootstrap healthy identities must have distinct tokens.",
                nameof(healthyIdentities));
        }

        var healthyPartitions =
            new IReadOnlyList<TcpBootstrapIdentity>[partitionShapes.Count];
        var connectionPartitions =
            new IReadOnlyList<TcpBootstrapIdentity>[partitionShapes.Count];
        var identityOffset = 0;

        for (var partitionIndex = 0;
             partitionIndex < partitionShapes.Count;
             partitionIndex++)
        {
            var shape = partitionShapes[partitionIndex];
            var healthyCount = shape.Connections - shape.SlowReaders;
            var healthy = healthyIdentities
                .Skip(identityOffset)
                .Take(healthyCount)
                .ToArray();
            identityOffset += healthyCount;
            healthyPartitions[partitionIndex] = healthy;

            var connections = new TcpBootstrapIdentity[shape.Connections];
            healthy.CopyTo(connections, 0);
            if (shape.SlowReaders > 0)
            {
                var slowTargets = GetSlowReaderTargetPool(
                    healthy,
                    shape.ActiveSenders);
                for (var slowIndex = 0; slowIndex < shape.SlowReaders; slowIndex++)
                {
                    connections[healthyCount + slowIndex] =
                        slowTargets[slowIndex % slowTargets.Count];
                }
            }

            connectionPartitions[partitionIndex] = connections;
        }

        return new TcpBootstrapIdentityPlan(
            healthyPartitions,
            connectionPartitions);
    }

    private static List<TcpBootstrapIdentity> GetSlowReaderTargetPool(
        TcpBootstrapIdentity[] healthyIdentities,
        int activeSenders)
    {
        var byUserId = healthyIdentities.ToDictionary(static identity => identity.UserId);
        var ring = byUserId.Keys.Order().ToArray();
        var targetUserIds = new HashSet<long>();
        var targets = new List<TcpBootstrapIdentity>(activeSenders);

        for (var senderIndex = 0; senderIndex < activeSenders; senderIndex++)
        {
            var senderUserId = healthyIdentities[senderIndex].UserId;
            var ringIndex = Array.BinarySearch(ring, senderUserId);
            if (ringIndex < 0)
            {
                throw new InvalidOperationException(
                    $"TCP bootstrap sender user {senderUserId} was absent from its peer ring.");
            }

            var targetUserId = ring[(ringIndex + 1) % ring.Length];
            if (targetUserIds.Add(targetUserId))
                targets.Add(byUserId[targetUserId]);
        }

        if (targets.Count == 0)
        {
            throw new InvalidOperationException(
                "TCP slow-reader planning produced no actively targeted healthy identity.");
        }

        return targets;
    }
}

internal readonly record struct TcpBootstrapPartitionShape(
    int Connections,
    int SlowReaders,
    int ActiveSenders);

internal sealed record TcpBootstrapIdentityPlan(
    IReadOnlyList<TcpBootstrapIdentity>[] HealthyPartitions,
    IReadOnlyList<TcpBootstrapIdentity>[] ConnectionPartitions);
