using ChatApp.TcpGateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class OutboundQueueBudgetTests
{
    [Fact]
    public void TryReserveRejectsBytesBeyondConfiguredLimit()
    {
        var budget = new OutboundQueueBudget(maximumBytes: 100);

        Assert.True(budget.TryReserve(60));
        Assert.False(budget.TryReserve(41));
        Assert.Equal(60, budget.CurrentBytes);

        budget.Release(60);

        Assert.Equal(0, budget.CurrentBytes);
    }

    [Fact]
    public void OversizedSingleFrameDoesNotConsumeBudget()
    {
        var budget = new OutboundQueueBudget(maximumBytes: 100);

        Assert.False(budget.TryReserve(101));
        Assert.Equal(0, budget.CurrentBytes);
    }

    [Fact]
    public async Task ConcurrentReservationsNeverExceedLimit()
    {
        var budget = new OutboundQueueBudget(maximumBytes: 100);
        var attempts = Enumerable.Range(0, 64)
            .Select(_ => Task.Run(() => budget.TryReserve(10)))
            .ToArray();

        var results = await Task.WhenAll(attempts);

        Assert.Equal(10, results.Count(static accepted => accepted));
        Assert.Equal(100, budget.CurrentBytes);

        foreach (var accepted in results)
        {
            if (accepted)
            {
                budget.Release(10);
            }
        }

        Assert.Equal(0, budget.CurrentBytes);
    }
}
