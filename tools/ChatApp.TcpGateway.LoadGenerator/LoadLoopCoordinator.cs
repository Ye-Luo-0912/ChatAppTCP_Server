namespace ChatApp.TcpGateway.LoadGenerator;

internal static class LoadLoopCoordinator
{
    public static async Task ObserveAsync(
        Task loop,
        int clientIndex,
        string loopName,
        LoadRunState runState,
        CancellationToken expectedCancellationToken)
    {
        try
        {
            await loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (expectedCancellationToken.IsCancellationRequested ||
                  runState.LifetimeCancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            runState.FailRuntime(
                $"Client {clientIndex} {loopName} failed: {exception.Message}");
            throw;
        }
    }
}
