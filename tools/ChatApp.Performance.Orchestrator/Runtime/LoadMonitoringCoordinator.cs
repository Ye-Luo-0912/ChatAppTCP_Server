namespace ChatApp.Performance.Orchestrator.Runtime;

internal sealed record LoadExitObservation(
    string Label,
    int ExitCode,
    TimeSpan Elapsed,
    TimeSpan ExpectedMinimumRuntime);

internal sealed record LoadMonitoringResult(
    bool ServicesAlive,
    IReadOnlyList<LoadExitObservation> Loads,
    string? FailFastReason);

internal static class LoadMonitoringCoordinator
{
    private static readonly TimeSpan EarlyExitTolerance = TimeSpan.FromSeconds(1);

    public static async Task<LoadMonitoringResult> WaitForCompletionAsync(
        IReadOnlyList<Task<LoadExitObservation>> loadTasks,
        Func<string?> findExitedService,
        TimeSpan pollInterval,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(loadTasks);
        ArgumentNullException.ThrowIfNull(findExitedService);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(
            pollInterval,
            TimeSpan.Zero);

        var pending = loadTasks
            .Select(static (task, index) => new PendingLoad(index, task))
            .ToList();
        var completed = new List<CompletedLoad>(loadTasks.Count);

        while (pending.Count != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var exitedService = findExitedService();
            if (exitedService is not null)
            {
                return CreateFailure(
                    servicesAlive: false,
                    completed,
                    $"{exitedService} exited while load children were still running.");
            }

            var poll = Task.Delay(pollInterval, cancellationToken);
            var waitCandidates = pending
                .Select(static item => (Task)item.Task)
                .Append(poll)
                .ToArray();
            var signaled = await Task.WhenAny(waitCandidates).ConfigureAwait(false);
            if (ReferenceEquals(signaled, poll))
                continue;

            var newlyCompleted = pending
                .Where(static item => item.Task.IsCompleted)
                .ToArray();
            foreach (var item in newlyCompleted)
            {
                pending.Remove(item);
                completed.Add(new CompletedLoad(
                    item.Index,
                    await item.Task.ConfigureAwait(false)));
            }

            exitedService = findExitedService();
            if (exitedService is not null)
            {
                return CreateFailure(
                    servicesAlive: false,
                    completed,
                    $"{exitedService} exited while load children were still running.");
            }

            var loadFailure = newlyCompleted
                .Select(static item => GetFailureReason(item.Task.Result))
                .FirstOrDefault(static reason => reason is not null);
            if (loadFailure is not null)
            {
                return CreateFailure(
                    servicesAlive: true,
                    completed,
                    loadFailure);
            }
        }

        var finalExitedService = findExitedService();
        return finalExitedService is null
            ? new LoadMonitoringResult(
                ServicesAlive: true,
                OrderObservations(completed),
                FailFastReason: null)
            : CreateFailure(
                servicesAlive: false,
                completed,
                $"{finalExitedService} exited before load monitoring completed.");
    }

    public static string? GetFailureReason(LoadExitObservation observation)
    {
        if (observation.ExitCode != 0)
        {
            return $"{observation.Label} exited with code {observation.ExitCode} " +
                   "(process crash or semantic/report gate failure).";
        }

        if (observation.Elapsed + EarlyExitTolerance <
            observation.ExpectedMinimumRuntime)
        {
            return $"{observation.Label} exited before its " +
                   "ramp/stabilization/measurement window completed " +
                   $"({observation.Elapsed.TotalSeconds:F2}s observed, " +
                   $"{observation.ExpectedMinimumRuntime.TotalSeconds:F2}s expected).";
        }

        return null;
    }

    private static LoadMonitoringResult CreateFailure(
        bool servicesAlive,
        IReadOnlyList<CompletedLoad> completed,
        string reason) =>
        new(
            servicesAlive,
            OrderObservations(completed),
            $"Fail-fast aborted the benchmark round: {reason}");

    private static LoadExitObservation[] OrderObservations(
        IEnumerable<CompletedLoad> completed) =>
        completed
            .OrderBy(static item => item.Index)
            .Select(static item => item.Observation)
            .ToArray();

    private sealed record PendingLoad(
        int Index,
        Task<LoadExitObservation> Task);

    private sealed record CompletedLoad(
        int Index,
        LoadExitObservation Observation);
}
