using System.Globalization;
using System.Net.Sockets;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal static class EndpointProbe
{
    private static readonly HttpClient HttpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(3)
    };

    public static async Task WaitForHttpSuccessAsync(
        Uri uri,
        ManagedProcess process,
        TimeSpan timeout,
        CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        Exception? lastException = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (process.HasExited)
                throw new InvalidOperationException($"{process.Label} exited before {uri} became ready.");
            try
            {
                using var response = await HttpClient.GetAsync(uri, ct).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                    return;
                var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                if (body.Length > 2_048)
                    body = body[..2_048];
                lastException = new HttpRequestException(
                    $"Endpoint returned {(int)response.StatusCode} {response.StatusCode}. Body={body}");
            }
            catch (Exception exception)
                when (exception is HttpRequestException or TaskCanceledException)
            {
                lastException = exception;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300), ct).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Endpoint did not become ready within {timeout.TotalSeconds:F0}s: {uri}",
            lastException);
    }

    public static async Task WaitForTcpAsync(
        string host,
        int port,
        ManagedProcess process,
        TimeSpan timeout,
        CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        Exception? lastException = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (process.HasExited)
                throw new InvalidOperationException($"{process.Label} exited before {host}:{port} became ready.");
            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
                return;
            }
            catch (Exception exception)
                when (exception is SocketException or OperationCanceledException)
            {
                lastException = exception;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(200), ct).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"TCP endpoint did not become ready within {timeout.TotalSeconds:F0}s: {host}:{port}",
            lastException);
    }

    public static async Task WaitForPrometheusValueAsync(
        Uri uri,
        string metricName,
        double expectedValue,
        ManagedProcess process,
        TimeSpan timeout,
        CancellationToken ct)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            if (process.HasExited)
                throw new InvalidOperationException($"{process.Label} exited before {metricName} became ready.");
            try
            {
                var metrics = await CapturePrometheusAsync(uri, ct).ConfigureAwait(false);
                if (metrics.Any(pair =>
                        (pair.Key.Equals(metricName, StringComparison.Ordinal) ||
                         pair.Key.StartsWith(metricName + "{", StringComparison.Ordinal)) &&
                        pair.Value.Equals(expectedValue)))
                {
                    return;
                }
            }
            catch (Exception exception)
                when (exception is HttpRequestException or TaskCanceledException)
            {
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300), ct).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Prometheus metric did not reach {expectedValue}: {metricName}");
    }

    public static async Task<Dictionary<string, double>> CapturePrometheusAsync(
        Uri uri,
        CancellationToken ct)
    {
        var content = await HttpClient.GetStringAsync(uri, ct).ConfigureAwait(false);
        var metrics = new Dictionary<string, double>(StringComparer.Ordinal);
        foreach (var rawLine in content.Split('\n'))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line[0] == '#')
                continue;
            var separator = line.LastIndexOf(' ');
            if (separator <= 0 || separator == line.Length - 1)
                continue;
            var name = line[..separator];
            if (!IsRelevantMetric(name))
                continue;
            if (double.TryParse(
                    line[(separator + 1)..],
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture,
                    out var value) &&
                double.IsFinite(value))
            {
                metrics[name] = value;
            }
        }

        return metrics;
    }

    private static bool IsRelevantMetric(string name) =>
        name.StartsWith("chatapp_", StringComparison.Ordinal) ||
        name.StartsWith("realtime_", StringComparison.Ordinal) ||
        name.StartsWith("process_", StringComparison.Ordinal) ||
        name.StartsWith("dotnet_", StringComparison.Ordinal) ||
        name.StartsWith("npgsql_", StringComparison.Ordinal) ||
        name.StartsWith("db_client_", StringComparison.Ordinal);
}
