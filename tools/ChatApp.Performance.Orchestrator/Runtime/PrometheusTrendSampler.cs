using System.Collections.Concurrent;
using ChatApp.Performance.Orchestrator.Diagnostics;

namespace ChatApp.Performance.Orchestrator.Runtime;

internal sealed class PrometheusTrendSampler
{
    private readonly ConcurrentDictionary<string, MetricAccumulator> _metrics = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _errors = new(StringComparer.Ordinal);
    private int _successfulPolls;

    public IReadOnlyList<string> Errors => _errors.Keys.Order(StringComparer.Ordinal).ToArray();
    public int SuccessfulPolls => Volatile.Read(ref _successfulPolls);

    public async Task RunAsync(Uri endpoint, TimeSpan interval, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(interval);
        do
        {
            try
            {
                var metrics = await EndpointProbe.CapturePrometheusAsync(endpoint, ct).ConfigureAwait(false);
                foreach (var (series, value) in metrics)
                {
                    if (IsSoakRelevant(series))
                        _metrics.GetOrAdd(series, static name => new MetricAccumulator(name)).Sample(value);
                }
                Interlocked.Increment(ref _successfulPolls);
            }
            catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException)
            {
                _errors.TryAdd($"Prometheus trend sampling failed: {exception.Message}", 0);
            }
        }
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false));
    }

    public IReadOnlyList<PrometheusMetricTrend> GetTrends() => _metrics.Values
        .OrderBy(static metric => metric.Series, StringComparer.Ordinal)
        .Select(static metric => metric.CreateTrend())
        .ToArray();

    private static bool IsSoakRelevant(string series) =>
        series.StartsWith("dotnet_gc_", StringComparison.Ordinal) ||
        series.StartsWith("dotnet_process_", StringComparison.Ordinal) ||
        series.StartsWith("dotnet_thread_pool_", StringComparison.Ordinal) ||
        series.StartsWith("db_client_connection_count", StringComparison.Ordinal) ||
        series.StartsWith("db_client_connection_max", StringComparison.Ordinal) ||
        series.StartsWith("chatapp_jetstream_pending", StringComparison.Ordinal) ||
        series.StartsWith("chatapp_nats_connection_connected", StringComparison.Ordinal) ||
        series.StartsWith("realtime_outbox_", StringComparison.Ordinal);

    private sealed class MetricAccumulator(string series)
    {
        private int _samples;
        private double _first;
        private double _last;
        private double _minimum = double.PositiveInfinity;
        private double _maximum = double.NegativeInfinity;

        public string Series { get; } = series;

        public void Sample(double value)
        {
            if (_samples++ == 0)
                _first = value;
            _last = value;
            _minimum = Math.Min(_minimum, value);
            _maximum = Math.Max(_maximum, value);
        }

        public PrometheusMetricTrend CreateTrend() => new()
        {
            Series = Series,
            Samples = _samples,
            FirstValue = _first,
            LastValue = _last,
            MinimumValue = _samples == 0 ? 0 : _minimum,
            MaximumValue = _samples == 0 ? 0 : _maximum,
            Delta = _last - _first
        };
    }
}
