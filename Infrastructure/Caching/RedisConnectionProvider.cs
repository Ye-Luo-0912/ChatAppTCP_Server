using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Caching;

public sealed class RedisConnectionProvider(
    IOptions<RedisOptions> options,
    ILogger<RedisConnectionProvider> logger)
    : IHostedService, IAsyncDisposable
{
    private readonly RedisOptions _options = options.Value;
    private readonly ILogger<RedisConnectionProvider> _logger = logger;
    private ConnectionMultiplexer? _connection;

    public IDatabase Database =>
        Volatile.Read(ref _connection)?.GetDatabase()
        ?? throw new InvalidOperationException("Redis connection has not been started.");

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var connectTask = ConnectionMultiplexer.ConnectAsync(_options.ConnectionString);
        var connection = await connectTask
            .WaitAsync(_options.StartupTimeout, cancellationToken)
            .ConfigureAwait(false);

        connection.ConnectionFailed += OnConnectionFailed;
        connection.ConnectionRestored += OnConnectionRestored;
        Volatile.Write(ref _connection, connection);
        _logger.DependencyConnected(GatewayDependency.Redis);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await using var connection = Interlocked.Exchange(ref _connection, null);
        if (connection is null)
        {
            return;
        }

        connection.ConnectionFailed -= OnConnectionFailed;
        connection.ConnectionRestored -= OnConnectionRestored;

        await connection.CloseAsync(allowCommandsToComplete: true)
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await using var connection = Interlocked.Exchange(ref _connection, null);
        if (connection is null)
        {
            return;
        }

        connection.ConnectionFailed -= OnConnectionFailed;
        connection.ConnectionRestored -= OnConnectionRestored;
        await connection.CloseAsync(allowCommandsToComplete: false).ConfigureAwait(false);
    }

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs args) =>
        _logger.DependencyDisconnected(
            GatewayDependency.Redis,
            args.EndPoint?.ToString(),
            args.FailureType.ToString(),
            args.Exception);

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs args) =>
        _logger.DependencyRestored(
            GatewayDependency.Redis,
            args.EndPoint?.ToString());
}
