using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Caching;

public sealed partial class RedisConnectionProvider(
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
        LogConnectionInitialized();
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var connection = Interlocked.Exchange(ref _connection, null);
        if (connection is null)
        {
            return;
        }

        connection.ConnectionFailed -= OnConnectionFailed;
        connection.ConnectionRestored -= OnConnectionRestored;

        try
        {
            await connection.CloseAsync(allowCommandsToComplete: true)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            connection.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        var connection = Interlocked.Exchange(ref _connection, null);
        if (connection is null)
        {
            return;
        }

        connection.ConnectionFailed -= OnConnectionFailed;
        connection.ConnectionRestored -= OnConnectionRestored;
        await connection.CloseAsync(allowCommandsToComplete: false).ConfigureAwait(false);
        connection.Dispose();
    }

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs args) =>
        LogConnectionFailed(
            args.EndPoint,
            args.FailureType,
            args.Exception);

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs args) =>
        LogConnectionRestored(args.EndPoint);

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "Redis/Garnet connection initialized.")]
    private partial void LogConnectionInitialized();

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Error,
        Message = "Redis/Garnet connection failed at {Endpoint}: {FailureType}.")]
    private partial void LogConnectionFailed(
        System.Net.EndPoint? endpoint,
        ConnectionFailureType failureType,
        Exception? exception);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Information,
        Message = "Redis/Garnet connection restored at {Endpoint}.")]
    private partial void LogConnectionRestored(System.Net.EndPoint? endpoint);
}


