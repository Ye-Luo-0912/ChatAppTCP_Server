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

    /// <summary>
    /// 获取第一个 server endpoint 用于 SCAN/KEYS 等 server 级操作（渐进式维护任务用）。
    /// 未连接或连接已关闭时返回 null。
    /// </summary>
    public IServer? GetServer()
    {
        var connection = Volatile.Read(ref _connection);
        if (connection is null)
            return null;
        var endpoints = connection.GetEndPoints();
        if (endpoints.Length == 0)
            return null;
        return connection.GetServer(endpoints[0]);
    }

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
