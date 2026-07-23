namespace ChatApp.TcpGateway.Infrastructure.Caching;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    public string ConnectionString { get; set; } = string.Empty;

    public TimeSpan StartupTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
