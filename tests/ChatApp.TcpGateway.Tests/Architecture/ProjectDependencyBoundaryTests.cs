using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Metrics;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Architecture;

/// <summary>
/// 架构边界自动检查：强制 AGENTS.md 中声明的依赖方向不被破坏。
/// 任何对 Core / Infrastructure / Observability 顶层命名空间的越界引用都会在此处失败，
/// 防止后续重构把业务逻辑下沉到 Core 或让 Core 反向依赖 Gateway/Hosting/Redis。
/// </summary>
public sealed class ProjectDependencyBoundaryTests
{
    /// <summary>
    /// Core 层只能依赖 BCL。禁止引用 Logging / Redis / Hosting / Gateway / Infrastructure / Observability。
    /// </summary>
    [Fact]
    public void Core_Assembly_DoesNot_Reference_Forbidden_Assemblies()
    {
        var coreAssembly = typeof(AttachmentRef).Assembly;
        var forbidden = new HashSet<string>(StringComparer.Ordinal)
        {
            "Microsoft.Extensions.Logging",
            "Microsoft.Extensions.Logging.Abstractions",
            "Microsoft.Extensions.Hosting",
            "Microsoft.Extensions.Hosting.Abstractions",
            "Microsoft.Extensions.DependencyInjection",
            "Microsoft.Extensions.DependencyInjection.Abstractions",
            "Microsoft.Extensions.Options",
            "StackExchange.Redis",
            "OpenTelemetry",
        };

        var referenced = coreAssembly.GetReferencedAssemblies()
            .Select(name => name.Name ?? string.Empty)
            .Where(name => forbidden.Contains(name))
            .ToList();

        Assert.Empty(referenced);
    }

    /// <summary>
    /// Observability 层不得引用 Gateway / Infrastructure 业务类型。
    /// 允许：BCL、Logging 抽象、Metrics 抽象。
    /// </summary>
    [Fact]
    public void Observability_Assembly_DoesNot_Reference_Gateway_Or_Infrastructure()
    {
        var observabilityAssembly = typeof(GatewayMetrics).Assembly;
        var forbidden = new HashSet<string>(StringComparer.Ordinal)
        {
            "ChatApp.TcpGateway.Gateway",
            "ChatApp.TcpGateway.Infrastructure",
        };

        var referenced = observabilityAssembly.GetReferencedAssemblies()
            .Select(name => name.Name ?? string.Empty)
            .Where(name => forbidden.Contains(name))
            .ToList();

        Assert.Empty(referenced);
    }

    /// <summary>
    /// Infrastructure 层不得引用 Gateway.Networking / Gateway 业务 handler。
    /// AGENTS.md 明确：Infrastructure 可依赖 Core + Redis + DI，但不得反向依赖 Gateway。
    /// </summary>
    [Fact]
    public void Infrastructure_Assembly_DoesNot_Reference_Gateway()
    {
        var infraAssembly = typeof(RedisOptions).Assembly;
        var forbidden = new HashSet<string>(StringComparer.Ordinal)
        {
            "ChatApp.TcpGateway.Gateway",
        };

        var referenced = infraAssembly.GetReferencedAssemblies()
            .Select(name => name.Name ?? string.Empty)
            .Where(name => forbidden.Contains(name))
            .ToList();

        Assert.Empty(referenced);
    }
}
