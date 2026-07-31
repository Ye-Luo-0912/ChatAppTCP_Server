using System.Reflection;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Tests.Diagnostics;

public sealed class GatewayLogContractTests
{
    private static LoggerMessageDefinition[] GetDefinitions() =>
        typeof(GatewayLog)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Select(method => new
            {
                Method = method,
                Attribute = method.GetCustomAttribute<LoggerMessageAttribute>()
            })
            .Where(item => item.Attribute is not null)
            .Select(item => new LoggerMessageDefinition(
                item.Method!,
                item.Attribute!))
            .ToArray();

    [Fact]
    public void LoggerMessageEventIdsAreUnique()
    {
        var definitions = GetDefinitions();
        var duplicates = definitions
            .GroupBy(item => item.Attribute.EventId)
            .Where(group => group.Count() > 1)
            .Select(group => new
            {
                EventId = group.Key,
                Methods = group.Select(item => item.Method.Name).ToArray()
            })
            .ToArray();

        Assert.Empty(duplicates);
    }

    [Fact]
    public void LoggerMessageEventNamesAreUnique()
    {
        var definitions = GetDefinitions();
        var duplicates = definitions
            .GroupBy(item => item.Attribute.EventName ?? string.Empty)
            .Where(group => group.Count() > 1)
            .Select(group => new
            {
                EventName = group.Key,
                Methods = group.Select(item => item.Method.Name).ToArray()
            })
            .ToArray();

        Assert.Empty(duplicates);
    }

    [Fact]
    public void LoggerMessageEventNamesAreNonEmpty()
    {
        var definitions = GetDefinitions();
        foreach (var definition in definitions)
        {
            Assert.False(
                string.IsNullOrWhiteSpace(definition.Attribute.EventName),
                $"{definition.Method.Name} must declare a non-empty EventName.");
        }
    }

    [Fact]
    public void AllEventIdsFallWithinAllocatedRanges()
    {
        // Each range must be a stable contract; EventIds outside these ranges
        // indicate an unplanned extension and should be rejected at build time.
        var allowedRanges = new[]
        {
            (Start: 1000, End: 1099), // Lifecycle
            (Start: 1100, End: 1199), // Transport
            (Start: 1200, End: 1299), // TCP commands
            (Start: 1300, End: 1399), // Dependencies
            (Start: 1400, End: 1499), // Realtime
            (Start: 1500, End: 1599), // Ephemeral
            (Start: 1600, End: 1699)  // Stubs (主线四 placeholder backends)
        };

        var definitions = GetDefinitions();
        foreach (var definition in definitions)
        {
            var id = definition.Attribute.EventId;
            var inside = false;
            foreach (var range in allowedRanges)
            {
                if (id >= range.Start && id <= range.End)
                {
                    inside = true;
                    break;
                }
            }

            Assert.True(
                inside,
                $"{definition.Method.Name} EventId {id} falls outside the allocated ranges.");
        }
    }

    private sealed record LoggerMessageDefinition(
        MethodInfo Method,
        LoggerMessageAttribute Attribute);
}
