namespace ChatApp.TcpGateway.Observability;

/// <summary>
/// Resolves a <see cref="PacketCommand"/> to a stable lowercase name used as a
/// low-cardinality metrics tag. Avoids per-call <c>ToString()</c> allocation on
/// hot paths and guarantees tag-value stability across enum renames.
/// </summary>
public static class PacketCommandNames
{
    private static readonly string[] Names = BuildNames();

    /// <summary>Returns the stable name for <paramref name="command"/>.</summary>
    public static string Get(PacketCommand command)
    {
        var index = (int)command;
        var names = Names;
        if ((uint)index < (uint)names.Length)
        {
            var name = names[index];
            if (name is not null)
                return name;
        }

        // Unknown commands fall back to a stable literal rather than the enum
        // textual representation, preserving tag cardinality bounds.
        return "unknown";
    }

    private static string[] BuildNames()
    {
        // PacketCommand is a ushort; allocate once at full range to keep lookup O(1).
        var values = (PacketCommand[])Enum.GetValuesAsUnderlyingType<PacketCommand>();
        int max = 0;
        foreach (var v in values)
        {
            var i = (int)(ushort)v;
            if (i > max)
                max = i;
        }

        var names = new string[max + 1];
        foreach (PacketCommand c in Enum.GetValues<PacketCommand>())
        {
            names[(int)c] = ToLowerSnake(c.ToString());
        }
        return names;
    }

    private static string ToLowerSnake(string value)
    {
        // PacketCommand names are already PascalCase without separators; lowercasing
        // is sufficient and avoids per-call char arrays for the common case.
        return value.ToLowerInvariant();
    }
}
