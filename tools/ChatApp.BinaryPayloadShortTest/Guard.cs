namespace ChatApp.BinaryPayloadShortTest;

/// <summary>二元校验助手：不满足即抛出，fail-fast 供 harness 各处使用。</summary>
internal static class Guard
{
    public static void Ensure(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }
}
