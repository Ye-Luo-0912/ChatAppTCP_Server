namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 全局出站队列字节预算。
/// <para>
/// 跟踪所有连接出站队列的字节总和，提供原子 TryReserve/Release。
/// 超过 <see cref="_maxBytes"/> 时 TryReserve 返回 false，调用方应丢弃低优先级帧
///（Typing/Presence）或拒绝新历史/Sync 响应。
/// </para>
/// </summary>
internal sealed class GlobalOutboundBudget
{
    private readonly long _maxBytes;
    private long _currentBytes;

    public GlobalOutboundBudget(long maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBytes);
        _maxBytes = maxBytes;
    }

    public long CurrentBytes => Volatile.Read(ref _currentBytes);
    public long MaxBytes => _maxBytes;

    /// <summary>
    /// 尝试预留指定字节数。成功返回 true，超限返回 false。
    /// </summary>
    public bool TryReserve(int byteCount)
    {
        if (byteCount <= 0)
            return true;

        var current = Interlocked.Add(ref _currentBytes, byteCount);
        if (current <= _maxBytes)
            return true;

        // 超限：回滚。
        Interlocked.Add(ref _currentBytes, -byteCount);
        return false;
    }

    /// <summary>
    /// 释放已预留的字节数。
    /// </summary>
    public void Release(int byteCount)
    {
        if (byteCount <= 0)
            return;
        Interlocked.Add(ref _currentBytes, -byteCount);
    }

    /// <summary>
    /// 当前已用预算比例（0.0 - 1.0+）。
    /// </summary>
    public double UsageRatio => (double)CurrentBytes / _maxBytes;
}
