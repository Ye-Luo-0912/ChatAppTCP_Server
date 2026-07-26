namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 全局入站缓冲字节预算。
/// <para>
/// 跟踪所有连接 Pipe 暂存字节与 Ordered/Query/Ephemeral lane 池化/复制 payload 的总和。
/// 在写入 Pipe 或复制到调度缓冲区前 <see cref="TryReserve"/>；消费/归还后 <see cref="Release"/>。
/// 超限时调用方应背压（暂停接收）或关闭连接，防止声称的全局上限形同虚设。
/// </para>
/// </summary>
internal sealed class GlobalInboundBudget
{
    private readonly long _maxBytes;
    private long _currentBytes;

    public GlobalInboundBudget(long maxBytes)
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
