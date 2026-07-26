namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 单连接 Pipe 入站字节租约：Fill 时预留，Read 消费时释放，断开时归还剩余。
/// </summary>
internal sealed class SessionInboundPipeLease(GlobalInboundBudget budget)
{
    private int _reserved;

    public bool TryReserve(int byteCount)
    {
        if (byteCount <= 0)
            return true;

        if (!budget.TryReserve(byteCount))
            return false;

        Interlocked.Add(ref _reserved, byteCount);
        return true;
    }

    public void Release(int byteCount)
    {
        if (byteCount <= 0)
            return;

        while (true)
        {
            var current = Volatile.Read(ref _reserved);
            if (current <= 0)
                return;

            var toRelease = Math.Min(current, byteCount);
            if (Interlocked.CompareExchange(ref _reserved, current - toRelease, current) == current)
            {
                budget.Release(toRelease);
                return;
            }
        }
    }

    public void ReleaseAll()
    {
        var remaining = Interlocked.Exchange(ref _reserved, 0);
        if (remaining > 0)
            budget.Release(remaining);
    }
}
