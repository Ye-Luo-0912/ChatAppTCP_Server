using System.Runtime.CompilerServices;

namespace ChatApp.ActorRuntime.Runtime;

/// <summary>
/// FIFO Actor 的生产侧容量凭证。计数覆盖 Ingress + Mailbox 中尚未开始处理的消息，
/// 因此 TryTell 返回 Accepted 后不会在 Consumer 侧再次因 Mailbox 满而静默丢失。
/// </summary>
internal sealed class ActorAdmission
{
    private int _pending;
    private int _retired;

    public bool IsRetired => Volatile.Read(ref _retired) != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReserve(int capacity)
    {
        if (Volatile.Read(ref _retired) != 0)
            return false;

        var current = Volatile.Read(ref _pending);
        while (current < capacity)
        {
            var observed = Interlocked.CompareExchange(
                ref _pending,
                current + 1,
                current);
            if (observed == current)
            {
                // 与 idle retirement 竞态时撤销刚取得的 credit，并让调用方重取 route。
                if (Volatile.Read(ref _retired) == 0)
                    return true;

                Interlocked.Decrement(ref _pending);
                return false;
            }

            current = observed;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Release()
    {
        var remaining = Interlocked.Decrement(ref _pending);
        if (remaining < 0)
            throw new InvalidOperationException("Actor admission credit released more than once.");
    }

    public bool TryRetireIfIdle()
    {
        if (Volatile.Read(ref _pending) != 0)
            return false;

        return Interlocked.CompareExchange(ref _retired, 1, 0) == 0;
    }
}
