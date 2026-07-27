using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ChatApp.ActorRuntime.Primitives;

/// <summary>
/// 单等待者信号：用于唤醒 Shard Consumer Loop。
/// <para>
/// v1 实现使用单槽 <see cref="Channel{T}"/>（容量 1，DropWrite 模式）：
/// <list type="bullet">
/// <item>每 Shard 只有一个信号通道，开销极小；</item>
/// <item>跨线程生产者（TryTell）通过 <see cref="Signal"/> 唤醒 Consumer；</item>
/// <item>Consumer 通过 <see cref="WaitAsync"/> 异步等待；</item>
/// <item>DropWrite 模式保证多次 Signal 合并为一次唤醒（已唤醒时不重复入队）。</item>
/// </list>
/// </para>
/// <para>
/// 基准证明单槽 Channel 仍是瓶颈后，可替换为自实现 IValueTaskSource 异步等待器
/// （基于 <c>ManualResetValueTaskSourceCore</c> + 单 slot 原子状态机）。
/// </para>
/// </summary>
internal sealed class SingleWaiterSignal
{
    // 容量 1 + BoundedChannelFullMode.DropWrite：多生产者 Signal 合并为一次唤醒。
    private static readonly BoundedChannelOptions Options = new(1)
    {
        FullMode = BoundedChannelFullMode.DropWrite,
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = false
    };

    private readonly Channel<byte> _channel = Channel.CreateBounded<byte>(Options);

    /// <summary>唤醒 Consumer。已唤醒时合并（DropWrite）。</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Signal()
    {
        _channel.Writer.TryWrite(0);
    }

    /// <summary>等待唤醒。配合 <see cref="TryReset"/> 可重复等待。</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<bool> WaitAsync(CancellationToken cancellationToken)
        => _channel.Reader.WaitToReadAsync(cancellationToken);

    /// <summary>读取并清除信号（如果存在）。与 WaitAsync 配合使用。</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReset()
    {
        // WaitToReadAsync 完成后，ReadAllAsync 模式要求读出 token 才能进入下一次 Wait。
        // 这里 TryRead 把信号消费掉，使下次 Wait 阻塞直到新 Signal。
        return _channel.Reader.TryRead(out _);
    }

    public void Complete() => _channel.Writer.TryComplete();
}
