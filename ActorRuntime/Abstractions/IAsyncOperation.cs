namespace ChatApp.ActorRuntime.Abstractions;

/// <summary>
/// 异步操作契约：由 Actor 在 Receive 中通过 <see cref="ActorContext.TrySubmitOperation{TWork}"/>
/// 提交到全局 <c>AsyncOperationExecutor</c>。操作完成后由操作自身负责把
/// OperationCompleted 消息 Post 回原 Actor（通过构造时捕获的 Runtime 引用）。
/// <para>
/// Actor Shard 不得直接 await 外部 I/O：一个慢请求会阻塞整个 Shard 的所有 Actor。
/// 必须通过 TrySubmitOperation 把 I/O 转交独立 Executor，Shard 继续 Suspend 处理其他 Actor。
/// </para>
/// <para>
/// 实现建议：每个 Actor Domain 定义自己的 readonly struct 操作，构造时捕获
/// ActorKey + Generation + CompletionHandle + Runtime 引用。
/// 提交时 struct 会被装箱一次到 <see cref="IAsyncOperation"/>（I/O 路径已有分配，可接受）。
/// </para>
/// </summary>
public interface IAsyncOperation
{
    /// <summary>
    /// 执行外部 I/O（NATS/Redis/Query/History 等）。
    /// 完成或失败后由实现负责 Post 回原 Actor 的 OperationCompleted 消息。
    /// </summary>
    ValueTask ExecuteAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Executor 在超时、异常或停机放弃操作时调用。
    /// 需要恢复 Suspend Actor 的实现应在这里投递失败 Completion。
    /// </summary>
    void OnFailure(Exception? exception, AsyncOperationFailureKind kind)
    {
    }
}

public enum AsyncOperationFailureKind : byte
{
    Faulted = 0,
    TimedOut = 1,
    RuntimeStopping = 2
}
