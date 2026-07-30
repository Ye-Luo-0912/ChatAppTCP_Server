using System.Collections.Concurrent;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Gateway.Networking.Executor;

/// <summary>
/// 帧装配超时扫描器：替代 <see cref="DeadlineWheel"/> 处理 Header/Payload 装配超时。
/// <para>
/// 与 <see cref="SendTimeoutTracker"/> 同构的设计哲学：
/// <list type="bullet">
/// <item><b>无每帧闭包分配</b>：超时判断基于 <see cref="FrameAssemblyState.AssemblyStartTimestamp"/>
/// 和 <see cref="FrameAssemblyState.CurrentPhaseTimeout"/>，不为每次装配创建捕获 session 的 <c>Action</c> 委托；</item>
/// <item><b>无每帧字典操作</b>：装配开始时注册一次，装配完成时注销一次。ReceiveAsync 循环内仅读取时间戳做内联快速检查；</item>
/// <item><b>无全局锁</b>：活跃装配集合用 <see cref="ConcurrentDictionary{TKey,TValue}"/>，
/// 不竞争 <see cref="DeadlineWheel"/> 的全局 <c>Lock</c>；</item>
/// <item><b>无 epoch int[1] 容器</b>：引用相等性天然解决 ABA——每次
/// <see cref="OnAssemblyStarted"/> 创建新 <see cref="FrameAssemblyState"/> 对象，
/// <see cref="OnAssemblyCompleted"/> 用 <c>ICollection.Remove(KeyValuePair)</c>
/// 基于引用相等性移除，旧完成不会误删新注册。</item>
/// </list>
/// </para>
/// <para>
/// 生命周期（per 帧装配周期）：
/// <list type="bullet">
/// <item><see cref="OnAssemblyStarted"/>：不完整帧首次出现或阶段切换时调用，返回 <see cref="FrameAssemblyState"/>；</item>
/// <item>每 100ms 扫描：遍历活跃集合，<c>GetElapsedTime(start) &gt;= timeout</c> 则 <c>session.Close(SlowFrameAssembly)</c>；</item>
/// <item><see cref="OnAssemblyCompleted"/>：帧装配完成（帧完整或进入 ReceivePayloadRemainder）时调用，传回 state 引用；</item>
/// <item><see cref="OnSessionClosed"/>：连接 Close 时 TryRemove 兜底。</item>
/// </list>
/// </para>
/// <para>
/// 与 <see cref="DeadlineWheel"/> 的分工：
/// <list type="bullet">
/// <item><b>本 Tracker</b>：Header/Payload 装配超时（高频注册/取消，每帧装配 1-2 次）；</item>
/// <item><b>DeadlineWheel</b>：Auth/Idle 超时（低频，每连接生命周期内数次，符合全局锁设计假设）；</item>
/// <item><b><see cref="SendTimeoutTracker"/></b>：Socket Send 超时（per 发送所有权周期）。</item>
/// </list>
/// </para>
/// </summary>
internal sealed class FrameAssemblyTimeoutTracker : IAsyncDisposable
{
    // 活跃装配集合：包含当前正在装配不完整帧的 Session。
    // Value = FrameAssemblyState（引用类型）：每次 OnAssemblyStarted 创建新对象，
    // OnAssemblyCompleted 用 ICollection.Remove(KeyValuePair) 基于引用相等性移除，
    // 天然防止旧完成误删新注册（ABA 问题），无需额外 epoch 计数器。
    private readonly ConcurrentDictionary<TcpClientSession, FrameAssemblyState> _activeAssemblies = new();
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _scanInterval;
    private readonly CancellationTokenSource _cts = new();
    private Task? _loopTask;
    private volatile bool _stopping;

    public static readonly TimeSpan DefaultScanInterval = TimeSpan.FromMilliseconds(100);

    public FrameAssemblyTimeoutTracker(
        TimeProvider? timeProvider = null,
        TimeSpan? scanInterval = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _scanInterval = scanInterval ?? DefaultScanInterval;
    }

    /// <summary>当前正在装配不完整帧的 Session 数（近似值，用于观测）。</summary>
    public int ActiveAssemblyCount => _activeAssemblies.Count;

    /// <summary>
    /// 不完整帧首次出现或阶段切换（Header→Payload）时注册/更新。
    /// <para>
    /// 返回的 <see cref="FrameAssemblyState"/> 引用必须在对应的
    /// <see cref="OnAssemblyCompleted"/> 中传回，确保旧完成不会误删新注册。
    /// </para>
    /// <para>
    /// 内部用 <see cref="ConcurrentDictionary{TKey,TValue}.AddOrUpdate"/> 原子操作：
    /// 首次注册添加新条目，阶段切换/重注册替换为新对象。
    /// 引用相等性确保 <see cref="OnAssemblyCompleted"/> 的 <c>ICollection.Remove</c>
    /// 只移除调用方持有的那个特定 state 对象。
    /// </para>
    /// </summary>
    /// <param name="session">当前连接。</param>
    /// <param name="phaseTimeout">当前装配阶段的超时时长（Header 或 Payload）。</param>
    /// <returns>当前装配周期的状态引用，传回给 <see cref="OnAssemblyCompleted"/>。</returns>
    public FrameAssemblyState OnAssemblyStarted(
        TcpClientSession session,
        TimeSpan phaseTimeout)
    {
        var timestamp = _timeProvider.GetTimestamp();
        return _activeAssemblies.AddOrUpdate(
            session,
            _ => new FrameAssemblyState(timestamp, phaseTimeout),
            (_, _) => new FrameAssemblyState(timestamp, phaseTimeout));
    }

    /// <summary>
    /// 帧装配完成（帧完整或进入 ReceivePayloadRemainder）时注销。
    /// 仅当 <paramref name="state"/> 引用与集合中的条目匹配时移除（引用相等性）。
    /// 若阶段切换已创建新 state，旧 state 的移除为 no-op，保留新注册。
    /// </summary>
    public void OnAssemblyCompleted(
        TcpClientSession session,
        FrameAssemblyState state)
        => ((ICollection<KeyValuePair<TcpClientSession, FrameAssemblyState>>)_activeAssemblies)
            .Remove(new KeyValuePair<TcpClientSession, FrameAssemblyState>(session, state));

    /// <summary>
    /// 连接关闭时清理：确保 Session 不残留在活跃集合中。
    /// </summary>
    public void OnSessionClosed(TcpClientSession session)
        => _activeAssemblies.TryRemove(session, out _);

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_loopTask is not null)
            return Task.CompletedTask;
        _loopTask = RunScanLoopAsync();
        return Task.CompletedTask;
    }

    private async Task RunScanLoopAsync()
    {
        using var timer = new PeriodicTimer(_scanInterval, _timeProvider);
        var token = _cts.Token;
        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                // 空闲时集合为空，跳过枚举近乎零开销。
                if (_activeAssemblies.IsEmpty)
                    continue;

                // 遍历活跃装配：对每个 Session 检查装配是否超时。
                // 超时则 Close（幂等）并直接移除条目——不依赖 Close→OnSessionClosed 回调链
                //（测试场景或未注入 tracker 的 Session 不会触发回调）。
                foreach (var kvp in _activeAssemblies)
                {
                    var state = kvp.Value;
                    var elapsed = _timeProvider.GetElapsedTime(state.AssemblyStartTimestamp);
                    if (elapsed >= state.CurrentPhaseTimeout)
                    {
                        kvp.Key.Close(SessionCloseReason.SlowFrameAssembly);
                        _activeAssemblies.TryRemove(kvp.Key, out _);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // 停机：正常退出。
        }
    }

    public async Task StopAsync()
    {
        if (_stopping)
            return;
        _stopping = true;

        await _cts.CancelAsync().ConfigureAwait(false);
        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.ConfigureAwait(false);
            }
            catch
            {
                // 扫描循环异常已被 catch，此处忽略。
            }
        }

        // 清空残留装配（连接 Close 路径会各自调用 OnSessionClosed，此处兜底）。
        _activeAssemblies.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts.Dispose();
    }
}

/// <summary>
/// 帧装配状态：记录装配起点时间戳与当前阶段超时时长。
/// <para>
/// 引用类型——每次 <see cref="FrameAssemblyTimeoutTracker.OnAssemblyStarted"/> 创建新实例，
/// <see cref="FrameAssemblyTimeoutTracker.OnAssemblyCompleted"/> 用引用相等性移除，
/// 天然解决阶段切换后的 ABA 问题，无需额外 epoch 计数器。
/// </para>
/// <para>
/// 不可变——所有字段 readonly，状态变更通过创建新实例 + CAS 替换完成。
/// </para>
/// </summary>
internal sealed class FrameAssemblyState
{
    /// <summary>装配开始时的单调时间戳（<see cref="TimeProvider.GetTimestamp"/>）。</summary>
    public readonly long AssemblyStartTimestamp;

    /// <summary>当前装配阶段的超时时长（HeaderAssemblyTimeout 或 PayloadAssemblyTimeout）。</summary>
    public readonly TimeSpan CurrentPhaseTimeout;

    public FrameAssemblyState(long assemblyStartTimestamp, TimeSpan currentPhaseTimeout)
    {
        AssemblyStartTimestamp = assemblyStartTimestamp;
        CurrentPhaseTimeout = currentPhaseTimeout;
    }
}
