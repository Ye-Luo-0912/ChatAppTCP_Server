using System.Collections.Concurrent;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 连接准入与过载保护跟踪器。
/// <para>
/// 跟踪未认证连接数、每 IP 并发连接数、每 IP 认证失败次数（滑动窗口）。
/// 在 <see cref="TcpGatewayService.StartClientAsync"/> 中调用 <see cref="TryAdmit"/> 决定是否接受新连接，
/// 在认证完成时调用 <see cref="MarkAuthenticated"/> 递减未认证计数，
/// 在认证失败时调用 <see cref="RecordAuthenticationFailure"/> 累计失败次数，
/// 在连接断开时调用 <see cref="Release"/> 释放占用的槽位。
/// </para>
/// <para>
/// 零计数 IP 键与空认证失败桶会在递减/清理时移除，避免公网 IP 基数导致字典无限增长。
/// </para>
/// </summary>
internal sealed class ConnectionAdmissionTracker
{
    private readonly int _maxUnauthenticatedConnections;
    private readonly int _maxConnectionsPerIp;
    private readonly int _maxAuthAttemptsPerIp;
    private readonly TimeSpan _authRateWindow;
    private long _unauthenticatedCount;
    private readonly ConcurrentDictionary<string, int> _connectionsPerIp = new();
    private readonly ConcurrentDictionary<string, AuthFailureBucket> _authFailuresPerIp = new();

    public ConnectionAdmissionTracker(
        int maxUnauthenticatedConnections,
        int maxConnectionsPerIp,
        int maxAuthAttemptsPerIp,
        TimeSpan authRateWindow)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxUnauthenticatedConnections);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxConnectionsPerIp);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAuthAttemptsPerIp);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(authRateWindow, TimeSpan.Zero);

        _maxUnauthenticatedConnections = maxUnauthenticatedConnections;
        _maxConnectionsPerIp = maxConnectionsPerIp;
        _maxAuthAttemptsPerIp = maxAuthAttemptsPerIp;
        _authRateWindow = authRateWindow;
    }

    /// <summary>
    /// 尝试准入新连接。
    /// <para>
    /// 原子递增未认证计数与每 IP 连接计数，若任一超限返回 false。
    /// 调用方应在返回 false 时立即断开连接并调用 <see cref="Release"/> 回滚已递增的计数。
    /// </para>
    /// </summary>
    public AdmissionResult TryAdmit(string remoteIp)
    {
        // 1. 检查未认证连接数。
        var unauth = Interlocked.Increment(ref _unauthenticatedCount);
        if (unauth > _maxUnauthenticatedConnections)
        {
            Interlocked.Decrement(ref _unauthenticatedCount);
            return AdmissionResult.RejectedUnauthenticatedLimit;
        }

        // 2. 检查每 IP 连接数。
        var perIp = _connectionsPerIp.AddOrUpdate(remoteIp, 1, static (_, c) => c + 1);
        if (perIp > _maxConnectionsPerIp)
        {
            // 回滚未认证计数和每 IP 计数。
            Interlocked.Decrement(ref _unauthenticatedCount);
            DecrementIp(remoteIp);
            return AdmissionResult.RejectedPerIpConnectionLimit;
        }

        // 3. 检查每 IP 认证失败次数（滑动窗口）。
        var failures = _authFailuresPerIp.GetOrAdd(
            remoteIp,
            static _ => new AuthFailureBucket());
        if (failures.GetCount(DateTimeOffset.UtcNow, _authRateWindow) >= _maxAuthAttemptsPerIp)
        {
            Interlocked.Decrement(ref _unauthenticatedCount);
            DecrementIp(remoteIp);
            TryRemoveEmptyAuthBucket(remoteIp, failures);
            return AdmissionResult.RejectedPerIpAuthRateLimit;
        }

        return AdmissionResult.Admitted;
    }

    /// <summary>
    /// 标记连接已认证成功，递减未认证计数。
    /// </summary>
    public void MarkAuthenticated()
    {
        Interlocked.Decrement(ref _unauthenticatedCount);
    }

    /// <summary>
    /// 记录一次认证失败（用于滑动窗口计数）。
    /// </summary>
    public void RecordAuthenticationFailure(string remoteIp)
    {
        var bucket = _authFailuresPerIp.GetOrAdd(
            remoteIp,
            static _ => new AuthFailureBucket());
        bucket.RecordFailure(DateTimeOffset.UtcNow, _authRateWindow);
    }

    /// <summary>
    /// 释放连接占用的槽位（连接断开时调用）。
    /// <para>
    /// 仅递减每 IP 连接计数；未认证计数由 <see cref="MarkAuthenticated"/> 或
    /// <see cref="ReleaseUnauthenticated"/> 处理。
    /// </para>
    /// </summary>
    public void Release(string remoteIp, bool wasAuthenticated)
    {
        DecrementIp(remoteIp);
        if (!wasAuthenticated)
        {
            Interlocked.Decrement(ref _unauthenticatedCount);
        }
    }

    /// <summary>
    /// 仅释放未认证计数（用于 TryAdmit 返回 false 后回滚已递增的计数，
    /// 或连接在认证前断开时调用）。
    /// </summary>
    public void ReleaseUnauthenticated()
    {
        Interlocked.Decrement(ref _unauthenticatedCount);
    }

    /// <summary>
    /// 清理已过期的认证失败桶与零计数 IP 条目，防止公网长期运行时字典膨胀。
    /// </summary>
    public void SweepExpiredEntries(DateTimeOffset now)
    {
        foreach (var pair in _authFailuresPerIp)
        {
            if (pair.Value.PruneAndIsEmpty(now, _authRateWindow))
                _authFailuresPerIp.TryRemove(new KeyValuePair<string, AuthFailureBucket>(pair.Key, pair.Value));
        }

        foreach (var pair in _connectionsPerIp)
        {
            if (pair.Value == 0)
                _connectionsPerIp.TryRemove(new KeyValuePair<string, int>(pair.Key, 0));
        }
    }

    public long CurrentUnauthenticatedCount => Volatile.Read(ref _unauthenticatedCount);
    public int CurrentConnectionsForIp(string remoteIp) =>
        _connectionsPerIp.TryGetValue(remoteIp, out var c) ? c : 0;

    /// <summary>测试/诊断：当前跟踪的 IP 连接数字典条目数。</summary>
    internal int TrackedIpCount => _connectionsPerIp.Count;

    /// <summary>测试/诊断：当前跟踪的认证失败桶条目数。</summary>
    internal int TrackedAuthFailureBucketCount => _authFailuresPerIp.Count;

    private void DecrementIp(string remoteIp)
    {
        while (true)
        {
            if (!_connectionsPerIp.TryGetValue(remoteIp, out var current))
                return;

            if (current <= 1)
            {
                if (_connectionsPerIp.TryRemove(
                        new KeyValuePair<string, int>(remoteIp, current)))
                    return;

                continue;
            }

            if (_connectionsPerIp.TryUpdate(remoteIp, current - 1, current))
                return;
        }
    }

    private void TryRemoveEmptyAuthBucket(string remoteIp, AuthFailureBucket bucket)
    {
        if (bucket.PruneAndIsEmpty(DateTimeOffset.UtcNow, _authRateWindow))
            _authFailuresPerIp.TryRemove(new KeyValuePair<string, AuthFailureBucket>(remoteIp, bucket));
    }

    /// <summary>
    /// 每 IP 认证失败滑动窗口桶。
    /// <para>
    /// 简化实现：用链表存储失败时间戳，查询时清理过期项。
    /// 适用于低频认证失败场景；高频场景可改为环形缓冲。
    /// </para>
    /// </summary>
    private sealed class AuthFailureBucket
    {
        private readonly Lock _gate = new();
        private readonly LinkedList<DateTimeOffset> _failures = new();

        public void RecordFailure(DateTimeOffset now, TimeSpan window)
        {
            lock (_gate)
            {
                PruneLocked(now, window);
                _failures.AddLast(now);
            }
        }

        public int GetCount(DateTimeOffset now, TimeSpan window)
        {
            lock (_gate)
            {
                PruneLocked(now, window);
                return _failures.Count;
            }
        }

        public bool PruneAndIsEmpty(DateTimeOffset now, TimeSpan window)
        {
            lock (_gate)
            {
                PruneLocked(now, window);
                return _failures.Count == 0;
            }
        }

        private void PruneLocked(DateTimeOffset now, TimeSpan window)
        {
            var cutoff = now - window;
            while (_failures.Count > 0 && _failures.First!.Value < cutoff)
            {
                _failures.RemoveFirst();
            }
        }
    }
}

/// <summary>连接准入结果。</summary>
internal enum AdmissionResult
{
    /// <summary>准入成功。</summary>
    Admitted = 0,

    /// <summary>拒绝：未认证连接数超限。</summary>
    RejectedUnauthenticatedLimit = 1,

    /// <summary>拒绝：单 IP 并发连接数超限。</summary>
    RejectedPerIpConnectionLimit = 2,

    /// <summary>拒绝：单 IP 认证失败次数超限。</summary>
    RejectedPerIpAuthRateLimit = 3
}
