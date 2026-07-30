using System.Collections.Concurrent;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

internal sealed class UserSessionRegistry
{
    private readonly ConcurrentDictionary<long, SessionBucket> _users = new();

    /// <summary>
    /// 加入会话。返回 true 表示该用户从离线变为首次在线（本机视角）。
    /// </summary>
    public bool Add(TcpClientSession session)
    {
        if (!session.IsAuthenticated || session.UserId == 0)
        {
            return false;
        }

        while (true)
        {
            var bucket = _users.GetOrAdd(
                session.UserId,
                static _ => new SessionBucket());

            lock (bucket.SyncRoot)
            {
                if (bucket.Retired)
                {
                    continue;
                }

                var becameOnline = bucket.Sessions.Count == 0;
                bucket.Sessions[session.ConnectionId] = session;
                Volatile.Write(
                    ref bucket.Snapshot,
                    [.. bucket.Sessions.Values]);
                return becameOnline;
            }
        }
    }

    /// <summary>
    /// 移除会话。返回 true 表示该用户已无剩余会话（本机视角最后下线）。
    /// </summary>
    public bool Remove(TcpClientSession session)
    {
        if (session.UserId == 0 ||
            !_users.TryGetValue(session.UserId, out var bucket))
        {
            return false;
        }

        lock (bucket.SyncRoot)
        {
            if (!bucket.Sessions.Remove(session.ConnectionId))
                return false;

            if (bucket.Sessions.Count != 0)
            {
                Volatile.Write(
                    ref bucket.Snapshot,
                    [.. bucket.Sessions.Values]);
                return false;
            }

            bucket.Retired = true;
            Volatile.Write(ref bucket.Snapshot, []);

            if (_users.TryGetValue(session.UserId, out var current) &&
                ReferenceEquals(current, bucket))
            {
                _users.TryRemove(session.UserId, out _);
            }

            return true;
        }
    }

    public TcpClientSession[] GetSnapshot(long userId)
    {
        return !_users.TryGetValue(userId, out var bucket) ? [] : Volatile.Read(ref bucket.Snapshot);
    }

    /// <summary>
    /// 同设备重复登录：返回应被踢下线的旧会话（不含 <paramref name="incoming"/>）。
    /// 不同 DeviceIdHash 的多设备会话保留。
    /// </summary>
    public TcpClientSession[] TakeOverSameDevice(
        TcpClientSession incoming)
    {
        if (!incoming.IsAuthenticated
            || incoming.UserId == 0
            || incoming.DeviceIdHash is null
            || !_users.TryGetValue(incoming.UserId, out var bucket))
        {
            return [];
        }

        lock (bucket.SyncRoot)
        {
            if (bucket.Retired)
                return [];

            // 锁内 LINQ 改普通循环，避免闭包和枚举器分配。
            List<TcpClientSession>? victims = null;
            foreach (var existing in bucket.Sessions.Values)
            {
                if (ReferenceEquals(existing, incoming))
                    continue;
                if (existing.ConnectionId == incoming.ConnectionId)
                    continue;
                if (existing.DeviceIdHash != incoming.DeviceIdHash)
                    continue;
                if (string.Equals(
                        // P0-5: 按 ConnectionLeaseId 区分新旧物理连接，而非 SessionId。
                        // Resume 复用原 SessionId，若按 SessionId 跳过会让同 Gateway 旧 Transport
                        // 漏过本机接管，退化为依赖 NATS SessionRevoked 往返才能关闭。
                        // 不同 ConnectionLeaseId 即不同物理连接，应作为本机 victim 立即关闭。
                        existing.ConnectionLeaseId,
                        incoming.ConnectionLeaseId,
                        StringComparison.Ordinal))
                    continue;

                victims ??= [];
                victims.Add(existing);
            }

            return victims is null ? [] : [.. victims];
        }
    }

    private sealed class SessionBucket
    {
        public object SyncRoot { get; } = new();

        public Dictionary<uint, TcpClientSession> Sessions { get; } = [];

        public TcpClientSession[] Snapshot = [];

        public bool Retired { get; set; }
    }
}
