using System.Collections.Concurrent;

namespace ChatApp.TcpGateway.Networking.Sessions;

internal sealed class UserSessionRegistry
{
    private readonly ConcurrentDictionary<long, SessionBucket> _users = new();

    public void Add(TcpClientSession session)
    {
        if (!session.IsAuthenticated || session.UserId == 0)
        {
            return;
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

                bucket.Sessions[session.ConnectionId] = session;
                Volatile.Write(
                    ref bucket.Snapshot,
                    [.. bucket.Sessions.Values]);
                return;
            }
        }
    }

    public void Remove(TcpClientSession session)
    {
        if (session.UserId == 0 ||
            !_users.TryGetValue(session.UserId, out var bucket))
        {
            return;
        }

        lock (bucket.SyncRoot)
        {
            bucket.Sessions.Remove(session.ConnectionId);
            if (bucket.Sessions.Count != 0)
            {
                Volatile.Write(
                    ref bucket.Snapshot,
                    [.. bucket.Sessions.Values]);
                return;
            }

            bucket.Retired = true;
            Volatile.Write(ref bucket.Snapshot, []);

            if (_users.TryGetValue(session.UserId, out var current) &&
                ReferenceEquals(current, bucket))
            {
                _users.TryRemove(session.UserId, out _);
            }
        }
    }

    public TcpClientSession[] GetSnapshot(long userId)
    {
        if (!_users.TryGetValue(userId, out var bucket))
        {
            return [];
        }

        return Volatile.Read(ref bucket.Snapshot);
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

            List<TcpClientSession>? victims = null;
            foreach (var existing in bucket.Sessions.Values)
            {
                if (ReferenceEquals(existing, incoming)
                    || existing.ConnectionId == incoming.ConnectionId)
                {
                    continue;
                }

                if (existing.DeviceIdHash != incoming.DeviceIdHash)
                    continue;

                if (string.Equals(
                        existing.SessionId,
                        incoming.SessionId,
                        StringComparison.Ordinal))
                {
                    continue;
                }

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
