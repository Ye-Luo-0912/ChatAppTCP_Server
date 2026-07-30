using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Observability.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.TcpGateway.Infrastructure.Authentication;

/// <summary>
/// Redis/Garnet 设备租约：key = tcp:devlease:{userId}:{deviceIdHash} → value = leaseOwnerToken\ntransportId\nsessionId。
/// <para>
/// 可选注入 <see cref="IRedisCircuitBreaker"/>：Redis 故障期间快速失败，避免心跳刷新与
/// TakeOver 串行触发超时。未注入时跳过熔断器逻辑（兼容旧测试）。
/// </para>
/// </summary>
/// <remarks>
/// P1-A2：租约值拆分为三字段：
/// <list type="bullet">
/// <item><c>leaseOwnerToken</c>（私有所有权凭证，仅用于 Redis CAS）</item>
/// <item><c>transportId</c>（公开路由标识，用于跨 Gateway 吊销匹配）</item>
/// <item><c>sessionId</c>（用户可见会话标识）</item>
/// </list>
/// <para>
/// 向后兼容：旧值格式为 <c>connectionLeaseId\nsessionId</c>（2 字段）。
/// Lua 脚本检测字段数并按相应格式解析——旧值的 <c>connectionLeaseId</c> 同时承担
/// LeaseOwnerToken 与 TransportId 角色（旧实现未分离）。
/// </para>
/// </remarks>
internal sealed class RedisDeviceSessionLeaseStore : IDeviceSessionLeaseStore
{
    private const string KeyPrefix = "tcp:devlease:";
    private readonly RedisConnectionProvider _connectionProvider;
    private readonly ILogger<RedisDeviceSessionLeaseStore> _logger;
    private readonly IRedisCircuitBreaker? _circuitBreaker;

    public RedisDeviceSessionLeaseStore(
        RedisConnectionProvider connectionProvider,
        ILogger<RedisDeviceSessionLeaseStore> logger,
        IRedisCircuitBreaker? circuitBreaker = null)
    {
        _connectionProvider = connectionProvider;
        _logger = logger;
        _circuitBreaker = circuitBreaker;
    }

    // 值格式（P1-A2）：leaseOwnerToken\ntransportId\nsessionId
    // 向后兼容旧值：connectionLeaseId\nsessionId（2 字段，旧实现未分离 secret/route）
    //
    // TakeOver：GET old; SET new with TTL;
    //   - 旧值 2 字段：prevLeaseOwnerToken = prevTransportId = field[0], prevSession = field[1]
    //   - 新值 3 字段：prevLeaseOwnerToken = field[0], prevTransportId = field[1], prevSession = field[2]
    //   返回 "prevTransportId\nprevSession" if prevLeaseOwnerToken != @leaseOwnerToken (else empty string).
    //   仅返回可广播的 TransportId，不返回私有 LeaseOwnerToken。
    private static readonly LuaScript TakeOverScript = LuaScript.Prepare(
        """
        local previous = redis.call('GET', @key)
        local newvalue = @leaseOwnerToken .. '\n' .. @transportId .. '\n' .. @sessionId
        redis.call('SET', @key, newvalue, 'PX', tonumber(@ttlMs))
        if previous and previous ~= false then
          local firstSep = string.find(previous, '\n')
          if firstSep then
            local secondSep = string.find(previous, '\n', firstSep + 1)
            local prevLeaseOwnerToken
            local prevTransportId
            local prevSession
            if secondSep then
              -- 新格式 3 字段：leaseOwnerToken\ntransportId\nsessionId
              prevLeaseOwnerToken = string.sub(previous, 1, firstSep - 1)
              prevTransportId = string.sub(previous, firstSep + 1, secondSep - 1)
              prevSession = string.sub(previous, secondSep + 1)
            else
              -- 旧格式 2 字段：connectionLeaseId\nsessionId（connectionLeaseId 同时承担两个角色）
              prevLeaseOwnerToken = string.sub(previous, 1, firstSep - 1)
              prevTransportId = prevLeaseOwnerToken
              prevSession = string.sub(previous, firstSep + 1)
            end
            if prevLeaseOwnerToken ~= @leaseOwnerToken then
              return prevTransportId .. '\n' .. prevSession
            end
          else
            -- 极旧格式：单字段 connectionLeaseId（无 sessionId）
            return previous .. '\n'
          end
        end
        return ''
        """);

    // DEL only if leaseOwnerToken matches.
    // 兼容旧值：2 字段时 field[0] 同时是 leaseOwnerToken。
    private static readonly LuaScript ReleaseIfOwnerScript = LuaScript.Prepare(
        """
        local current = redis.call('GET', @key)
        if current then
          local firstSep = string.find(current, '\n')
          local leaseOwnerToken
          if firstSep then
            leaseOwnerToken = string.sub(current, 1, firstSep - 1)
          else
            leaseOwnerToken = current
          end
          if leaseOwnerToken == @leaseOwnerToken then
            return redis.call('DEL', @key)
          end
        end
        return 0
        """);

    // PEXPIRE only if leaseOwnerToken matches.
    // 兼容旧值：2 字段时 field[0] 同时是 leaseOwnerToken。
    private static readonly LuaScript RefreshIfOwnerScript = LuaScript.Prepare(
        """
        local current = redis.call('GET', @key)
        if current then
          local firstSep = string.find(current, '\n')
          local leaseOwnerToken
          if firstSep then
            leaseOwnerToken = string.sub(current, 1, firstSep - 1)
          else
            leaseOwnerToken = current
          end
          if leaseOwnerToken == @leaseOwnerToken then
            redis.call('PEXPIRE', @key, tonumber(@ttlMs))
            return 1
          end
        end
        return 0
        """);

    public async ValueTask<TakeOverResult> TakeOverAsync(
        long userId,
        ulong deviceIdHash,
        string sessionId,
        string transportId,
        string leaseOwnerToken,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        ArgumentException.ThrowIfNullOrWhiteSpace(transportId);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseOwnerToken);
        if (ttl <= TimeSpan.Zero)
            ttl = TimeSpan.FromHours(24);

        // 熔断器开路时返回 DependencyUnavailable 而非抛异常——
        // 调用方据此 fail-closed（拒绝 Resume、回滚本地状态、要求完整认证）。
        // Same-device fencing 的接管侧属于安全不变量。
        // 与 GetCurrentSessionIdAsync 的熔断器处理对齐。
        if (_circuitBreaker is { IsAvailable: false })
            return TakeOverResult.Unavailable(new RedisException("Redis circuit breaker is open"));

        var key = CreateKey(userId, deviceIdHash);
        var ttlMs = (long)Math.Clamp(ttl.TotalMilliseconds, 1_000, 7 * 24 * 60 * 60 * 1000d);

        try
        {
            var result = await TakeOverScript
                .EvaluateAsync(
                    _connectionProvider.Database,
                    new { key = (RedisKey)key, sessionId, transportId, leaseOwnerToken, ttlMs })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();

            if (result.IsNull)
                return TakeOverResult.NoPreviousLease();

            var previous = (string?)result;
            if (string.IsNullOrWhiteSpace(previous))
                return TakeOverResult.NoPreviousLease();

            // Lua 返回格式：prevTransportId\nprevSession（仅可广播字段，不含私有 LeaseOwnerToken）
            // 解析出旧 TransportId 和旧 SessionId，供调用方按 transport 精确匹配。
            var sepIndex = previous.IndexOf('\n');
            string? prevTransportId;
            string? prevSession;
            if (sepIndex < 0)
            {
                prevTransportId = previous;
                prevSession = null;
            }
            else
            {
                prevTransportId = sepIndex > 0 ? previous[..sepIndex] : null;
                prevSession = sepIndex < previous.Length - 1
                    ? previous[(sepIndex + 1)..]
                    : null;
            }

            prevTransportId = string.IsNullOrWhiteSpace(prevTransportId) ? null : prevTransportId;
            prevSession = string.IsNullOrWhiteSpace(prevSession) ? null : prevSession;

            if (prevTransportId is null && prevSession is null)
                return TakeOverResult.NoPreviousLease();

            return TakeOverResult.Success(prevSession, prevTransportId);
        }
        catch (RedisException exception)
        {
            _circuitBreaker?.RecordFailure();
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseTakeOver,
                exception);
            // 返回 DependencyUnavailable 让 Coordinator fail-closed。
            // 旧实现抛异常导致调用方必须 try/catch；新接口将三态显式化，调用方按 Status 分支即可。
            return TakeOverResult.Unavailable(exception);
        }
    }

    public async ValueTask ReleaseIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string leaseOwnerToken,
        CancellationToken cancellationToken)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(leaseOwnerToken))
            return;

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：跳过 Release，依赖租约 TTL 自然失效。
            return;
        }

        var key = CreateKey(userId, deviceIdHash);
        try
        {
            await ReleaseIfOwnerScript
                .EvaluateAsync(
                    _connectionProvider.Database,
                    new { key = (RedisKey)key, leaseOwnerToken })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();
        }
        catch (RedisException exception)
        {
            _circuitBreaker?.RecordFailure();
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseRelease,
                exception);
        }
    }

    public async ValueTask<bool> RefreshIfOwnerAsync(
        long userId,
        ulong deviceIdHash,
        string leaseOwnerToken,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        if (userId <= 0 || string.IsNullOrWhiteSpace(leaseOwnerToken))
            return false;
        if (ttl <= TimeSpan.Zero)
            return false;

        if (_circuitBreaker is { IsAvailable: false })
        {
            // 熔断器开路：刷新失败但不关闭连接（与原 RedisException 路径一致）。
            return false;
        }

        var key = CreateKey(userId, deviceIdHash);
        var ttlMs = (long)Math.Clamp(ttl.TotalMilliseconds, 1_000, 7 * 24 * 60 * 60 * 1000d);

        try
        {
            var result = await RefreshIfOwnerScript
                .EvaluateAsync(
                    _connectionProvider.Database,
                    new { key = (RedisKey)key, leaseOwnerToken, ttlMs })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();

            return result.IsNull ? false : (long)result == 1;
        }
        catch (RedisException exception)
        {
            _circuitBreaker?.RecordFailure();
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseRefresh,
                exception);
            return false;
        }
    }

    public async ValueTask<string?> GetCurrentSessionIdAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        if (userId <= 0)
            return null;

        // 熔断器开路时抛异常而非返回 null——返回 null 会让 Coordinator 误判为"无租约"
        // 并放行 Resume（fail-open）。Same-device fencing 属于安全不变量，必须 fail-closed。
        // Coordinator 在 TryResumeAsync 入口已有 breaker 检查（CircuitOpen 路径），
        // 此处仅为 race-condition 兜底（breaker 在 Coordinator 检查与 lease 查询之间开路）。
        if (_circuitBreaker is { IsAvailable: false })
            throw new RedisException("Redis circuit breaker is open");

        var key = CreateKey(userId, deviceIdHash);
        try
        {
            var current = await _connectionProvider.Database
                .StringGetAsync(key)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            _circuitBreaker?.RecordSuccess();

            if (current.IsNullOrEmpty)
                return null;

            var value = (string)current!;
            var firstSep = value.IndexOf('\n');
            if (firstSep < 0)
                return value; // 极旧格式：单字段 connectionLeaseId（视为 sessionId）

            var secondSep = value.IndexOf('\n', firstSep + 1);
            // 值格式：leaseOwnerToken\ntransportId\nsessionId（3 字段）或
            //       connectionLeaseId\nsessionId（2 字段，向后兼容）
            // 仅返回 sessionId 部分（最后一字段）。
            return secondSep >= 0
                ? value[(secondSep + 1)..]
                : value[(firstSep + 1)..];
        }
        catch (RedisException exception)
        {
            _circuitBreaker?.RecordFailure();
            _logger.DependencyOperationFailed(
                GatewayDependency.Redis,
                GatewayDependencyOperation.DeviceLeaseQuery,
                exception);
            // 抛异常让 Coordinator fail-closed（拒绝 Resume，要求完整认证）。
            // 旧实现返回 null 导致 Redis 故障期间旧 Token 绕过设备租约 fencing 校验。
            throw;
        }
    }

    private static string CreateKey(long userId, ulong deviceIdHash) =>
        string.Concat(
            KeyPrefix,
            userId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ":",
            deviceIdHash.ToString(System.Globalization.CultureInfo.InvariantCulture));
}
