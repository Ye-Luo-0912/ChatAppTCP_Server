using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Networking.Ephemeral;
using ChatApp.TcpGateway.Gateway.Networking.Executor;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 单个 TCP session 在 OrderedWrite、Query、Ephemeral 三条 lane 上的注册租约集合。
/// 注册按顺序完成，任一 lane 冲突时只回滚本次已经取得的租约；释放按 holder/generation
/// 条件执行，因此旧 session 的重复 finally 不会删除 connectionId 相同的后继注册。
/// </summary>
internal readonly struct SessionCommandRegistrationSet
{
    private readonly SessionCommandExecutor.Registration _orderedWrite;
    private readonly SessionCommandExecutor.Registration _query;
    private readonly EphemeralCommandPipeline.Registration _ephemeral;

    private SessionCommandRegistrationSet(
        in SessionCommandExecutor.Registration orderedWrite,
        in SessionCommandExecutor.Registration query,
        in EphemeralCommandPipeline.Registration ephemeral)
    {
        _orderedWrite = orderedWrite;
        _query = query;
        _ephemeral = ephemeral;
    }

    public bool IsComplete =>
        _orderedWrite.IsValid &&
        _query.IsValid &&
        _ephemeral.IsValid;

    public static bool TryRegister(
        uint connectionId,
        long userId,
        SessionCommandExecutor orderedWriteExecutor,
        SessionCommandExecutor queryExecutor,
        EphemeralCommandPipeline ephemeralPipeline,
        out SessionCommandRegistrationSet registrations)
    {
        registrations = default;
        if (!orderedWriteExecutor.TryRegisterConnection(
                connectionId,
                userId,
                out var orderedWrite))
        {
            return false;
        }

        if (!queryExecutor.TryRegisterConnection(
                connectionId,
                userId,
                out var query))
        {
            orderedWrite.Unregister();
            return false;
        }

        if (!ephemeralPipeline.TryRegisterConnection(
                connectionId,
                userId,
                out var ephemeral))
        {
            query.Unregister();
            orderedWrite.Unregister();
            return false;
        }

        registrations = new SessionCommandRegistrationSet(
            in orderedWrite,
            in query,
            in ephemeral);
        return true;
    }

    public bool TryEnqueue(
        CommandLane lane,
        in SessionCommand command)
        => lane switch
        {
            CommandLane.Query => _query.TryEnqueue(in command),
            CommandLane.Ephemeral => _ephemeral.TryEnqueue(in command),
            _ => _orderedWrite.TryEnqueue(in command)
        };

    public void Unregister()
    {
        // 逆注册顺序释放。每个 lease 自身做 owner + expected holder/generation 校验，
        // default、重复释放及旧 session 延迟清理都是安全 no-op。
        _ephemeral.Unregister();
        _query.Unregister();
        _orderedWrite.Unregister();
    }
}
