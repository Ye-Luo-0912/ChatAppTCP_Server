using System.Collections.Frozen;
using ChatApp.Realtime.Abstractions.Events;

namespace ChatApp.TcpGateway.Gateway.Messaging.Realtime;

/// <summary>
/// 显式注册的 <see cref="RealtimeEventType"/> -&gt; <see cref="IRealtimeEventHandler"/> 查找表。
/// <para>
/// 不使用 attribute 扫描或反射发现：注册在 DI 扩展方法中以普通 <c>Add(type, handler)</c> 完成，
/// 与 <c>CommandDispatcher</c> 的注册风格一致。
/// </para>
/// <para>
/// 同一类型多次注册以最后一次为准（便于测试覆盖）。查找返回 false 时由调用方走"不支持事件"路径。
/// </para>
/// </summary>
internal sealed class RealtimeEventHandlerRegistry
{
    private readonly FrozenDictionary<RealtimeEventType, IRealtimeEventHandler> _handlers;

    public RealtimeEventHandlerRegistry(
        IEnumerable<KeyValuePair<RealtimeEventType, IRealtimeEventHandler>> registrations)
    {
        // 重复注册以最后一次为准，便于测试覆盖或运行时覆盖。
        var builder = new Dictionary<RealtimeEventType, IRealtimeEventHandler>();
        foreach (var pair in registrations)
            builder[pair.Key] = pair.Value;
        _handlers = builder.ToFrozenDictionary();
    }

    public bool TryGet(RealtimeEventType type, out IRealtimeEventHandler handler) =>
        _handlers.TryGetValue(type, out handler!);
}
