using System.Buffers;
using ChatApp.Binary.Core;
using ChatApp.Shared.Protocol.Tcp;
using ChatApp.Shared.Protocol.Tcp.Binary.Schemas;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Gateway.Serialization;

/// <summary>
/// 二进制 payload 解码失败的稳定异常。与 JSON 路径的 <see cref="System.Text.Json.JsonException"/>
/// 同属"恶意/损坏 payload"错误通道：Inline lane 捕获后按协议错误关闭连接，
/// 排队 lane 沿既有 handler 异常兜底处理。<see cref="SchemaNotCovered"/> 为 true 表示
/// 命令不在二进制目录内（协商后不应出现），一律按协议违例 fail-closed。
/// </summary>
public sealed class BinaryPayloadDecodeException : Exception
{
    public BinaryPayloadDecodeException(PacketCommand command, BinaryStatus status, bool schemaNotCovered)
        : base($"binary payload decode failed for {command}: {status}")
    {
        Command = command;
        Status = status;
        SchemaNotCovered = schemaNotCovered;
    }

    public PacketCommand Command { get; }

    public BinaryStatus Status { get; }

    public bool SchemaNotCovered { get; }
}

/// <summary>
/// 二进制 codec 适配层：把 <see cref="TcpBinaryWireCodec"/> 的稳定 outcome 翻译为
/// Gateway 既有错误通道（成功 → DTO；失败 → <see cref="BinaryPayloadDecodeException"/>）。
/// </summary>
internal static class TcpBinaryPayloadCodec
{
    /// <summary>
    /// 按命令分发到二进制 schema 解码。未覆盖命令与畸形/超限 payload fail-closed 抛出；
    /// 寄存器对每个命令只产出唯一 DTO 类型，类型不匹配属编程错误直接抛 InvalidCastException。
    /// </summary>
    public static T? Deserialize<T>(PacketCommand command, in ReadOnlySequence<byte> payload)
        where T : class
    {
        var decode = TcpBinaryWireCodec.TryDecode(command, payload, BinaryLimits.Default);
        switch (decode.Status)
        {
            case TcpBinaryWireStatus.Decoded:
                return (T)decode.Value!;
            case TcpBinaryWireStatus.DecodeFailure:
                throw new BinaryPayloadDecodeException(command, decode.DecodeStatus, schemaNotCovered: false);
            default:
                throw new BinaryPayloadDecodeException(
                    command,
                    BinaryStatus.UnsupportedWireType,
                    schemaNotCovered: true);
        }
    }
}

/// <summary>
/// 连接级 payload 格式分流的唯一入站入口：JSON 会话沿用 handler 注入的
/// <see cref="IPayloadCodec{T}"/>，二进制会话走 <see cref="TcpBinaryPayloadCodec"/>。
/// 格式在握手完成时固定且不可变，因此这里按 session 读取无需同步。
/// <para>
/// 二进制寄存器只产出共享规范 DTO（object），必须经 <see cref="BinaryPayloadMapper.ToLocal{T}"/>
/// 转回网关本地 DTO 再交给 handler——两种格式下 handler 看到的是同一本地类型，
/// 业务代码无需感知载荷格式。
/// </para>
/// </summary>
internal static class SessionPayload
{
    public static T? Deserialize<T>(
        TcpClientSession session,
        PacketCommand command,
        IPayloadCodec<T> jsonCodec,
        in ReadOnlySequence<byte> payload)
        where T : class
    {
        if (session.NegotiatedPayloadFormat != PayloadFormat.Binary)
        {
            return jsonCodec.Deserialize(payload);
        }

        var shared = TcpBinaryPayloadCodec.Deserialize<object>(command, payload);
        return BinaryPayloadMapper.ToLocal<T>(command, shared);
    }
}
