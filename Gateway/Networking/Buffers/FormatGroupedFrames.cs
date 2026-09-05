using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Gateway.Networking.Buffers;

/// <summary>
/// fanout 按格式分组共享帧：JSON / binary 各至多编码一次，目标会话按其协商格式
/// 取对应共享帧入队（TryQueue 内部 retain，Dispose 释放生产者引用）。
/// 两种帧都惰性创建——纯 JSON 部署不付二进制编码成本，反之亦然；
/// 混合 fanout 时每种格式仍只编码一次，禁止逐 session 序列化。
/// </summary>
internal struct FormatGroupedFrame<T> : IDisposable
    where T : class
{
    private readonly PacketCommand _command;
    private readonly IPayloadCodec<T> _jsonCodec;
    private readonly T _value;
    private SharedOutboundFrame? _jsonFrame;
    private SharedOutboundFrame? _binaryFrame;

    public FormatGroupedFrame(PacketCommand command, IPayloadCodec<T> jsonCodec, T value)
    {
        _command = command;
        _jsonCodec = jsonCodec;
        _value = value;
    }

    public SharedOutboundFrame GetFrame(TcpClientSession target)
    {
        if (target.NegotiatedPayloadFormat == PayloadFormat.Binary)
        {
            _binaryFrame ??= OutboundFrameFactory.CreateBinary(_command, _value);
            return _binaryFrame;
        }

        _jsonFrame ??= OutboundFrameFactory.Create(_command, _jsonCodec, _value);
        return _jsonFrame;
    }

    public void Dispose()
    {
        _jsonFrame?.Dispose();
        _binaryFrame?.Dispose();
        _jsonFrame = null;
        _binaryFrame = null;
    }
}
