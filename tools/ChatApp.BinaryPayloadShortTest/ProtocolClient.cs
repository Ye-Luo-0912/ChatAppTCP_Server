using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using ChatApp.Binary.Core;
using ChatApp.Shared.Protocol.Tcp.Binary;
using ChatApp.Shared.Protocol.Tcp.Binary.Schemas;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using LocalMessageAcknowledgement = ChatApp.TcpGateway.Core.Messaging.MessageAcknowledgement;

namespace ChatApp.BinaryPayloadShortTest;

/// <summary>共享包 wire codec 与网关 JSON codec 的集中持有（均为无状态实例）。</summary>
internal static class WireCodecs
{
    public static readonly JsonPayloadCodec<ClientHello> ClientHello =
        new(GatewayJsonSerializerContext.Default.ClientHello);

    public static readonly JsonPayloadCodec<ServerHello> ServerHello =
        new(GatewayJsonSerializerContext.Default.ServerHello);

    public static readonly JsonPayloadCodec<AuthenticationRequest> AuthenticationRequest =
        new(GatewayJsonSerializerContext.Default.AuthenticationRequest);

    public static readonly JsonPayloadCodec<AuthenticationResponse> AuthenticationResponse =
        new(GatewayJsonSerializerContext.Default.AuthenticationResponse);

    public static readonly JsonPayloadCodec<LocalMessageAcknowledgement> MessageAcknowledgement =
        new(GatewayJsonSerializerContext.Default.MessageAcknowledgement);

    public static readonly JsonPayloadCodec<ChatApp.TcpGateway.Core.Messaging.ChatMessage> ChatMessage =
        new(GatewayJsonSerializerContext.Default.ChatMessage);
}

/// <summary>固定容量的 <see cref="IBufferWriter{Byte}"/>：复用缓冲、避免 JSON 热路径每消息分配。</summary>
internal sealed class FixedBufferWriter(byte[] buffer) : IBufferWriter<byte>
{
    public int WrittenCount { get; private set; }

    public byte[] Buffer => buffer;

    public void Reset() => WrittenCount = 0;

    public void Advance(int count)
    {
        Guard.Ensure(count >= 0 && WrittenCount + count <= buffer.Length, "fixed buffer writer over-advance.");
        WrittenCount += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0) => buffer.AsMemory(WrittenCount).Slice(0, Required(sizeHint));

    public Span<byte> GetSpan(int sizeHint = 0) => buffer.AsSpan(WrittenCount)[..Required(sizeHint)];

    private int Required(int sizeHint)
    {
        var requested = sizeHint < 1 ? 1 : sizeHint;
        Guard.Ensure(
            WrittenCount + requested <= buffer.Length,
            $"fixed buffer overflow: need {requested}, have {buffer.Length - WrittenCount}.");
        return requested;
    }
}

internal readonly record struct ReceivedFrame(PacketCommand Command, byte[] Payload);

/// <summary>
/// 单条客户端连接：真实 socket + 真实帧协议。握手段恒 JSON（与测试组装一致），
/// 会话负载按协商格式走 JSON 或 chatapp-bin-v1 二进制。
/// </summary>
internal sealed class ProtocolClient : IAsyncDisposable
{
    public const long SenderUserId = 42;
    public const long ReceiverUserId = 42;

    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;

    private ProtocolClient(TcpClient tcp, NetworkStream stream, WireFormat negotiated)
    {
        _tcp = tcp;
        _stream = stream;
        Negotiated = negotiated;
    }

    public WireFormat Negotiated { get; }

    public static async Task<ProtocolClient> ConnectAndAuthenticateAsync(
        int port,
        WireFormat requested,
        ulong deviceIdHash,
        CancellationToken cancellationToken)
    {
        var tcp = new TcpClient();
        try
        {
            tcp.NoDelay = true;
            await tcp.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            var client = new ProtocolClient(tcp, tcp.GetStream(), requested);
            await client.HandshakeAsync(requested, cancellationToken);
            await client.AuthenticateAsync(deviceHash: deviceIdHash, cancellationToken);
            return client;
        }
        catch
        {
            tcp.Dispose();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stream.Dispose();
        _tcp.Dispose();
        await Task.CompletedTask;
    }

    public async Task WriteFrameAsync(
        PacketCommand command,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        var frame = new byte[PacketProtocol.HeaderSize + payload.Length];
        PacketParser.WriteHeader(frame, command, payload.Length);
        payload.Span.CopyTo(frame.AsSpan(PacketProtocol.HeaderSize));
        await _stream.WriteAsync(frame.AsMemory(), cancellationToken);
    }

    public async Task<ReceivedFrame> ReadFrameAsync(CancellationToken cancellationToken)
    {
        var header = new byte[PacketProtocol.HeaderSize];
        await _stream.ReadExactlyAsync(header.AsMemory(), cancellationToken);
        Guard.Ensure(
            BinaryPrimitives.ReadUInt32LittleEndian(header) == PacketProtocol.MagicNumber,
            "invalid packet magic from server.");

        var command = (PacketCommand)BinaryPrimitives.ReadUInt16LittleEndian(
            header.AsSpan(PacketProtocol.CommandOffset));
        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            header.AsSpan(PacketProtocol.LengthOffset));
        Guard.Ensure(
            payloadLength >= 0 && payloadLength <= PacketProtocol.MaxPayloadSize,
            $"invalid payload length {payloadLength} from server.");

        var payload = new byte[payloadLength];
        if (payloadLength != 0)
        {
            await _stream.ReadExactlyAsync(payload.AsMemory(), cancellationToken);
        }

        return new ReceivedFrame(command, payload);
    }

    private async Task HandshakeAsync(WireFormat requested, CancellationToken cancellationToken)
    {
        var featureBits = requested == WireFormat.Binary
            ? (uint)(GatewayFeature.CommandCapabilities | GatewayFeature.BinaryPayload)
            : (uint)GatewayFeature.CommandCapabilities;

        var helloWriter = new FixedBufferWriter(new byte[4 * 1024]);
        WireCodecs.ClientHello.Serialize(helloWriter, new ClientHello
        {
            ProtocolVersion = PacketProtocol.CurrentProtocolVersion,
            FeatureBits = featureBits,
            InstallationId = "binary-shorttest",
            ResumeToken = null
        });
        await WriteFrameAsync(
            PacketCommand.ClientHello,
            helloWriter.Buffer.AsMemory(0, helloWriter.WrittenCount),
            cancellationToken);

        var frame = await ReadFrameAsync(cancellationToken);
        Guard.Ensure(frame.Command == PacketCommand.ServerHello, "expected ServerHello after ClientHello.");
        var serverHello = WireCodecs.ServerHello.Deserialize(new ReadOnlySequence<byte>(frame.Payload))
            ?? throw new InvalidOperationException("ServerHello deserialization failed.");
        Guard.Ensure(
            serverHello.ProtocolVersion == PacketProtocol.CurrentProtocolVersion,
            $"unexpected protocol version {serverHello.ProtocolVersion}.");

        var expectedFormat = requested == WireFormat.Binary
            ? BinaryPayloadFormat.Id
            : ProtocolPayloadFormat.Json;
        Guard.Ensure(
            string.Equals(serverHello.PayloadFormat, expectedFormat, StringComparison.Ordinal),
            $"unexpected payload format '{serverHello.PayloadFormat}' (expected '{expectedFormat}').");
    }

    private async Task AuthenticateAsync(ulong deviceHash, CancellationToken cancellationToken)
    {
        if (Negotiated == WireFormat.Binary)
        {
            var encodeBuffer = new byte[BinaryLimits.Default.MaxMessageBytes];
            var encode = TcpBinaryWireEncoder.TryEncode(
                new ChatApp.Shared.Protocol.Tcp.AuthenticationRequest
                {
                    AccessToken = "valid-token",
                    DeviceIdHash = deviceHash
                },
                encodeBuffer,
                BinaryLimits.Default);
            Guard.Ensure(
                encode.Status == TcpBinaryWireEncodeStatus.Encoded,
                $"binary auth encode failed: {encode.Status}");
            await WriteFrameAsync(
                PacketCommand.AuthenticationRequest,
                encodeBuffer.AsMemory(0, encode.Written),
                cancellationToken);
        }
        else
        {
            var authWriter = new FixedBufferWriter(new byte[4 * 1024]);
            WireCodecs.AuthenticationRequest.Serialize(authWriter, new AuthenticationRequest
            {
                AccessToken = "valid-token",
                DeviceIdHash = deviceHash
            });
            await WriteFrameAsync(
                PacketCommand.AuthenticationRequest,
                authWriter.Buffer.AsMemory(0, authWriter.WrittenCount),
                cancellationToken);
        }

        var frame = await ReadFrameAsync(cancellationToken);
        Guard.Ensure(frame.Command == PacketCommand.AuthenticationResponse, "expected AuthenticationResponse.");
        AuthenticationResponse? authentication;
        if (Negotiated == WireFormat.Binary)
        {
            var decode = TcpBinaryWireCodec.TryDecode(
                PacketCommand.AuthenticationResponse,
                new ReadOnlySequence<byte>(frame.Payload),
                BinaryLimits.Default);
            Guard.Ensure(
                decode.Status == TcpBinaryWireStatus.Decoded,
                $"binary auth decode failed: {decode.Status}");
            authentication = MapAuthenticationResponse((ChatApp.Shared.Protocol.Tcp.AuthenticationResponse)decode.Value!);
        }
        else
        {
            authentication = WireCodecs.AuthenticationResponse.Deserialize(new ReadOnlySequence<byte>(frame.Payload));
        }

        if (authentication is null || !authentication.Success)
        {
            throw new InvalidOperationException("authentication rejected by gateway.");
        }

        Guard.Ensure(authentication.UserId == SenderUserId, $"unexpected user id {authentication.UserId}.");
    }

    private static AuthenticationResponse MapAuthenticationResponse(
        ChatApp.Shared.Protocol.Tcp.AuthenticationResponse shared) => new()
    {
        Success = shared.Success,
        UserId = shared.UserId,
        ErrorMessage = shared.ErrorMessage,
        SessionId = shared.SessionId,
        DeviceIdHash = shared.DeviceIdHash,
        DeviceId = shared.DeviceId,
        ResumeToken = shared.ResumeToken
    };
}
