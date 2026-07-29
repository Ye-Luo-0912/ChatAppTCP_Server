using System.Buffers;
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text.Json;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;

namespace ChatApp.ResumeVerification.Runtime;

/// <summary>
/// 支持 Resume 流程的协议客户端。复用 LoadGenerator.ProtocolClient 的收发模式，
/// 额外提供 ClientHello 握手与 ResumeToken 恢复能力。
/// </summary>
internal sealed class ResumeCapableProtocolClient : IAsyncDisposable
{
    private static readonly JsonPayloadCodec<ClientHello> ClientHelloCodec =
        new(GatewayJsonSerializerContext.Default.ClientHello);

    private static readonly JsonPayloadCodec<ServerHello> ServerHelloCodec =
        new(GatewayJsonSerializerContext.Default.ServerHello);

    private static readonly JsonPayloadCodec<ResumeResponse> ResumeResponseCodec =
        new(GatewayJsonSerializerContext.Default.ResumeResponse);

    private static readonly JsonPayloadCodec<ProtocolErrorFrame> ProtocolErrorCodec =
        new(GatewayJsonSerializerContext.Default.ProtocolErrorFrame);

    private static readonly JsonPayloadCodec<AuthenticationRequest> AuthenticationRequestCodec =
        new(GatewayJsonSerializerContext.Default.AuthenticationRequest);

    private static readonly JsonPayloadCodec<AuthenticationResponse> AuthenticationResponseCodec =
        new(GatewayJsonSerializerContext.Default.AuthenticationResponse);

    private readonly TcpClient _client = new();
    private NetworkStream? _stream;

    /// <summary>建立 TCP 连接。</summary>
    public async ValueTask ConnectAsync(
        string host,
        int port,
        CancellationToken cancellationToken)
    {
        _client.NoDelay = true;
        await _client.ConnectAsync(host, port, cancellationToken)
            .ConfigureAwait(false);
        _stream = _client.GetStream();
    }

    /// <summary>
    /// 发送不带 ResumeToken 的 ClientHello，接收 ServerHello，返回协商的能力位。
    /// </summary>
    public async ValueTask<ServerHello> HandshakeAsync(
        uint featureBits,
        CancellationToken cancellationToken)
    {
        var hello = new ClientHello
        {
            ProtocolVersion = PacketProtocol.CurrentProtocolVersion,
            FeatureBits = featureBits,
            ClientTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        await SendAsync(
                PacketCommand.ClientHello,
                ClientHelloCodec,
                hello,
                cancellationToken)
            .ConfigureAwait(false);

        var frame = await ReceiveFrameAsync(cancellationToken).ConfigureAwait(false);
        if (frame.Command != PacketCommand.ServerHello)
        {
            throw new InvalidDataException(
                $"Expected ServerHello, received {frame.Command}.");
        }

        return ServerHelloCodec.Deserialize(new ReadOnlySequence<byte>(frame.Payload))
            ?? throw new InvalidDataException("Received an empty ServerHello.");
    }

    /// <summary>
    /// 发送 AuthenticationRequest，接收 AuthenticationResponse，捕获 ResumeToken。
    /// </summary>
    public async ValueTask<AuthenticatedSession> AuthenticateAsync(
        string accessToken,
        ulong? deviceIdHash,
        CancellationToken cancellationToken)
    {
        var request = new AuthenticationRequest
        {
            AccessToken = accessToken,
            DeviceIdHash = deviceIdHash
        };

        await SendAsync(
                PacketCommand.AuthenticationRequest,
                AuthenticationRequestCodec,
                request,
                cancellationToken)
            .ConfigureAwait(false);

        var frame = await ReceiveFrameAsync(cancellationToken).ConfigureAwait(false);
        if (frame.Command != PacketCommand.AuthenticationResponse)
        {
            throw new InvalidDataException(
                $"Expected AuthenticationResponse, received {frame.Command}.");
        }

        var response = AuthenticationResponseCodec.Deserialize(
                new ReadOnlySequence<byte>(frame.Payload))
            ?? throw new InvalidDataException("Received an empty AuthenticationResponse.");

        if (response.Success != true)
        {
            throw new InvalidOperationException(
                response.ErrorMessage ?? "Authentication failed.");
        }

        return new AuthenticatedSession(
            response.UserId,
            response.SessionId,
            response.DeviceIdHash,
            response.ResumeToken);
    }

    /// <summary>
    /// 发送携带 ResumeToken 的 ClientHello，尝试恢复会话。
    /// 服务端可能回复 ResumeResponse（成功）、ProtocolErrorFrame（ResumeFailed）
    /// 或直接 ServerHello（resume 被忽略，回退到完整认证）。
    /// </summary>
    public async ValueTask<ResumeAttemptResult> ResumeAsync(
        string resumeToken,
        uint featureBits,
        CancellationToken cancellationToken)
    {
        var hello = new ClientHello
        {
            ProtocolVersion = PacketProtocol.CurrentProtocolVersion,
            FeatureBits = featureBits,
            ClientTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ResumeToken = resumeToken
        };

        await SendAsync(
                PacketCommand.ClientHello,
                ClientHelloCodec,
                hello,
                cancellationToken)
            .ConfigureAwait(false);

        var frame = await ReceiveFrameAsync(cancellationToken).ConfigureAwait(false);

        if (frame.Command == PacketCommand.ResumeResponse)
        {
            var response = ResumeResponseCodec.Deserialize(
                    new ReadOnlySequence<byte>(frame.Payload))
                ?? throw new InvalidDataException("Received an empty ResumeResponse.");

            if (response.Success)
            {
                return new ResumeAttemptResult(
                    ResumeAttemptOutcome.Success,
                    new AuthenticatedSession(
                        response.UserId,
                        response.SessionId,
                        null,
                        response.ResumeToken),
                    ErrorMessage: null);
            }

            return new ResumeAttemptResult(
                ResumeAttemptOutcome.Failed,
                Session: null,
                ErrorMessage: response.ErrorMessage ?? "ResumeResponse.Success=false");
        }

        if (frame.Command == PacketCommand.Error)
        {
            var error = ProtocolErrorCodec.Deserialize(
                    new ReadOnlySequence<byte>(frame.Payload))
                ?? throw new InvalidDataException("Received an empty ProtocolErrorFrame.");

            return new ResumeAttemptResult(
                ResumeAttemptOutcome.Failed,
                Session: null,
                ErrorMessage: $"Code={error.Code}; {error.Message}");
        }

        if (frame.Command == PacketCommand.ServerHello)
        {
            return new ResumeAttemptResult(
                ResumeAttemptOutcome.ServerHelloFallback,
                Session: null,
                ErrorMessage: "Server replied with ServerHello; resume ignored.");
        }

        throw new InvalidDataException(
            $"Unexpected frame during resume: {frame.Command}.");
    }

    /// <summary>
    /// 低层帧接收，返回命令与原始 payload。用于场景需要手动处理非预期帧。
    /// </summary>
    public async ValueTask<ReceivedFrame> ReceiveFrameAsync(
        CancellationToken cancellationToken)
    {
        var header = new byte[PacketProtocol.HeaderSize];
        await ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
        if (magic != PacketProtocol.MagicNumber)
        {
            throw new InvalidDataException("Received an invalid packet magic number.");
        }

        var command = (PacketCommand)BinaryPrimitives.ReadUInt16LittleEndian(
            header.AsSpan(PacketProtocol.CommandOffset));
        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            header.AsSpan(PacketProtocol.LengthOffset));

        if (payloadLength is < 0 or > PacketProtocol.MaxPayloadSize)
        {
            throw new InvalidDataException(
                $"Received invalid payload length {payloadLength}.");
        }

        var payload = new byte[payloadLength];
        if (payloadLength != 0)
        {
            await ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false);
        }

        return new ReceivedFrame(command, payload);
    }

    /// <summary>发送空 Heartbeat 帧，用于保活。</summary>
    public ValueTask SendHeartbeatAsync(CancellationToken cancellationToken) =>
        SendEmptyAsync(PacketCommand.Heartbeat, cancellationToken);

    public ValueTask DisposeAsync()
    {
        _stream?.Dispose();
        _client.Dispose();
        return ValueTask.CompletedTask;
    }

    private async ValueTask SendAsync<T>(
        PacketCommand command,
        JsonPayloadCodec<T> codec,
        T value,
        CancellationToken cancellationToken)
    {
        var payloadWriter = new ArrayBufferWriter<byte>();
        codec.Serialize(payloadWriter, value);

        var frame = new byte[PacketProtocol.HeaderSize + payloadWriter.WrittenCount];
        PacketParser.WriteHeader(frame, command, payloadWriter.WrittenCount);
        payloadWriter.WrittenSpan.CopyTo(frame.AsSpan(PacketProtocol.HeaderSize));

        await GetStream().WriteAsync(frame, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask SendEmptyAsync(
        PacketCommand command,
        CancellationToken cancellationToken)
    {
        var header = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(header, command, payloadLength: 0);
        await GetStream().WriteAsync(header, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ReadExactlyAsync(
        Memory<byte> destination,
        CancellationToken cancellationToken)
    {
        var read = 0;
        while (read < destination.Length)
        {
            var bytesRead = await GetStream()
                .ReadAsync(destination[read..], cancellationToken)
                .ConfigureAwait(false);
            if (bytesRead == 0)
            {
                throw new EndOfStreamException("The gateway closed the connection.");
            }
            read += bytesRead;
        }
    }

    private NetworkStream GetStream() =>
        _stream ?? throw new InvalidOperationException("The client is not connected.");
}

/// <summary>已认证会话信息。</summary>
internal sealed record AuthenticatedSession(
    long UserId,
    string? SessionId,
    ulong? DeviceIdHash,
    string? ResumeToken);

/// <summary>Resume 尝试结果。</summary>
internal enum ResumeAttemptOutcome
{
    /// <summary>ResumeResponse.Success=true，会话已恢复。</summary>
    Success,

    /// <summary>ResumeFailed 错误帧或 ResumeResponse.Success=false。</summary>
    Failed,

    /// <summary>服务端直接回复 ServerHello，resume 被忽略。</summary>
    ServerHelloFallback
}

/// <summary>Resume 尝试结果详情。</summary>
internal sealed record ResumeAttemptResult(
    ResumeAttemptOutcome Outcome,
    AuthenticatedSession? Session,
    string? ErrorMessage);

/// <summary>接收到的帧。</summary>
internal sealed record ReceivedFrame(PacketCommand Command, byte[] Payload);
