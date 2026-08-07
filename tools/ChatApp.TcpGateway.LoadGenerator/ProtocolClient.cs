using System.Buffers;
using System.Buffers.Binary;
using System.Net.Sockets;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;

namespace ChatApp.TcpGateway.LoadGenerator;

internal sealed class ProtocolClient : IAsyncDisposable
{
    private static readonly JsonPayloadCodec<AuthenticationRequest>
        AuthenticationRequestCodec =
            new(GatewayJsonSerializerContext.Default.AuthenticationRequest);

    private static readonly JsonPayloadCodec<AuthenticationResponse>
        AuthenticationResponseCodec =
            new(GatewayJsonSerializerContext.Default.AuthenticationResponse);

    private static readonly JsonPayloadCodec<ChatMessage>
        ChatMessageCodec =
            new(GatewayJsonSerializerContext.Default.ChatMessage);

    private static readonly JsonPayloadCodec<MessageAcknowledgement>
        MessageAcknowledgementCodec =
            new(GatewayJsonSerializerContext.Default.MessageAcknowledgement);

    private readonly TcpClient _client = new();
    private NetworkStream? _stream;

    public async ValueTask ConnectAsync(
        string host,
        int port,
        bool constrainReceiveBuffer,
        CancellationToken cancellationToken)
    {
        _client.NoDelay = true;
        if (constrainReceiveBuffer)
        {
            _client.ReceiveBufferSize = 1024;
        }

        await _client.ConnectAsync(
                host,
                port,
                cancellationToken)
            .ConfigureAwait(false);
        _stream = _client.GetStream();
    }

    public async ValueTask<AuthenticationResult> AuthenticateAsync(
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

        var responseFrame = await ReceiveAsync(
                cancellationToken)
            .ConfigureAwait(false);

        if (responseFrame.Command !=
            PacketCommand.AuthenticationResponse)
        {
            throw new InvalidDataException(
                $"Expected authentication response, received {responseFrame.Command}.");
        }

        var response = AuthenticationResponseCodec.Deserialize(
            new ReadOnlySequence<byte>(responseFrame.Payload));

        if (response?.Success != true)
        {
            return new AuthenticationResult(
                Succeeded: false,
                FailureKind: ClassifyAuthFailure(response?.ErrorMessage),
                ErrorMessage: response?.ErrorMessage ?? "Authentication failed.",
                Identity: null,
                ResumeTokenIssued: false);
        }

        return new AuthenticationResult(
            Succeeded: true,
            FailureKind: AuthFailureKind.None,
            ErrorMessage: null,
            Identity: new AuthenticatedIdentity(
                response.UserId,
                response.SessionId,
                response.DeviceIdHash),
            ResumeTokenIssued: !string.IsNullOrWhiteSpace(response.ResumeToken));
    }

    private static AuthFailureKind ClassifyAuthFailure(string? errorMessage)
    {
        if (string.IsNullOrWhiteSpace(errorMessage))
            return AuthFailureKind.Other;

        var normalized = errorMessage.ToLowerInvariant();
        if (normalized.Contains("dependency", StringComparison.Ordinal) ||
            normalized.Contains("unavailable", StringComparison.Ordinal) ||
            normalized.Contains("redis", StringComparison.Ordinal))
        {
            return AuthFailureKind.DependencyUnavailable;
        }

        if (normalized.Contains("无效", StringComparison.Ordinal) ||
            normalized.Contains("过期", StringComparison.Ordinal) ||
            normalized.Contains("invalid", StringComparison.Ordinal) ||
            normalized.Contains("suspend", StringComparison.Ordinal))
        {
            return AuthFailureKind.InvalidToken;
        }

        return AuthFailureKind.Other;
    }

    public ValueTask SendHeartbeatAsync(
        CancellationToken cancellationToken) =>
        SendEmptyAsync(
            PacketCommand.Heartbeat,
            cancellationToken);

    public async ValueTask WaitForRemoteCloseAsync(
        CancellationToken cancellationToken)
    {
        var probe = new byte[1];
        var read = await GetStream()
            .ReadAsync(probe, cancellationToken)
            .ConfigureAwait(false);
        if (read == 0)
            return;

        throw new InvalidDataException(
            "Connection-only client received unexpected protocol data while " +
            "observing remote liveness.");
    }

    public async ValueTask ReceiveHeartbeatAsync(
        CancellationToken cancellationToken)
    {
        var frame = await ReceiveAsync(cancellationToken)
            .ConfigureAwait(false);

        if (frame.Command !=
            PacketCommand.HeartbeatAcknowledgement)
        {
            throw new InvalidDataException(
                $"Expected heartbeat acknowledgement, received {frame.Command}.");
        }
    }

    public ValueTask SendChatMessageAsync(
        ChatMessage message,
        CancellationToken cancellationToken) =>
        SendAsync(
            PacketCommand.ChatMessage,
            ChatMessageCodec,
            message,
            cancellationToken);

    public async ValueTask<ChatInboundFrame> ReceiveChatInboundAsync(
        CancellationToken cancellationToken)
    {
        var frame = await ReceiveAsync(cancellationToken)
            .ConfigureAwait(false);
        var payload = new ReadOnlySequence<byte>(frame.Payload);

        return frame.Command switch
        {
            PacketCommand.ChatMessage => new ChatInboundFrame(
                ChatMessageCodec.Deserialize(payload)
                ?? throw new InvalidDataException(
                    "Received an empty chat message."),
                Acknowledgement: null,
                IsHeartbeatAcknowledgement: false),
            PacketCommand.MessageAcknowledgement => new ChatInboundFrame(
                Message: null,
                MessageAcknowledgementCodec.Deserialize(payload)
                ?? throw new InvalidDataException(
                    "Received an empty message acknowledgement."),
                IsHeartbeatAcknowledgement: false),
            PacketCommand.HeartbeatAcknowledgement => new ChatInboundFrame(
                Message: null,
                Acknowledgement: null,
                IsHeartbeatAcknowledgement: true),
            _ => throw new InvalidDataException(
                $"Expected chat message, message acknowledgement, or heartbeat " +
                $"acknowledgement; received {frame.Command}.")
        };
    }

    public async ValueTask SendInvalidPacketAndWaitForCloseAsync(
        CancellationToken cancellationToken)
    {
        var invalidHeader = new byte[PacketProtocol.HeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(
            invalidHeader,
            PacketProtocol.MagicNumber ^ uint.MaxValue);

        await GetStream()
            .WriteAsync(invalidHeader, cancellationToken)
            .ConfigureAwait(false);

        var probe = new byte[1];
        var read = await GetStream()
            .ReadAsync(probe, cancellationToken)
            .ConfigureAwait(false);

        if (read != 0)
        {
            throw new InvalidDataException(
                "Gateway did not close the invalid protocol connection.");
        }
    }

    public ValueTask DisposeAsync()
    {
        _stream?.Dispose();
        _client.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Slowloris 攻击：发送不完整帧（Header 或 Payload 阶段），随后仅等待 Gateway
    /// 关闭连接。返回 true 表示 Gateway 在装配 deadline 内主动关闭了连接（防御生效）；
    /// false 表示连接在调用方取消前仍保持打开（未执行 deadline 强制）。
    /// </summary>
    /// <param name="phase">Header：只发送部分 Header 字节；Payload：完整 Header + 极小 Payload 切片。</param>
    /// <param name="delayMs">分段发送之间的间隔（模拟逐字节慢速攻击）。</param>
    /// <param name="cancellationToken">取消令牌；取消后立即返回 false，不视为已被 Gateway 关闭。</param>
    public async ValueTask<bool> SendSlowlorisAndWaitForCloseAsync(
        SlowlorisPhase phase,
        int delayMs,
        CancellationToken cancellationToken)
    {
        var stream = GetStream();
        if (phase == SlowlorisPhase.Header)
        {
            // 只发送 10 字节 Header 的前 6 字节（magic + 部分 command），随后停下。
            // Gateway 的 Header 装配 deadline 应在超时后关闭该连接。
            var partial = new byte[6];
            BinaryPrimitives.WriteUInt32LittleEndian(
                partial,
                PacketProtocol.MagicNumber);
            await stream.WriteAsync(partial, cancellationToken)
                .ConfigureAwait(false);
            await Task.Delay(delayMs, cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            // 发送完整 Header，声明一段 4 KiB Payload，但只发送 1 字节，
            // 制造不完整 Payload，触发 Gateway 的 Payload 装配 deadline。
            var header = new byte[PacketProtocol.HeaderSize];
            PacketParser.WriteHeader(
                header,
                PacketCommand.AuthenticationRequest,
                payloadLength: 4096);
            await stream.WriteAsync(header, cancellationToken)
                .ConfigureAwait(false);
            await stream.WriteAsync(new byte[1], cancellationToken)
                .ConfigureAwait(false);
            await Task.Delay(delayMs, cancellationToken)
                .ConfigureAwait(false);
        }

        // 等待 Gateway 关闭连接（read 返回 0）。若取消先触发，视为未强制执行。
        var probe = new byte[1];
        var read = await stream.ReadAsync(probe, cancellationToken)
            .ConfigureAwait(false);
        return read == 0;
    }

    private async ValueTask SendAsync<T>(
        PacketCommand command,
        JsonPayloadCodec<T> codec,
        T value,
        CancellationToken cancellationToken)
    {
        var payloadWriter = new ArrayBufferWriter<byte>();
        codec.Serialize(payloadWriter, value);

        var frame = new byte[
            PacketProtocol.HeaderSize + payloadWriter.WrittenCount];
        PacketParser.WriteHeader(
            frame,
            command,
            payloadWriter.WrittenCount);
        payloadWriter.WrittenSpan.CopyTo(
            frame.AsSpan(PacketProtocol.HeaderSize));

        await GetStream()
            .WriteAsync(frame, cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask SendEmptyAsync(
        PacketCommand command,
        CancellationToken cancellationToken)
    {
        var header = new byte[PacketProtocol.HeaderSize];
        PacketParser.WriteHeader(
            header,
            command,
            payloadLength: 0);

        await GetStream()
            .WriteAsync(header, cancellationToken)
            .ConfigureAwait(false);
    }

    private async ValueTask<ReceivedFrame> ReceiveAsync(
        CancellationToken cancellationToken)
    {
        var header = new byte[PacketProtocol.HeaderSize];
        await ReadExactlyAsync(
                header,
                cancellationToken)
            .ConfigureAwait(false);

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
        if (magic != PacketProtocol.MagicNumber)
        {
            throw new InvalidDataException(
                "Received an invalid packet magic number.");
        }

        var command = (PacketCommand)
            BinaryPrimitives.ReadUInt16LittleEndian(
                header.AsSpan(PacketProtocol.CommandOffset));
        var payloadLength =
            BinaryPrimitives.ReadInt32LittleEndian(
                header.AsSpan(PacketProtocol.LengthOffset));

        if (payloadLength is < 0 or > PacketProtocol.MaxPayloadSize)
        {
            throw new InvalidDataException(
                $"Received invalid payload length {payloadLength}.");
        }

        var payload = new byte[payloadLength];
        if (payloadLength != 0)
        {
            await ReadExactlyAsync(
                    payload,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        return new ReceivedFrame(command, payload);
    }

    private async ValueTask ReadExactlyAsync(
        Memory<byte> destination,
        CancellationToken cancellationToken)
    {
        var read = 0;
        while (read < destination.Length)
        {
            var bytesRead = await GetStream()
                .ReadAsync(
                    destination[read..],
                    cancellationToken)
                .ConfigureAwait(false);

            if (bytesRead == 0)
            {
                throw new EndOfStreamException(
                    "The gateway closed the connection.");
            }

            read += bytesRead;
        }
    }

    private NetworkStream GetStream() =>
        _stream
        ?? throw new InvalidOperationException(
            "The client is not connected.");

    private sealed record ReceivedFrame(
        PacketCommand Command,
        byte[] Payload);
}

internal sealed record AuthenticatedIdentity(
    long UserId,
    string? SessionId,
    ulong? DeviceIdHash);

internal enum AuthFailureKind
{
    None,
    InvalidToken,
    DependencyUnavailable,
    Other
}

internal sealed record AuthenticationResult(
    bool Succeeded,
    AuthFailureKind FailureKind,
    string? ErrorMessage,
    AuthenticatedIdentity? Identity,
    bool ResumeTokenIssued);
internal sealed record ChatInboundFrame(
    ChatMessage? Message,
    MessageAcknowledgement? Acknowledgement,
    bool IsHeartbeatAcknowledgement);
