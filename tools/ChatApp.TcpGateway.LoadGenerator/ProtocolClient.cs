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

    public async ValueTask<AuthenticatedIdentity> AuthenticateAsync(
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
            throw new InvalidOperationException(
                response?.ErrorMessage ?? "Authentication failed.");
        }

        return new AuthenticatedIdentity(
            response.UserId,
            response.SessionId,
            response.DeviceIdHash);
    }

    public ValueTask SendHeartbeatAsync(
        CancellationToken cancellationToken) =>
        SendEmptyAsync(
            PacketCommand.Heartbeat,
            cancellationToken);

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
                Acknowledgement: null),
            PacketCommand.MessageAcknowledgement => new ChatInboundFrame(
                Message: null,
                MessageAcknowledgementCodec.Deserialize(payload)
                ?? throw new InvalidDataException(
                    "Received an empty message acknowledgement.")),
            _ => throw new InvalidDataException(
                $"Expected chat message or acknowledgement, received {frame.Command}.")
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
internal sealed record ChatInboundFrame(
    ChatMessage? Message,
    MessageAcknowledgement? Acknowledgement);
