using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using ChatApp.Shared.Protocol.Tcp.Json;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using SharedGatewayFeature = ChatApp.Shared.Protocol.Tcp.GatewayFeature;
using SharedProtocolErrorCode = ChatApp.Shared.Protocol.Tcp.ProtocolErrorCode;
using SharedTcp = ChatApp.Shared.Protocol.Tcp;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Serialization;

/// <summary>
/// Locks the TCP control-plane wire contract while Gateway moves DTO ownership to
/// ChatApp.Protocol.Tcp. Both source-generated contexts must remain byte-compatible.
/// </summary>
public sealed class TcpProtocolContractCompatibilityTests
{
    [Fact]
    public void ClientHello_Is_Byte_And_Read_Compatible()
    {
        var value = new SharedTcp.ClientHello
        {
            ProtocolVersion = 1,
            FeatureBits = (uint)(SharedGatewayFeature.CommandCapabilities | SharedGatewayFeature.SessionResume),
            InstallationId = "b248f55f-835c-483a-bc6a-f872f6ce47b1",
            ClientTimeMs = 1_784_800_123_456,
            ResumeToken = "resume-token",
            MaxPayloadBytes = 81_920
        };

        AssertWireCompatible(
            value,
            GatewayJsonSerializerContext.Default.ClientHello,
            TcpProtocolJsonSerializerContext.Default.ClientHello);
    }

    [Fact]
    public void ServerHello_Is_Byte_And_Read_Compatible()
    {
        var value = new SharedTcp.ServerHello
        {
            ProtocolVersion = 1,
            FeatureBits = (uint)(SharedGatewayFeature.CommandCapabilities | SharedGatewayFeature.ConversationSync),
            ServerDeviceId = "gateway-east-01",
            ServerTimeMs = 1_784_800_123_789,
            HeartbeatIntervalMs = 30_000,
            MaxPayloadBytes = 81_920,
            ResumeSupported = true,
            PayloadFormat = SharedTcp.ProtocolPayloadFormat.Json
        };

        AssertWireCompatible(
            value,
            GatewayJsonSerializerContext.Default.ServerHello,
            TcpProtocolJsonSerializerContext.Default.ServerHello);
    }

    [Fact]
    public void GoAway_Is_Byte_And_Read_Compatible()
    {
        var value = new SharedTcp.GoAway
        {
            RetryAfterMs = 2_500,
            Reason = "rolling-upgrade",
            ServerHint = "gateway-east-02"
        };

        AssertWireCompatible(
            value,
            GatewayJsonSerializerContext.Default.GoAway,
            TcpProtocolJsonSerializerContext.Default.GoAway);
    }

    [Fact]
    public void ResumeResponse_Is_Byte_And_Read_Compatible()
    {
        var value = new SharedTcp.ResumeResponse
        {
            Success = false,
            FailureKind = SharedTcp.ResumeFailureKind.DependencyUnavailable,
            ResumeToken = "next-resume-token",
            UserId = 42,
            SessionId = "session-42",
            DeviceId = "device-42",
            LastConversationSequence = 12_345,
            ErrorMessage = "resume dependency unavailable",
            RetryAfterMs = 1_500
        };

        AssertWireCompatible(
            value,
            GatewayJsonSerializerContext.Default.ResumeResponse,
            TcpProtocolJsonSerializerContext.Default.ResumeResponse);
    }

    [Fact]
    public void ProtocolErrorFrame_Is_Byte_And_Read_Compatible()
    {
        var value = new SharedTcp.ProtocolErrorFrame
        {
            Code = SharedProtocolErrorCode.RateLimited,
            Fatal = false,
            RetryAfterMs = 1_000,
            Message = "rate limited",
            OriginCommand = (ushort)PacketCommand.ChatMessage
        };

        AssertWireCompatible(
            value,
            GatewayJsonSerializerContext.Default.ProtocolErrorFrame,
            TcpProtocolJsonSerializerContext.Default.ProtocolErrorFrame);
    }

    [Fact]
    public void GatewayPacketApi_UsesSharedPacketCommandAsItsSingleSource()
    {
        Assert.Equal(
            typeof(PacketCommand),
            typeof(PacketFrame).GetProperty(nameof(PacketFrame.Command))!.PropertyType);
        Assert.DoesNotContain(
            typeof(PacketFrame).Assembly.GetTypes(),
            static type => type.IsEnum && type.Name == nameof(PacketCommand));
    }

    [Fact]
    public void GatewayHeaderCodec_RoundTripsEverySharedPacketCommand()
    {
        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            var header = new byte[PacketProtocol.HeaderSize];
            PacketParser.WriteHeader(header, command, payloadLength: 0);

            Assert.Equal(
                (ushort)command,
                BinaryPrimitives.ReadUInt16LittleEndian(
                    header.AsSpan(PacketProtocol.CommandOffset, sizeof(ushort))));

            Assert.True(PacketParser.TryPeekCommand(
                new ReadOnlySequence<byte>(header),
                out var decoded));
            Assert.Equal(command, decoded);
        }
    }

    [Fact]
    public void GatewayFeature_Shared_Alias_Has_Exact_Stable_Name_Value_Surface()
    {
        Assert.Equal(typeof(uint), Enum.GetUnderlyingType(typeof(SharedGatewayFeature)));

        Assert.Equal(
            new (string Name, ulong Value)[]
            {
                ("None", 0),
                ("BinaryPayload", 1u << 0),
                ("Compression", 1u << 1),
                ("StreamingChat", 1u << 2),
                ("CommandCapabilities", 1u << 3),
                ("SessionResume", 1u << 4),
                ("ConversationSync", 1u << 5),
                ("ConversationPreferences", 1u << 6),
                ("MessageMutation", 1u << 7),
                ("PresenceAndTyping", 1u << 8),
                ("MessageReactions", 1u << 9),
                ("GroupManagement", 1u << 10),
                ("PushTokenManagement", 1u << 11)
            },
            GetOrderedEnumSurface<SharedGatewayFeature>());
    }

    [Fact]
    public void ProtocolErrorCode_Shared_Alias_Has_Exact_Stable_Name_Value_Surface()
    {
        Assert.Equal(typeof(ushort), Enum.GetUnderlyingType(typeof(SharedProtocolErrorCode)));

        Assert.Equal(
            new (string Name, ulong Value)[]
            {
                ("None", 0),
                ("ProtocolViolation", 1),
                ("UnsupportedCommand", 2),
                ("UnsupportedVersion", 3),
                ("InvalidPayload", 4),
                ("AuthRequired", 10),
                ("AuthRejected", 11),
                ("SessionRevoked", 12),
                ("ResumeFailed", 13),
                ("DependencyUnavailable", 14),
                ("AccountSuspended", 15),
                ("RateLimited", 20),
                ("PayloadTooLarge", 21),
                ("FeatureNotNegotiated", 22),
                ("ServerOverloaded", 30),
                ("Shutdown", 31),
                ("OutboundQueueFull", 32),
                ("InternalError", 99)
            },
            GetOrderedEnumSurface<SharedProtocolErrorCode>());
    }

    private static void AssertWireCompatible<T>(
        T value,
        JsonTypeInfo<T> gatewayTypeInfo,
        JsonTypeInfo<T> sharedTypeInfo)
        where T : class
    {
        var gatewayBytes = JsonSerializer.SerializeToUtf8Bytes(value, gatewayTypeInfo);
        var sharedBytes = JsonSerializer.SerializeToUtf8Bytes(value, sharedTypeInfo);

        Assert.Equal(gatewayBytes, sharedBytes);

        var sharedRead = JsonSerializer.Deserialize(gatewayBytes, sharedTypeInfo);
        var gatewayRead = JsonSerializer.Deserialize(sharedBytes, gatewayTypeInfo);

        Assert.NotNull(sharedRead);
        Assert.NotNull(gatewayRead);
        Assert.Equal(gatewayBytes, JsonSerializer.SerializeToUtf8Bytes(sharedRead, sharedTypeInfo));
        Assert.Equal(sharedBytes, JsonSerializer.SerializeToUtf8Bytes(gatewayRead, gatewayTypeInfo));
    }

    private static (string Name, ulong Value)[] GetOrderedEnumSurface<TEnum>()
        where TEnum : struct, Enum =>
        Enum.GetNames<TEnum>()
            .Select(name =>
                (name, Convert.ToUInt64(Enum.Parse<TEnum>(name), CultureInfo.InvariantCulture)))
            .ToArray();
}
