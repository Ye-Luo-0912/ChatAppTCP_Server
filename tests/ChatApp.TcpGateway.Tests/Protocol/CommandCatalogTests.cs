using ChatApp.TcpGateway.Core.Protocol;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Protocol;

/// <summary>
/// CommandCatalog 完整性测试。强制每个 PacketCommand 枚举值都在目录中登记，
/// 防止新增命令时漏改 catalog。同时锁定协议不变量（Inline 命令集合、payload 上限范围等）。
/// </summary>
public class CommandCatalogTests
{
    [Fact]
    public void EveryEnumValueIsClassified()
    {
        var unclassified = new List<PacketCommand>();
        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            if (CommandCatalog.TryGetDescriptor(command) is null)
                unclassified.Add(command);
        }

        Assert.Empty(unclassified);
    }

    [Fact]
    public void EveryClientToServerCommandHasValidPayloadLimit()
    {
        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            var descriptor = CommandCatalog.TryGetDescriptor(command);
            Assert.NotNull(descriptor);
            if (descriptor!.Value.Direction == CommandDirection.ClientToServer)
            {
                // 客户端可发送命令的 payload 上限必须在 [0, MaxPayloadSize] 范围内。
                Assert.InRange(
                    descriptor.Value.MaxPayloadBytes,
                    0,
                    PacketProtocol.MaxPayloadSize);
            }
        }
    }

    [Fact]
    public void EveryServerToClientCommandReturnsNegativeOnePayload()
    {
        // 服务端→客户端命令客户端不可发送，GetMaxPayload 必须返回 -1。
        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            var descriptor = CommandCatalog.TryGetDescriptor(command);
            Assert.NotNull(descriptor);
            if (descriptor!.Value.Direction == CommandDirection.ServerToClient)
            {
                Assert.Equal(-1, descriptor.Value.MaxPayloadBytes);
                Assert.Equal(-1, CommandCatalog.GetMaxPayload(command));
            }
        }
    }

    [Fact]
    public void EveryCommandHasPositiveRateCost()
    {
        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            var descriptor = CommandCatalog.TryGetDescriptor(command);
            Assert.NotNull(descriptor);
            Assert.True(descriptor!.Value.RateCost > 0,
                $"{command} 的 RateCost 必须为正数");
        }
    }

    [Fact]
    public void GetMaxPayload_AgreesWithDescriptor()
    {
        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            var descriptor = CommandCatalog.TryGetDescriptor(command);
            Assert.NotNull(descriptor);
            if (descriptor!.Value.Direction == CommandDirection.ClientToServer)
                Assert.Equal(descriptor.Value.MaxPayloadBytes,
                    CommandCatalog.GetMaxPayload(command));
            else
                Assert.Equal(-1, CommandCatalog.GetMaxPayload(command));
        }
    }

    [Fact]
    public void GetCost_AgreesWithDescriptor()
    {
        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            var descriptor = CommandCatalog.TryGetDescriptor(command);
            Assert.NotNull(descriptor);
            Assert.Equal(descriptor!.Value.RateCost, CommandCatalog.GetCost(command));
        }
    }

    [Fact]
    public void GetLane_AgreesWithDescriptor()
    {
        foreach (var command in Enum.GetValues<PacketCommand>())
        {
            var descriptor = CommandCatalog.TryGetDescriptor(command);
            Assert.NotNull(descriptor);
            Assert.Equal(descriptor!.Value.Lane, CommandCatalog.GetLane(command));
        }
    }

    [Fact]
    public void InlineLaneContainsExactly_ControlCommands()
    {
        // 协议不变量：ClientHello / AuthenticationRequest / Heartbeat / PresenceUnwatch 必须是 Inline。
        // 这四类命令若误入异步 lane，会导致握手/认证/心跳串行化失败，多帧 TCP 段可能重排。
        var expectedInline = new[]
        {
            PacketCommand.ClientHello,
            PacketCommand.AuthenticationRequest,
            PacketCommand.Heartbeat,
            PacketCommand.PresenceUnwatch
        };

        foreach (var command in expectedInline)
        {
            Assert.Equal(CommandLane.Inline, CommandCatalog.GetLane(command));
        }
    }

    [Fact]
    public void TypingNotifyIsEphemeral()
    {
        // Typing 是当前唯一的 Ephemeral lane 命令。DropOldest 语义依赖此假设。
        Assert.Equal(CommandLane.Ephemeral, CommandCatalog.GetLane(PacketCommand.TypingNotify));
    }

    [Fact]
    public void QueryLaneCommandsAreReads()
    {
        // Query lane 命令必须是只读查询，不得包含写操作。
        var expectedQuery = new[]
        {
            PacketCommand.MessageHistoryRequest,
            PacketCommand.ConversationListRequest,
            PacketCommand.SyncBootstrapRequest,
            PacketCommand.ListGroupMembersRequest,
            PacketCommand.PresenceQuery
        };

        foreach (var command in expectedQuery)
            Assert.Equal(CommandLane.Query, CommandCatalog.GetLane(command));
    }

    [Fact]
    public void IsPreAuthentication_ReturnsTrueOnlyForHandshakeCommands()
    {
        Assert.True(CommandCatalog.IsPreAuthentication(PacketCommand.ClientHello));
        Assert.True(CommandCatalog.IsPreAuthentication(PacketCommand.AuthenticationRequest));

        // 任意业务命令必须返回 false。
        Assert.False(CommandCatalog.IsPreAuthentication(PacketCommand.Heartbeat));
        Assert.False(CommandCatalog.IsPreAuthentication(PacketCommand.ChatMessage));
        Assert.False(CommandCatalog.IsPreAuthentication(PacketCommand.PresenceUnwatch));
    }

    [Fact]
    public void GetMaxPayload_ReturnsNegativeOneForServerToClientCommands()
    {
        // 关键不变量：服务端→客户端命令客户端不可发送，必须返回 -1。
        Assert.Equal(-1, CommandCatalog.GetMaxPayload(PacketCommand.AuthenticationResponse));
        Assert.Equal(-1, CommandCatalog.GetMaxPayload(PacketCommand.ServerHello));
        Assert.Equal(-1, CommandCatalog.GetMaxPayload(PacketCommand.GoAway));
        Assert.Equal(-1, CommandCatalog.GetMaxPayload(PacketCommand.MessageAcknowledgement));
        Assert.Equal(-1, CommandCatalog.GetMaxPayload(PacketCommand.HeartbeatAcknowledgement));
        Assert.Equal(-1, CommandCatalog.GetMaxPayload(PacketCommand.Error));
    }

    [Theory]
    [InlineData(PacketCommand.Heartbeat, 0)]
    [InlineData(PacketCommand.AuthenticationRequest, 4 * 1024)]
    [InlineData(PacketCommand.ClientHello, 4 * 1024)]
    [InlineData(PacketCommand.ChatMessage, 64 * 1024)]
    [InlineData(PacketCommand.MessageReceipt, 1024)]
    [InlineData(PacketCommand.MessageHistoryRequest, 4 * 1024)]
    [InlineData(PacketCommand.SyncBootstrapRequest, 16 * 1024)]
    [InlineData(PacketCommand.CreateGroupRequest, 16 * 1024)]
    [InlineData(PacketCommand.AddGroupMembersRequest, 16 * 1024)]
    [InlineData(PacketCommand.TypingNotify, 512)]
    [InlineData(PacketCommand.PresenceUnwatch, 4 * 1024)]
    [InlineData(PacketCommand.RegisterPushTokenRequest, 2 * 1024)]
    public void GetMaxPayload_ReturnsExpectedValueForClientCommands(
        PacketCommand command, int expected)
    {
        Assert.Equal(expected, CommandCatalog.GetMaxPayload(command));
    }

    [Theory]
    [InlineData(PacketCommand.Heartbeat, 1)]
    [InlineData(PacketCommand.AuthenticationRequest, 2)]
    [InlineData(PacketCommand.ChatMessage, 4)]
    [InlineData(PacketCommand.SyncBootstrapRequest, 8)]
    [InlineData(PacketCommand.CreateGroupRequest, 8)]
    [InlineData(PacketCommand.MessageReceipt, 1)]
    [InlineData(PacketCommand.TypingNotify, 1)]
    [InlineData(PacketCommand.UnregisterPushTokenRequest, 1)]
    public void GetCost_ReturnsExpectedValue(PacketCommand command, int expected)
    {
        Assert.Equal(expected, CommandCatalog.GetCost(command));
    }
}
