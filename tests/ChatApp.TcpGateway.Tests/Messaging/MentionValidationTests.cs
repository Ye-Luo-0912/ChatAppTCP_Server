using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Networking;

namespace ChatApp.TcpGateway.Tests.Messaging;

public sealed class MentionValidationTests
{
    // ---- NormalizeMentionedUserIds ----

    [Fact]
    public void MentionedUserIds_Null_When_NotGroup()
    {
        var result = TcpGatewayService.NormalizeMentionedUserIds(
            [1002L, 1003L],
            isGroup: false,
            senderUserId: 1001);

        Assert.Null(result);
    }

    [Fact]
    public void MentionedUserIds_Null_When_InputNull()
    {
        var result = TcpGatewayService.NormalizeMentionedUserIds(
            raw: null,
            isGroup: true,
            senderUserId: 1001);

        Assert.Null(result);
    }

    [Fact]
    public void MentionedUserIds_Null_When_InputEmpty()
    {
        var result = TcpGatewayService.NormalizeMentionedUserIds(
            Array.Empty<long>(),
            isGroup: true,
            senderUserId: 1001);

        Assert.Null(result);
    }

    [Fact]
    public void MentionedUserIds_RemovesSelfMention()
    {
        var result = TcpGatewayService.NormalizeMentionedUserIds(
            [1002L, 1001, 1003],
            isGroup: true,
            senderUserId: 1001);

        Assert.NotNull(result);
        Assert.Equal([1002L, 1003], result);
    }

    [Fact]
    public void MentionedUserIds_RemovesNonPositiveIds()
    {
        var result = TcpGatewayService.NormalizeMentionedUserIds(
            [1002L, 0, -1, 1003],
            isGroup: true,
            senderUserId: 1001);

        Assert.NotNull(result);
        Assert.Equal([1002L, 1003], result);
    }

    [Fact]
    public void MentionedUserIds_Deduplicates()
    {
        var result = TcpGatewayService.NormalizeMentionedUserIds(
            [1002L, 1003, 1002, 1004, 1003],
            isGroup: true,
            senderUserId: 1001);

        Assert.NotNull(result);
        Assert.Equal([1002L, 1003, 1004], result);
    }

    [Fact]
    public void MentionedUserIds_TruncatesAtLimit()
    {
        var raw = new List<long>(ChatMessageLimits.MaxMentionedUserIds + 10);
        for (var i = 1; i <= ChatMessageLimits.MaxMentionedUserIds + 10; i++)
            raw.Add(2000L + i);

        var result = TcpGatewayService.NormalizeMentionedUserIds(
            raw,
            isGroup: true,
            senderUserId: 1001);

        Assert.NotNull(result);
        Assert.Equal(ChatMessageLimits.MaxMentionedUserIds, result.Count);
    }

    [Fact]
    public void MentionedUserIds_Null_When_AllFilteredOut()
    {
        var result = TcpGatewayService.NormalizeMentionedUserIds(
            [1001L, 0, -1],
            isGroup: true,
            senderUserId: 1001);

        Assert.Null(result);
    }

    [Fact]
    public void MentionedUserIds_PreservesOrder()
    {
        var result = TcpGatewayService.NormalizeMentionedUserIds(
            [1005L, 1002, 1004, 1003],
            isGroup: true,
            senderUserId: 1001);

        Assert.NotNull(result);
        Assert.Equal([1005L, 1002, 1004, 1003], result);
    }

    // ---- NormalizeMentionedRoles ----

    [Fact]
    public void MentionedRoles_Null_When_NotGroup()
    {
        var result = TcpGatewayService.NormalizeMentionedRoles(
            ["all", "admin"],
            isGroup: false);

        Assert.Null(result);
    }

    [Fact]
    public void MentionedRoles_Null_When_InputNull()
    {
        var result = TcpGatewayService.NormalizeMentionedRoles(
            raw: null,
            isGroup: true);

        Assert.Null(result);
    }

    [Fact]
    public void MentionedRoles_Null_When_InputEmpty()
    {
        var result = TcpGatewayService.NormalizeMentionedRoles(
            Array.Empty<string>(),
            isGroup: true);

        Assert.Null(result);
    }

    [Fact]
    public void MentionedRoles_TrimsWhitespace()
    {
        var result = TcpGatewayService.NormalizeMentionedRoles(
            ["  all  ", "admin"],
            isGroup: true);

        Assert.NotNull(result);
        Assert.Equal(["all", "admin"], result);
    }

    [Fact]
    public void MentionedRoles_RemovesBlankEntries()
    {
        var result = TcpGatewayService.NormalizeMentionedRoles(
            ["all", "  ", "", "admin"],
            isGroup: true);

        Assert.NotNull(result);
        Assert.Equal(["all", "admin"], result);
    }

    [Fact]
    public void MentionedRoles_Deduplicates()
    {
        var result = TcpGatewayService.NormalizeMentionedRoles(
            ["all", "admin", "all", "admin"],
            isGroup: true);

        Assert.NotNull(result);
        Assert.Equal(["all", "admin"], result);
    }

    [Fact]
    public void MentionedRoles_TruncatesLongEntryToLimit()
    {
        var longRole = new string('a', ChatMessageLimits.MaxMentionedRoleLength + 10);
        var result = TcpGatewayService.NormalizeMentionedRoles(
            [longRole],
            isGroup: true);

        Assert.NotNull(result);
        Assert.Single(result);
        Assert.Equal(ChatMessageLimits.MaxMentionedRoleLength, result[0].Length);
    }

    [Fact]
    public void MentionedRoles_TruncatesAtCountLimit()
    {
        var raw = new List<string>(ChatMessageLimits.MaxMentionedRoles + 5);
        for (var i = 0; i < ChatMessageLimits.MaxMentionedRoles + 5; i++)
            raw.Add($"role{i}");

        var result = TcpGatewayService.NormalizeMentionedRoles(
            raw,
            isGroup: true);

        Assert.NotNull(result);
        Assert.Equal(ChatMessageLimits.MaxMentionedRoles, result.Count);
    }

    [Fact]
    public void MentionedRoles_Null_When_AllFilteredOut()
    {
        var result = TcpGatewayService.NormalizeMentionedRoles(
            ["  ", ""],
            isGroup: true);

        Assert.Null(result);
    }

    [Fact]
    public void MentionedRoles_DeduplicatesAfterTrim()
    {
        var result = TcpGatewayService.NormalizeMentionedRoles(
            ["all", "  all  ", "ALL"],
            isGroup: true);

        Assert.NotNull(result);
        // "all" 和 "  all  " trim 后相同；"ALL" 大小写不同视为不同角色
        Assert.Equal(["all", "ALL"], result);
    }
}
