using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Gateway.Commands.Groups;

namespace ChatApp.TcpGateway.Tests.Commands;

/// <summary>
/// 群成员列表 keyset 分页逻辑测试（P2-1）。
/// <para>
/// 验证 <see cref="GroupCommandHandler.PaginateMembers"/> 的首页、翻页、末页、
/// 空列表、页大小边界、无效游标降级等行为。
/// </para>
/// </summary>
public sealed class GroupMemberPaginationTests
{
    /// <summary>
    /// 构造测试成员列表，按 Realtime 排序规则（role ASC, joined_at_ms ASC, user_id ASC）。
    /// </summary>
    private static ConversationMemberItem[] BuildMembers(int count)
    {
        var members = new ConversationMemberItem[count];
        for (var i = 0; i < count; i++)
        {
            members[i] = new ConversationMemberItem
            {
                UserId = 1000 + i,
                // 交替角色，确保 role 优先排序
                Role = i < 2 ? ConversationMemberRole.Owner
                    : i < 5 ? ConversationMemberRole.Admin
                    : ConversationMemberRole.Member,
                JoinedAtMs = 1700000000000L + i
            };
        }
        return members;
    }

    [Fact]
    public void FirstPage_ReturnsFirstNMembers()
    {
        var all = BuildMembers(10);

        var (page, nextCursor, hasMore) = GroupCommandHandler.PaginateMembers(all, pageSize: 4, cursor: null);

        Assert.Equal(4, page.Length);
        Assert.True(hasMore);
        Assert.NotNull(nextCursor);
        Assert.Equal(all[0].UserId, page[0].UserId);
        Assert.Equal(all[3].UserId, page[3].UserId);
    }

    [Fact]
    public void SecondPage_UsesNextCursor_FromPreviousPage()
    {
        var all = BuildMembers(10);

        var (_, firstCursor, _) = GroupCommandHandler.PaginateMembers(all, pageSize: 4, cursor: null);
        var (secondPage, secondCursor, hasMore) =
            GroupCommandHandler.PaginateMembers(all, pageSize: 4, cursor: firstCursor);

        Assert.Equal(4, secondPage.Length);
        Assert.True(hasMore);
        Assert.NotNull(secondCursor);
        // 第二页应从 all[4] 开始
        Assert.Equal(all[4].UserId, secondPage[0].UserId);
        Assert.Equal(all[7].UserId, secondPage[3].UserId);
    }

    [Fact]
    public void LastPage_HasMoreFalse_AndNextCursorNull()
    {
        var all = BuildMembers(10);

        // 翻到最后一页：10 个成员，PageSize=4 → 第 3 页只有 2 个
        var (_, firstCursor, _) = GroupCommandHandler.PaginateMembers(all, pageSize: 4, cursor: null);
        var (_, secondCursor, _) = GroupCommandHandler.PaginateMembers(all, pageSize: 4, cursor: firstCursor);
        var (lastPage, nextCursor, hasMore) =
            GroupCommandHandler.PaginateMembers(all, pageSize: 4, cursor: secondCursor);

        Assert.Equal(2, lastPage.Length);
        Assert.False(hasMore);
        Assert.Null(nextCursor);
        Assert.Equal(all[8].UserId, lastPage[0].UserId);
        Assert.Equal(all[9].UserId, lastPage[1].UserId);
    }

    [Fact]
    public void EmptyList_ReturnsEmptyPage_NoCursor()
    {
        var all = Array.Empty<ConversationMemberItem>();

        var (page, nextCursor, hasMore) = GroupCommandHandler.PaginateMembers(all, pageSize: 10, cursor: null);

        Assert.Empty(page);
        Assert.False(hasMore);
        Assert.Null(nextCursor);
    }

    [Fact]
    public void PageSizeAboveMax_ClampedTo200()
    {
        // 构造 250 个成员，PageSize=300 应被 clamp 到 200
        var all = BuildMembers(250);

        var (page, _, hasMore) = GroupCommandHandler.PaginateMembers(all, pageSize: 300, cursor: null);

        Assert.Equal(200, page.Length);
        Assert.True(hasMore);
    }

    [Fact]
    public void DefaultPageSize_Is50_WhenNull()
    {
        var all = BuildMembers(80);

        var (page, _, hasMore) = GroupCommandHandler.PaginateMembers(all, pageSize: null, cursor: null);

        Assert.Equal(50, page.Length);
        Assert.True(hasMore);
    }

    [Fact]
    public void InvalidCursor_DelegatesToFirstPage()
    {
        var all = BuildMembers(10);

        // 无效 base64 / 格式错误 → 退化为首页
        var (page, _, _) = GroupCommandHandler.PaginateMembers(all, pageSize: 4, cursor: "not-a-valid-cursor!!!");

        Assert.Equal(4, page.Length);
        Assert.Equal(all[0].UserId, page[0].UserId);
    }

    [Fact]
    public void CursorBeyondEnd_ReturnsEmptyPage()
    {
        var all = BuildMembers(3);

        // 构造指向末尾成员的游标，验证"之后"返回空页。
        var lastMember = all[^1];
        var manualCursor = Convert.ToBase64String(
            System.Text.Encoding.UTF8.GetBytes(
                $"{(byte)lastMember.Role}.{lastMember.JoinedAtMs}.{lastMember.UserId}"));

        var (page, nextCursor, hasMore) =
            GroupCommandHandler.PaginateMembers(all, pageSize: 3, cursor: manualCursor);

        Assert.Empty(page);
        Assert.False(hasMore);
        Assert.Null(nextCursor);
    }

    [Fact]
    public void FullPaginationWalk_CoversAllMembers()
    {
        var all = BuildMembers(13);
        var collected = new List<long>(13);
        string? cursor = null;

        for (var i = 0; i < 10; i++) // 安全上限，防止无限循环
        {
            var (page, nextCursor, hasMore) = GroupCommandHandler.PaginateMembers(all, pageSize: 5, cursor: cursor);
            collected.AddRange(page.Select(p => p.UserId));
            cursor = nextCursor;
            if (!hasMore) break;
        }

        Assert.Equal(13, collected.Count);
        // 全量覆盖，无遗漏、无重复
        Assert.Equal(all.Select(m => m.UserId).ToHashSet(), collected.ToHashSet());
    }
}
