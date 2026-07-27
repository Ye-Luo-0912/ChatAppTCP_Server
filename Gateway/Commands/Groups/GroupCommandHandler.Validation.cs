using ChatApp.TcpGateway.Core.Messaging.Conversations;

namespace ChatApp.TcpGateway.Gateway.Commands.Groups;

/// <summary>
/// 群组命令的廉价结构校验。
/// <para>
/// 网关层只做"格式与边界"校验：字段非空、长度上限、数量上限、ID 为正、Enum 合法、列表去重。
/// 权限矩阵（Owner/Admin/Member 角色）与业务规则（最后一位 Owner 不可退群、不可移除自己等）
/// 仍由 RealtimeServices/持久层判定，网关不重复实现。
/// </para>
/// <para>
/// 校验失败统一返回 <c>invalid_request</c> 错误码，避免泄露具体规则到协议层。
/// </para>
/// </summary>
internal sealed partial class GroupCommandHandler
{
    /// <summary>RequestId 最大长度（与 Realtime 侧 DefaultGroupConversationProcessor.Validate 一致）。</summary>
    private const int MaxRequestIdLength = 64;

    /// <summary>ConversationId 最大长度（dm:lo:hi / group:guid 两种形态均远小于此）。</summary>
    private const int MaxConversationIdLength = 128;

    /// <summary>Title 最大长度（与 Realtime 侧一致）。</summary>
    private const int MaxTitleLength = 128;

    /// <summary>单次 AddMembers 命令的成员数量上限（与 Realtime MaxAddMembersPerRequest 一致）。</summary>
    private const int MaxAddMembersPerRequest = 50;

    /// <summary>单次 CreateGroup 命令的初始成员数量上限（含创建者，与 Realtime MaxMembersPerGroup 一致）。</summary>
    private const int MaxCreateGroupInitialMembers = 200;

    /// <summary>
    /// 校验 CreateGroup 请求的廉价结构。
    /// </summary>
    /// <returns>规范化后的成员列表（去重 + 正 ID + 含创建者）；null 表示校验失败。</returns>
    private static List<long>? ValidateCreateGroupRequest(
        string requestId,
        string title,
        IReadOnlyList<long>? memberUserIds,
        long actorUserId,
        out string errorMessage)
    {
        if (string.IsNullOrWhiteSpace(requestId) || requestId.Length > MaxRequestIdLength)
        {
            errorMessage = "创建群请求参数无效。";
            return null;
        }

        if (string.IsNullOrWhiteSpace(title) || title.Trim().Length > MaxTitleLength)
        {
            errorMessage = "创建群请求参数无效。";
            return null;
        }

        // 初始成员列表可为空（仅创建者）；非空时校验数量、正 ID、去重。
        // 创建者自动加入由 Realtime 侧完成，此处不预加入，只校验客户端显式传入的列表。
        List<long>? normalized = null;
        if (memberUserIds is { Count: > 0 })
        {
            // +1 给创建者（Realtime 侧会加入创建者）。
            if (memberUserIds.Count > MaxCreateGroupInitialMembers - 1)
            {
                errorMessage = "创建群请求参数无效。";
                return null;
            }

            normalized = NormalizeUserIds(memberUserIds, actorUserId);
            if (normalized is null)
            {
                errorMessage = "创建群请求参数无效。";
                return null;
            }
        }

        errorMessage = string.Empty;
        return normalized ?? [];
    }

    /// <summary>
    /// 校验 AddMembers 请求的廉价结构。
    /// </summary>
    /// <returns>规范化后的成员列表（去重 + 正 ID）；null 表示校验失败。</returns>
    private static List<long>? ValidateAddMembersRequest(
        string requestId,
        string conversationId,
        IReadOnlyList<long>? memberUserIds,
        out string errorMessage)
    {
        if (string.IsNullOrWhiteSpace(requestId) || requestId.Length > MaxRequestIdLength
            || string.IsNullOrWhiteSpace(conversationId)
            || conversationId.Length > MaxConversationIdLength)
        {
            errorMessage = "添加成员请求参数无效。";
            return null;
        }

        if (memberUserIds is not { Count: > 0 })
        {
            errorMessage = "添加成员请求参数无效。";
            return null;
        }

        if (memberUserIds.Count > MaxAddMembersPerRequest)
        {
            errorMessage = "添加成员请求参数无效。";
            return null;
        }

        var normalized = NormalizeUserIds(memberUserIds, excludeUserId: 0);
        if (normalized is null || normalized.Count == 0)
        {
            errorMessage = "添加成员请求参数无效。";
            return null;
        }

        errorMessage = string.Empty;
        return normalized;
    }

    /// <summary>
    /// 校验通用 Mutate 请求的廉价结构（RemoveMember / Leave / ChangeRole / ListMembers 共用）。
    /// </summary>
    private static bool ValidateMutateRequest(
        string requestId,
        string conversationId,
        out string errorMessage)
    {
        if (string.IsNullOrWhiteSpace(requestId) || requestId.Length > MaxRequestIdLength
            || string.IsNullOrWhiteSpace(conversationId)
            || conversationId.Length > MaxConversationIdLength)
        {
            errorMessage = "请求参数无效。";
            return false;
        }

        errorMessage = string.Empty;
        return true;
    }

    /// <summary>
    /// 校验 ChangeMemberRole 的 NewRole 枚举值合法性。
    /// 网关层只校验枚举值在已知范围内，权限校验（仅 Owner 可改角色等）由 Realtime 侧判定。
    /// </summary>
    private static bool IsValidMemberRole(ConversationMemberRole role) =>
        role is ConversationMemberRole.Owner
            or ConversationMemberRole.Admin
            or ConversationMemberRole.Member;

    /// <summary>
    /// 规范化用户 ID 列表：去重 + 仅保留正 ID。
    /// </summary>
    /// <param name="rawIds">客户端传入的原始列表。</param>
    /// <param name="excludeUserId">需要排除的 ID（如创建者自身，避免重复；传 0 表示不排除）。</param>
    /// <returns>去重后的正 ID 列表；输入含非法值时返回 null。</returns>
    private static List<long>? NormalizeUserIds(IReadOnlyList<long> rawIds, long excludeUserId)
    {
        // 用 HashSet 去重，避免 O(N^2) 扫描。
        var seen = new HashSet<long>(rawIds.Count);
        var result = new List<long>(rawIds.Count);
        foreach (var id in rawIds)
        {
            if (id <= 0)
                return null;
            if (id == excludeUserId)
                continue;
            if (seen.Add(id))
                result.Add(id);
        }
        return result;
    }
}
