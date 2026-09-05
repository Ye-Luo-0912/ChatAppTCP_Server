// 共享规范类型一律经显式别名引用（Shared*/Tcp* 前缀），与本地 Core.Messaging 同名类型
// 严格区分：本文件同时引用两套命名空间，using 整个共享命名空间会产生大量二义性。
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Attachments;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using SharedAddGroupMembersRequest = ChatApp.Shared.Protocol.Tcp.TcpAddGroupMembersRequest;
using SharedAddGroupMembersResponse = ChatApp.Shared.Protocol.Tcp.TcpAddGroupMembersResponse;
using SharedAddReactionAcknowledgement = ChatApp.Shared.Protocol.Tcp.AddReactionAcknowledgement;
using SharedAddReactionRequest = ChatApp.Shared.Protocol.Tcp.AddReactionRequest;
using SharedAttachmentDownloadAuthorizeRequest = ChatApp.Shared.Protocol.Tcp.AttachmentDownloadAuthorizeRequest;
using SharedAttachmentDownloadAuthorizeResponse = ChatApp.Shared.Protocol.Tcp.AttachmentDownloadAuthorizeResponse;
using SharedAttachmentFinalizeRequest = ChatApp.Shared.Protocol.Tcp.AttachmentFinalizeRequest;
using SharedAttachmentFinalizeResponse = ChatApp.Shared.Protocol.Tcp.AttachmentFinalizeResponse;
using SharedAttachmentLifecycleChanged = ChatApp.Shared.Protocol.Tcp.AttachmentLifecycleChanged;
using SharedAuthenticationRequest = ChatApp.Shared.Protocol.Tcp.AuthenticationRequest;
using SharedAuthenticationResponse = ChatApp.Shared.Protocol.Tcp.AuthenticationResponse;
using SharedChangeMemberRoleRequest = ChatApp.Shared.Protocol.Tcp.TcpChangeMemberRoleRequest;
using SharedChangeMemberRoleResponse = ChatApp.Shared.Protocol.Tcp.TcpChangeMemberRoleResponse;
using SharedChatMessage = ChatApp.Shared.Protocol.Tcp.ChatMessage;
using SharedConversationChangedUpdate = ChatApp.Shared.Protocol.Tcp.ConversationChangedUpdate;
using SharedConversationDissolvedUpdate = ChatApp.Shared.Protocol.Tcp.TcpConversationDissolvedUpdate;
using SharedConversationListPage = ChatApp.Shared.Protocol.Tcp.ConversationListPage;
using SharedConversationListRequest = ChatApp.Shared.Protocol.Tcp.ConversationListRequest;
using SharedConversationReadUpdate = ChatApp.Shared.Protocol.Tcp.ConversationReadUpdate;
using SharedCreateGroupRequest = ChatApp.Shared.Protocol.Tcp.TcpCreateGroupRequest;
using SharedCreateGroupResponse = ChatApp.Shared.Protocol.Tcp.TcpCreateGroupResponse;
using SharedDissolveGroupRequest = ChatApp.Shared.Protocol.Tcp.TcpDissolveGroupRequest;
using SharedDissolveGroupResponse = ChatApp.Shared.Protocol.Tcp.TcpDissolveGroupResponse;
using SharedLeaveGroupRequest = ChatApp.Shared.Protocol.Tcp.TcpLeaveGroupRequest;
using SharedLeaveGroupResponse = ChatApp.Shared.Protocol.Tcp.TcpLeaveGroupResponse;
using SharedListGroupMembersRequest = ChatApp.Shared.Protocol.Tcp.TcpListGroupMembersRequest;
using SharedListGroupMembersResponse = ChatApp.Shared.Protocol.Tcp.TcpListGroupMembersResponse;
using SharedMarkReadRequest = ChatApp.Shared.Protocol.Tcp.ConversationMarkReadRequest;
using SharedMarkReadResponse = ChatApp.Shared.Protocol.Tcp.ConversationMarkReadResponse;
using SharedMemberJoinedUpdate = ChatApp.Shared.Protocol.Tcp.TcpMemberJoinedUpdate;
using SharedMemberLeftUpdate = ChatApp.Shared.Protocol.Tcp.TcpMemberLeftUpdate;
using SharedMemberRemovedUpdate = ChatApp.Shared.Protocol.Tcp.TcpMemberRemovedUpdate;
using SharedMessageAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageAcknowledgement;
using SharedMessageEditAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageEditAcknowledgement;
using SharedMessageEditedUpdate = ChatApp.Shared.Protocol.Tcp.MessageEditedUpdate;
using SharedMessageEditRequest = ChatApp.Shared.Protocol.Tcp.MessageEditRequest;
using SharedMessageReadReceiptItem = ChatApp.Shared.Protocol.Tcp.MessageReadReceiptItem;
using SharedReadReceiptQueryRequest = ChatApp.Shared.Protocol.Tcp.MessageReadReceiptQueryRequest;
using SharedReadReceiptQueryResponse = ChatApp.Shared.Protocol.Tcp.MessageReadReceiptQueryResponse;
using SharedMessageReadReceiptQueryRequest = ChatApp.Shared.Protocol.Tcp.MessageReadReceiptQueryRequest;
using SharedMessageReadReceiptQueryResponse = ChatApp.Shared.Protocol.Tcp.MessageReadReceiptQueryResponse;
using SharedMessageReceipt = ChatApp.Shared.Protocol.Tcp.MessageReceipt;
using SharedMessageReceiptAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageReceiptAcknowledgement;
using SharedMessageReceiptUpdated = ChatApp.Shared.Protocol.Tcp.MessageReceiptUpdated;
using SharedMessageRecallAcknowledgement = ChatApp.Shared.Protocol.Tcp.MessageRecallAcknowledgement;
using SharedMessageRecalledUpdate = ChatApp.Shared.Protocol.Tcp.MessageRecalledUpdate;
using SharedMessageRecallRequest = ChatApp.Shared.Protocol.Tcp.MessageRecallRequest;
using SharedMembersAddedUpdate = ChatApp.Shared.Protocol.Tcp.TcpMembersAddedUpdate;
using SharedPresenceChanged = ChatApp.Shared.Protocol.Tcp.TcpPresenceChanged;
using SharedPresenceQueryRequest = ChatApp.Shared.Protocol.Tcp.TcpPresenceQueryRequest;
using SharedPresenceSnapshotItem = ChatApp.Shared.Protocol.Tcp.TcpPresenceSnapshotItem;
using SharedPresenceSnapshotResponse = ChatApp.Shared.Protocol.Tcp.TcpPresenceSnapshotResponse;
using SharedPresenceUnwatchRequest = ChatApp.Shared.Protocol.Tcp.TcpPresenceUnwatchRequest;
using SharedReactionAddedUpdate = ChatApp.Shared.Protocol.Tcp.ReactionAddedUpdate;
using SharedReactionRemovedUpdate = ChatApp.Shared.Protocol.Tcp.ReactionRemovedUpdate;
using SharedRegisterPushTokenRequest = ChatApp.Shared.Protocol.Tcp.TcpRegisterPushTokenRequest;
using SharedRegisterPushTokenResponse = ChatApp.Shared.Protocol.Tcp.TcpRegisterPushTokenResponse;
using SharedRelationshipCommandRequest = ChatApp.Shared.Protocol.Tcp.TcpRelationshipCommandRequest;
using SharedRelationshipCommandResponse = ChatApp.Shared.Protocol.Tcp.TcpRelationshipCommandResponse;
using SharedRelationshipListChangedUpdate = ChatApp.Shared.Protocol.Tcp.TcpRelationshipListChangedUpdate;
using SharedRemoveGroupMemberRequest = ChatApp.Shared.Protocol.Tcp.TcpRemoveGroupMemberRequest;
using SharedRemoveGroupMemberResponse = ChatApp.Shared.Protocol.Tcp.TcpRemoveGroupMemberResponse;
using SharedRemoveReactionAcknowledgement = ChatApp.Shared.Protocol.Tcp.RemoveReactionAcknowledgement;
using SharedRemoveReactionRequest = ChatApp.Shared.Protocol.Tcp.RemoveReactionRequest;
using SharedRoleChangedUpdate = ChatApp.Shared.Protocol.Tcp.TcpRoleChangedUpdate;
using SharedSetPrefsRequest = ChatApp.Shared.Protocol.Tcp.ConversationSetPrefsRequest;
using SharedSetPrefsResponse = ChatApp.Shared.Protocol.Tcp.ConversationSetPrefsResponse;
using SharedTcpTypingNotify = ChatApp.Shared.Protocol.Tcp.TcpTypingNotify;
using SharedTcpTypingUpdate = ChatApp.Shared.Protocol.Tcp.TcpTypingUpdate;
using SharedUnregisterPushTokenRequest = ChatApp.Shared.Protocol.Tcp.TcpUnregisterPushTokenRequest;
using SharedUnregisterPushTokenResponse = ChatApp.Shared.Protocol.Tcp.TcpUnregisterPushTokenResponse;
using SharedUnreadCountChanged = ChatApp.Shared.Protocol.Tcp.UnreadCountChanged;
using TcpAttachmentRef = ChatApp.Shared.Protocol.Tcp.TcpAttachmentRef;
using TcpConversationListCursor = ChatApp.Shared.Protocol.Tcp.TcpConversationListCursor;
using TcpConversationListItem = ChatApp.Shared.Protocol.Tcp.TcpConversationListItem;
using TcpConversationMemberItem = ChatApp.Shared.Protocol.Tcp.TcpConversationMemberItem;
using TcpConversationType = ChatApp.Shared.Protocol.Tcp.TcpConversationType;
using TcpGroupMemberRole = ChatApp.Shared.Protocol.Tcp.TcpGroupMemberRole;
using TcpPushPlatform = ChatApp.Shared.Protocol.Tcp.TcpPushPlatform;
using TcpRelationshipListRequest = ChatApp.Shared.Protocol.Tcp.TcpRelationshipListRequest;
using TcpRelationshipListResponse = ChatApp.Shared.Protocol.Tcp.TcpRelationshipListResponse;
using TcpRelationshipOperation = ChatApp.Shared.Protocol.Tcp.TcpRelationshipOperation;
// PushPlatform（Realtime.Abstractions）未进入项目级 global using，本地与共享平台枚举都需显式别名。
using PushPlatform = ChatApp.Realtime.Abstractions.Push.PushPlatform;

namespace ChatApp.TcpGateway.Gateway.Serialization;

/// <summary>
/// 网关本地 DTO（Core/Messaging/**，含 Realtime.Abstractions 契约类型）↔ chatapp-bin-v1
/// 共享规范 DTO（ChatApp.Protocol.Tcp）的双向映射层，仅在连接协商为二进制载荷时使用：
/// 出站 <see cref="OutboundFrameFactory.CreateBinary"/> 先 <see cref="ToShared"/> 再由
/// TcpBinaryWireEncoder 编码；入站 <see cref="SessionPayload.Deserialize{T}}"/> 解码出共享 DTO
/// 后经 <see cref="ToLocal{T}}"/> 转回本地 DTO，保证两种格式下 handler 看到同一本地类型。
/// <para>
/// wire 类型本身就是共享类型的命令（握手帧、MessageHistoryRequest/Response、
/// SyncBootstrapRequest/Response、TcpCall*、TcpRelationshipList*、GoAway、ProtocolErrorFrame）
/// 在本层恒等通过。约定与客户端侧 BinaryPayloadMapper 一致：DateTime(UTC) ↔ Unix 毫秒；
/// 枚举两侧数值一致按数值映射；共享有本地无的字段置默认，本地有共享无的字段丢弃（逐处注释）。
/// 未覆盖的命令/类型 fail-closed 抛 <see cref="InvalidOperationException"/>。
/// </para>
/// </summary>
internal static partial class BinaryPayloadMapper
{
    // ──────────── 出站：本地 DTO → 共享规范 DTO ────────────

    /// <summary>
    /// 出站二进制编码前的统一分发：按命令把网关本地 DTO 转共享规范 DTO。
    /// wire 类型即共享类型的命令恒等返回；未覆盖的命令 fail-closed 抛出，
    /// 绝不把本地 DTO 直接交给寄存器（寄存器按具体类型分发，未知类型一律 SchemaNotCovered）。
    /// </summary>
    public static object ToShared(PacketCommand command, object localValue) => command switch
    {
        // 认证
        PacketCommand.AuthenticationRequest => ToShared(Require<AuthenticationRequest>(command, localValue)),
        PacketCommand.AuthenticationResponse => ToShared(Require<AuthenticationResponse>(command, localValue)),

        // 消息 / 回执 / 编辑 / 撤回 / 反应
        PacketCommand.ChatMessage => ToShared(Require<ChatMessage>(command, localValue)),
        PacketCommand.MessageAcknowledgement => ToShared(Require<MessageAcknowledgement>(command, localValue)),
        PacketCommand.MessageReceipt => ToShared(Require<MessageReceiptRequest>(command, localValue)),
        PacketCommand.MessageReceiptAcknowledgement => ToShared(Require<MessageReceiptAcknowledgement>(command, localValue)),
        PacketCommand.MessageReceiptUpdated => ToShared(Require<MessageReceiptUpdate>(command, localValue)),
        PacketCommand.MessageEditRequest => ToShared(Require<MessageEditRequest>(command, localValue)),
        PacketCommand.MessageEditAck => ToShared(Require<MessageEditAcknowledgement>(command, localValue)),
        PacketCommand.MessageEdited => ToShared(Require<MessageEditedUpdate>(command, localValue)),
        PacketCommand.MessageRecallRequest => ToShared(Require<MessageRecallRequest>(command, localValue)),
        PacketCommand.MessageRecallAck => ToShared(Require<MessageRecallAcknowledgement>(command, localValue)),
        PacketCommand.MessageRecalled => ToShared(Require<MessageRecalledUpdate>(command, localValue)),
        PacketCommand.AddReactionRequest => ToShared(Require<AddReactionRequest>(command, localValue)),
        PacketCommand.AddReactionAck => ToShared(Require<AddReactionAcknowledgement>(command, localValue)),
        PacketCommand.ReactionAdded => ToShared(Require<ReactionAddedUpdate>(command, localValue)),
        PacketCommand.RemoveReactionRequest => ToShared(Require<RemoveReactionRequest>(command, localValue)),
        PacketCommand.RemoveReactionAck => ToShared(Require<RemoveReactionAcknowledgement>(command, localValue)),
        PacketCommand.ReactionRemoved => ToShared(Require<ReactionRemovedUpdate>(command, localValue)),

        // 会话列表 / 已读 / 偏好 / 群组
        PacketCommand.ConversationListRequest => ToShared(Require<ConversationListRequest>(command, localValue)),
        PacketCommand.ConversationListPage => ToShared(Require<ConversationListResponse>(command, localValue)),
        PacketCommand.ConversationMarkReadRequest => ToShared(Require<ConversationMarkReadRequest>(command, localValue)),
        PacketCommand.ConversationMarkReadResponse => ToShared(Require<ConversationMarkReadResponse>(command, localValue)),
        PacketCommand.ConversationChanged => ToShared(Require<ConversationChanged>(command, localValue)),
        PacketCommand.UnreadCountChanged => ToShared(Require<UnreadCountChanged>(command, localValue)),
        PacketCommand.ConversationSetPrefsRequest => ToShared(Require<ConversationSetPrefsRequest>(command, localValue)),
        PacketCommand.ConversationSetPrefsResponse => ToShared(Require<ConversationSetPrefsResponse>(command, localValue)),
        PacketCommand.ConversationRead => ToShared(Require<ConversationReadUpdate>(command, localValue)),
        PacketCommand.MessageReadReceiptQueryRequest => ToShared(Require<MessageReadReceiptQueryRequest>(command, localValue)),
        PacketCommand.MessageReadReceiptQueryResponse => ToShared(Require<MessageReadReceiptQueryResponse>(command, localValue)),
        PacketCommand.CreateGroupRequest => ToShared(Require<CreateGroupRequest>(command, localValue)),
        PacketCommand.CreateGroupResponse => ToShared(Require<CreateGroupResponse>(command, localValue)),
        PacketCommand.AddGroupMembersRequest => ToShared(Require<AddGroupMembersRequest>(command, localValue)),
        PacketCommand.AddGroupMembersResponse => ToShared(Require<AddGroupMembersResponse>(command, localValue)),
        PacketCommand.RemoveGroupMemberRequest => ToShared(Require<RemoveGroupMemberRequest>(command, localValue)),
        PacketCommand.RemoveGroupMemberResponse => ToShared(Require<RemoveGroupMemberResponse>(command, localValue)),
        PacketCommand.LeaveGroupRequest => ToShared(Require<LeaveGroupRequest>(command, localValue)),
        PacketCommand.LeaveGroupResponse => ToShared(Require<LeaveGroupResponse>(command, localValue)),
        PacketCommand.ChangeMemberRoleRequest => ToShared(Require<ChangeMemberRoleRequest>(command, localValue)),
        PacketCommand.ChangeMemberRoleResponse => ToShared(Require<ChangeMemberRoleResponse>(command, localValue)),
        PacketCommand.ListGroupMembersRequest => ToShared(Require<ListGroupMembersRequest>(command, localValue)),
        PacketCommand.ListGroupMembersResponse => ToShared(Require<ListGroupMembersResponse>(command, localValue)),
        PacketCommand.MemberJoined => ToShared(Require<MemberJoinedUpdate>(command, localValue)),
        PacketCommand.MemberLeft => ToShared(Require<MemberLeftUpdate>(command, localValue)),
        PacketCommand.MemberRemoved => ToShared(Require<MemberRemovedUpdate>(command, localValue)),
        PacketCommand.RoleChanged => ToShared(Require<RoleChangedUpdate>(command, localValue)),
        PacketCommand.MembersAddedUpdate => ToShared(Require<MembersAddedUpdate>(command, localValue)),
        PacketCommand.ConversationDissolvedUpdate => ToShared(Require<ConversationDissolvedUpdate>(command, localValue)),
        PacketCommand.DissolveGroupRequest => ToShared(Require<DissolveGroupRequest>(command, localValue)),
        PacketCommand.DissolveGroupResponse => ToShared(Require<DissolveGroupResponse>(command, localValue)),

        // 关系
        PacketCommand.RelationshipCommandRequest => ToShared(Require<RelationshipCommandRequest>(command, localValue)),
        PacketCommand.RelationshipCommandResponse => ToShared(Require<RelationshipCommandResponse>(command, localValue)),
        PacketCommand.RelationshipListChanged => ToShared(Require<RelationshipListChangedUpdate>(command, localValue)),

        // 在线 / 输入 / 推送
        PacketCommand.TypingNotify => ToShared(Require<TypingNotify>(command, localValue)),
        PacketCommand.TypingUpdate => ToShared(Require<TypingUpdate>(command, localValue)),
        PacketCommand.PresenceQuery => ToShared(Require<PresenceQueryRequest>(command, localValue)),
        PacketCommand.PresenceSnapshot => ToShared(Require<PresenceSnapshotResponse>(command, localValue)),
        PacketCommand.PresenceChanged => ToShared(Require<PresenceChanged>(command, localValue)),
        PacketCommand.PresenceUnwatch => ToShared(Require<PresenceUnwatchRequest>(command, localValue)),
        PacketCommand.RegisterPushTokenRequest => ToShared(Require<RegisterPushTokenRequest>(command, localValue)),
        PacketCommand.RegisterPushTokenResponse => ToShared(Require<RegisterPushTokenResponse>(command, localValue)),
        PacketCommand.UnregisterPushTokenRequest => ToShared(Require<UnregisterPushTokenRequest>(command, localValue)),
        PacketCommand.UnregisterPushTokenResponse => ToShared(Require<UnregisterPushTokenResponse>(command, localValue)),

        // 附件
        PacketCommand.AttachmentLifecycleChanged => ToShared(Require<AttachmentLifecycleUpdate>(command, localValue)),
        PacketCommand.AttachmentFinalizeRequest => ToShared(Require<AttachmentFinalizeRequest>(command, localValue)),
        PacketCommand.AttachmentFinalizeResponse => ToShared(Require<AttachmentFinalizeResponse>(command, localValue)),
        PacketCommand.AttachmentDownloadAuthorizeRequest => ToShared(Require<AttachmentDownloadAuthorizeRequest>(command, localValue)),
        PacketCommand.AttachmentDownloadAuthorizeResponse => ToShared(Require<AttachmentDownloadAuthorizeResponse>(command, localValue)),

        // ───── wire 类型即共享类型的命令：恒等通过 ─────
        PacketCommand.MessageHistoryRequest => Require<MessageHistoryRequest>(command, localValue),
        PacketCommand.MessageHistoryPage => Require<MessageHistoryResponse>(command, localValue),
        PacketCommand.SyncBootstrapRequest => Require<SyncBootstrapRequest>(command, localValue),
        PacketCommand.SyncBootstrapResponse => Require<SyncBootstrapResponse>(command, localValue),
        PacketCommand.CallCommandRequest => Require<TcpCallCommandRequest>(command, localValue),
        PacketCommand.CallCommandResponse => Require<TcpCallCommandResponse>(command, localValue),
        PacketCommand.CallSignal => Require<TcpCallSignal>(command, localValue),
        PacketCommand.RelationshipListRequest => Require<TcpRelationshipListRequest>(command, localValue),
        PacketCommand.RelationshipListResponse => Require<TcpRelationshipListResponse>(command, localValue),
        PacketCommand.Error => Require<ProtocolErrorFrame>(command, localValue),
        PacketCommand.GoAway => Require<GoAway>(command, localValue),

        // 握手段（ClientHello/ServerHello/ResumeResponse）始终 JSON、Heartbeat 恒为空载荷，
        // 都不经本层；未覆盖命令 fail-closed。
        _ => throw Unmapped(command, localValue)
    };

    // ──────────── 入站：共享规范 DTO → 本地 DTO ────────────

    /// <summary>
    /// 入站二进制解码后的统一分发：把共享规范 DTO 转回网关本地 DTO。
    /// <paramref name="sharedValue"/> 为 null（仅理论可能：寄存器成功分支保证非 null）时返回 null，
    /// 交由调用方按"载荷无效"处理；未覆盖的命令或类型不匹配一律 fail-closed 抛出。
    /// </summary>
    public static T? ToLocal<T>(PacketCommand command, object? sharedValue)
        where T : class
    {
        if (sharedValue is null)
        {
            return null;
        }

        var local = ToLocalCore(command, sharedValue);
        return (T)local;
    }

    private static object ToLocalCore(PacketCommand command, object sharedValue) => command switch
    {
        PacketCommand.AuthenticationRequest => ToLocal(Require<SharedAuthenticationRequest>(command, sharedValue)),
        PacketCommand.AuthenticationResponse => ToLocal(Require<SharedAuthenticationResponse>(command, sharedValue)),
        PacketCommand.ChatMessage => ToLocal(Require<SharedChatMessage>(command, sharedValue)),
        PacketCommand.MessageAcknowledgement => ToLocal(Require<SharedMessageAcknowledgement>(command, sharedValue)),
        PacketCommand.MessageReceipt => ToLocal(Require<SharedMessageReceipt>(command, sharedValue)),
        PacketCommand.MessageReceiptAcknowledgement => ToLocal(Require<SharedMessageReceiptAcknowledgement>(command, sharedValue)),
        PacketCommand.MessageReceiptUpdated => ToLocal(Require<SharedMessageReceiptUpdated>(command, sharedValue)),
        PacketCommand.MessageEditRequest => ToLocal(Require<SharedMessageEditRequest>(command, sharedValue)),
        PacketCommand.MessageEditAck => ToLocal(Require<SharedMessageEditAcknowledgement>(command, sharedValue)),
        PacketCommand.MessageEdited => ToLocal(Require<SharedMessageEditedUpdate>(command, sharedValue)),
        PacketCommand.MessageRecallRequest => ToLocal(Require<SharedMessageRecallRequest>(command, sharedValue)),
        PacketCommand.MessageRecallAck => ToLocal(Require<SharedMessageRecallAcknowledgement>(command, sharedValue)),
        PacketCommand.MessageRecalled => ToLocal(Require<SharedMessageRecalledUpdate>(command, sharedValue)),
        PacketCommand.AddReactionRequest => ToLocal(Require<SharedAddReactionRequest>(command, sharedValue)),
        PacketCommand.AddReactionAck => ToLocal(Require<SharedAddReactionAcknowledgement>(command, sharedValue)),
        PacketCommand.ReactionAdded => ToLocal(Require<SharedReactionAddedUpdate>(command, sharedValue)),
        PacketCommand.RemoveReactionRequest => ToLocal(Require<SharedRemoveReactionRequest>(command, sharedValue)),
        PacketCommand.RemoveReactionAck => ToLocal(Require<SharedRemoveReactionAcknowledgement>(command, sharedValue)),
        PacketCommand.ReactionRemoved => ToLocal(Require<SharedReactionRemovedUpdate>(command, sharedValue)),
        PacketCommand.ConversationListRequest => ToLocal(Require<SharedConversationListRequest>(command, sharedValue)),
        PacketCommand.ConversationListPage => ToLocal(Require<SharedConversationListPage>(command, sharedValue)),
        PacketCommand.ConversationMarkReadRequest => ToLocal(Require<SharedMarkReadRequest>(command, sharedValue)),
        PacketCommand.ConversationMarkReadResponse => ToLocal(Require<SharedMarkReadResponse>(command, sharedValue)),
        PacketCommand.ConversationChanged => ToLocal(Require<SharedConversationChangedUpdate>(command, sharedValue)),
        PacketCommand.UnreadCountChanged => ToLocal(Require<SharedUnreadCountChanged>(command, sharedValue)),
        PacketCommand.ConversationSetPrefsRequest => ToLocal(Require<SharedSetPrefsRequest>(command, sharedValue)),
        PacketCommand.ConversationSetPrefsResponse => ToLocal(Require<SharedSetPrefsResponse>(command, sharedValue)),
        PacketCommand.ConversationRead => ToLocal(Require<SharedConversationReadUpdate>(command, sharedValue)),
        PacketCommand.MessageReadReceiptQueryRequest => ToLocal(Require<SharedReadReceiptQueryRequest>(command, sharedValue)),
        PacketCommand.MessageReadReceiptQueryResponse => ToLocal(Require<SharedReadReceiptQueryResponse>(command, sharedValue)),
        PacketCommand.CreateGroupRequest => ToLocal(Require<SharedCreateGroupRequest>(command, sharedValue)),
        PacketCommand.CreateGroupResponse => ToLocal(Require<SharedCreateGroupResponse>(command, sharedValue)),
        PacketCommand.AddGroupMembersRequest => ToLocal(Require<SharedAddGroupMembersRequest>(command, sharedValue)),
        PacketCommand.AddGroupMembersResponse => ToLocal(Require<SharedAddGroupMembersResponse>(command, sharedValue)),
        PacketCommand.RemoveGroupMemberRequest => ToLocal(Require<SharedRemoveGroupMemberRequest>(command, sharedValue)),
        PacketCommand.RemoveGroupMemberResponse => ToLocal(Require<SharedRemoveGroupMemberResponse>(command, sharedValue)),
        PacketCommand.LeaveGroupRequest => ToLocal(Require<SharedLeaveGroupRequest>(command, sharedValue)),
        PacketCommand.LeaveGroupResponse => ToLocal(Require<SharedLeaveGroupResponse>(command, sharedValue)),
        PacketCommand.ChangeMemberRoleRequest => ToLocal(Require<SharedChangeMemberRoleRequest>(command, sharedValue)),
        PacketCommand.ChangeMemberRoleResponse => ToLocal(Require<SharedChangeMemberRoleResponse>(command, sharedValue)),
        PacketCommand.ListGroupMembersRequest => ToLocal(Require<SharedListGroupMembersRequest>(command, sharedValue)),
        PacketCommand.ListGroupMembersResponse => ToLocal(Require<SharedListGroupMembersResponse>(command, sharedValue)),
        PacketCommand.MemberJoined => ToLocal(Require<SharedMemberJoinedUpdate>(command, sharedValue)),
        PacketCommand.MemberLeft => ToLocal(Require<SharedMemberLeftUpdate>(command, sharedValue)),
        PacketCommand.MemberRemoved => ToLocal(Require<SharedMemberRemovedUpdate>(command, sharedValue)),
        PacketCommand.RoleChanged => ToLocal(Require<SharedRoleChangedUpdate>(command, sharedValue)),
        PacketCommand.MembersAddedUpdate => ToLocal(Require<SharedMembersAddedUpdate>(command, sharedValue)),
        PacketCommand.ConversationDissolvedUpdate => ToLocal(Require<SharedConversationDissolvedUpdate>(command, sharedValue)),
        PacketCommand.DissolveGroupRequest => ToLocal(Require<SharedDissolveGroupRequest>(command, sharedValue)),
        PacketCommand.DissolveGroupResponse => ToLocal(Require<SharedDissolveGroupResponse>(command, sharedValue)),
        PacketCommand.RelationshipCommandRequest => ToLocal(Require<SharedRelationshipCommandRequest>(command, sharedValue)),
        PacketCommand.RelationshipCommandResponse => ToLocal(Require<SharedRelationshipCommandResponse>(command, sharedValue)),
        PacketCommand.RelationshipListChanged => ToLocal(Require<SharedRelationshipListChangedUpdate>(command, sharedValue)),
        PacketCommand.TypingNotify => ToLocal(Require<SharedTcpTypingNotify>(command, sharedValue)),
        PacketCommand.TypingUpdate => ToLocal(Require<SharedTcpTypingUpdate>(command, sharedValue)),
        PacketCommand.PresenceQuery => ToLocal(Require<SharedPresenceQueryRequest>(command, sharedValue)),
        PacketCommand.PresenceSnapshot => ToLocal(Require<SharedPresenceSnapshotResponse>(command, sharedValue)),
        PacketCommand.PresenceChanged => ToLocal(Require<SharedPresenceChanged>(command, sharedValue)),
        PacketCommand.PresenceUnwatch => ToLocal(Require<SharedPresenceUnwatchRequest>(command, sharedValue)),
        PacketCommand.RegisterPushTokenRequest => ToLocal(Require<SharedRegisterPushTokenRequest>(command, sharedValue)),
        PacketCommand.RegisterPushTokenResponse => ToLocal(Require<SharedRegisterPushTokenResponse>(command, sharedValue)),
        PacketCommand.UnregisterPushTokenRequest => ToLocal(Require<SharedUnregisterPushTokenRequest>(command, sharedValue)),
        PacketCommand.UnregisterPushTokenResponse => ToLocal(Require<SharedUnregisterPushTokenResponse>(command, sharedValue)),
        PacketCommand.AttachmentLifecycleChanged => ToLocal(Require<SharedAttachmentLifecycleChanged>(command, sharedValue)),
        PacketCommand.AttachmentFinalizeRequest => ToLocal(Require<SharedAttachmentFinalizeRequest>(command, sharedValue)),
        PacketCommand.AttachmentFinalizeResponse => ToLocal(Require<SharedAttachmentFinalizeResponse>(command, sharedValue)),
        PacketCommand.AttachmentDownloadAuthorizeRequest => ToLocal(Require<SharedAttachmentDownloadAuthorizeRequest>(command, sharedValue)),
        PacketCommand.AttachmentDownloadAuthorizeResponse => ToLocal(Require<SharedAttachmentDownloadAuthorizeResponse>(command, sharedValue)),

        // wire 类型即共享类型的命令：恒等返回（含恒等入站）。
        PacketCommand.MessageHistoryRequest => Require<MessageHistoryRequest>(command, sharedValue),
        PacketCommand.MessageHistoryPage => Require<MessageHistoryResponse>(command, sharedValue),
        PacketCommand.SyncBootstrapRequest => Require<SyncBootstrapRequest>(command, sharedValue),
        PacketCommand.SyncBootstrapResponse => Require<SyncBootstrapResponse>(command, sharedValue),
        PacketCommand.CallCommandRequest => Require<TcpCallCommandRequest>(command, sharedValue),
        PacketCommand.CallCommandResponse => Require<TcpCallCommandResponse>(command, sharedValue),
        PacketCommand.CallSignal => Require<TcpCallSignal>(command, sharedValue),
        PacketCommand.RelationshipListRequest => Require<TcpRelationshipListRequest>(command, sharedValue),
        PacketCommand.RelationshipListResponse => Require<TcpRelationshipListResponse>(command, sharedValue),
        PacketCommand.Error => Require<ProtocolErrorFrame>(command, sharedValue),
        PacketCommand.GoAway => Require<GoAway>(command, sharedValue),

        // 握手段（ClientHello/ServerHello/ResumeResponse）始终 JSON、Heartbeat 恒为空载荷，
        // 都不经本层；未覆盖命令 fail-closed。
        _ => throw Unmapped(command, sharedValue)
    };

    // ──────────── 公共小工具 ────────────

    /// <summary>按命令校验载荷具体类型；类型不符属编程错误/恶意构造，fail-closed。</summary>
    private static T Require<T>(PacketCommand command, object value)
        where T : class =>
        value is T typed
            ? typed
            : throw Unmapped(command, value);

    private static InvalidOperationException Unmapped(PacketCommand command, object value) =>
        new($"二进制载荷映射未覆盖命令 {command} 的载荷类型 {value.GetType().FullName}，fail-closed 拒绝编解码。");

    /// <summary>UTC DateTime → Unix 毫秒。Unspecified 按 UTC 处理（与网关时间源约定一致），仅 Local 做换算。</summary>
    private static long ToUnixMs(DateTime value) =>
        (value.Kind == DateTimeKind.Local
            ? new DateTimeOffset(value.ToUniversalTime())
            : new DateTimeOffset(DateTime.SpecifyKind(value, DateTimeKind.Utc)))
        .ToUnixTimeMilliseconds();

    private static DateTime FromUnixMs(long ms) =>
        DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;

    /// <summary>群成员角色两侧枚举数值一致（Owner=1/Admin=2/Member=3），按数值映射。</summary>
    private static TcpGroupMemberRole ToSharedRole(ConversationMemberRole role) => (TcpGroupMemberRole)(byte)role;

    private static ConversationMemberRole ToLocalRole(TcpGroupMemberRole role) => (ConversationMemberRole)(byte)role;

    /// <summary>关系操作两侧枚举数值一致（SendFriendRequest=1 … UnblockUser=6），按数值映射。</summary>
    private static TcpRelationshipOperation ToSharedOperation(RelationshipOperation operation) =>
        (TcpRelationshipOperation)(byte)operation;

    private static RelationshipOperation ToLocalOperation(TcpRelationshipOperation operation) =>
        (RelationshipOperation)(byte)operation;

    /// <summary>会话类型两侧数值一致（Direct=1/Group=2）。共享侧另有 Unknown=0，
    /// 网关不会产出；解码遇到 Unknown 时回退 Direct，避免把未定义枚举值带进本地 DTO。</summary>
    private static TcpConversationType ToSharedConversationType(ConversationType type) =>
        (TcpConversationType)(byte)type;

    private static ConversationType ToLocalConversationType(TcpConversationType type) =>
        type == TcpConversationType.Group ? ConversationType.Group : ConversationType.Direct;

    /// <summary>推送平台两侧数值一致（Fcm=1/Apns=2/WebPush=3），按数值映射。</summary>
    private static TcpPushPlatform ToSharedPlatform(PushPlatform platform) => (TcpPushPlatform)(byte)platform;

    private static PushPlatform ToLocalPlatform(TcpPushPlatform platform) => (PushPlatform)(byte)platform;

    private static TcpAttachmentRef[]? MapAttachmentRefs(IReadOnlyList<AttachmentRef>? refs)
    {
        if (refs is null)
        {
            return null;
        }

        var mapped = new TcpAttachmentRef[refs.Count];
        for (var i = 0; i < refs.Count; i++)
        {
            var r = refs[i];
            mapped[i] = new TcpAttachmentRef
            {
                RefVersion = r.RefVersion,
                AttachmentId = r.AttachmentId,
                FileName = r.FileName,
                ContentType = r.ContentType,
                SizeBytes = r.SizeBytes,
                Status = (short)r.Status,
                DownloadApiHint = r.DownloadApiHint,
                DownloadToken = r.DownloadToken,
                ThumbnailApiHint = r.ThumbnailApiHint,
                IsVoice = r.IsVoice,
                VoiceCodec = r.VoiceCodec,
                VoiceContainer = r.VoiceContainer,
                VoiceDurationMs = r.VoiceDurationMs,
                VoiceSampleRateHz = r.VoiceSampleRateHz,
                VoiceChannels = r.VoiceChannels,
                // VOICE-MSG-2 waveform：语音波形峰值包络透传（缺省/空 = 无波形）。
                VoiceWaveformPeaks = r.VoiceWaveformPeaks
            };
        }

        return mapped;
    }

    private static AttachmentRef[]? MapAttachmentRefs(IReadOnlyList<TcpAttachmentRef>? refs)
    {
        if (refs is null)
        {
            return null;
        }

        var mapped = new AttachmentRef[refs.Count];
        for (var i = 0; i < refs.Count; i++)
        {
            var r = refs[i];
            mapped[i] = new AttachmentRef
            {
                RefVersion = r.RefVersion,
                AttachmentId = r.AttachmentId,
                FileName = r.FileName,
                ContentType = r.ContentType,
                SizeBytes = r.SizeBytes,
                Status = (AttachmentWireStatus)r.Status,
                DownloadApiHint = r.DownloadApiHint,
                DownloadToken = r.DownloadToken,
                ThumbnailApiHint = r.ThumbnailApiHint,
                IsVoice = r.IsVoice,
                VoiceCodec = r.VoiceCodec,
                VoiceContainer = r.VoiceContainer,
                VoiceDurationMs = r.VoiceDurationMs,
                VoiceSampleRateHz = r.VoiceSampleRateHz,
                VoiceChannels = r.VoiceChannels,
                // VOICE-MSG-2 waveform：语音波形峰值包络透传（缺省/空 = 无波形）。
                VoiceWaveformPeaks = r.VoiceWaveformPeaks
            };
        }

        return mapped;
    }

    private static TcpConversationListItem[] MapListItems(IReadOnlyList<ConversationListItem> items)
    {
        var mapped = new TcpConversationListItem[items.Count];
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            mapped[i] = new TcpConversationListItem
            {
                ConversationId = item.ConversationId,
                Type = ToSharedConversationType(item.Type),
                PeerUserId = item.PeerUserId,
                Title = item.Title,
                LastMessageId = item.LastMessageId,
                LastMessagePreview = item.LastMessagePreview,
                LastMessageAtMs = item.LastMessageAtMs,
                LastSenderUserId = item.LastSenderUserId,
                UnreadCount = item.UnreadCount,
                LastReadMessageId = item.LastReadMessageId,
                LastReadAtMs = item.LastReadAtMs,
                IsPinned = item.IsPinned,
                PinnedAtMs = item.PinnedAtMs,
                IsMuted = item.IsMuted,
                MutedUntilMs = item.MutedUntilMs
            };
        }

        return mapped;
    }

    private static ConversationListItem[] MapListItems(IReadOnlyList<TcpConversationListItem> items)
    {
        var mapped = new ConversationListItem[items.Count];
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            mapped[i] = new ConversationListItem
            {
                ConversationId = item.ConversationId,
                Type = ToLocalConversationType(item.Type),
                PeerUserId = item.PeerUserId,
                Title = item.Title,
                LastMessageId = item.LastMessageId,
                LastMessagePreview = item.LastMessagePreview,
                LastMessageAtMs = item.LastMessageAtMs,
                LastSenderUserId = item.LastSenderUserId,
                UnreadCount = item.UnreadCount,
                LastReadMessageId = item.LastReadMessageId,
                LastReadAtMs = item.LastReadAtMs,
                IsPinned = item.IsPinned,
                PinnedAtMs = item.PinnedAtMs,
                IsMuted = item.IsMuted,
                MutedUntilMs = item.MutedUntilMs
            };
        }

        return mapped;
    }

    private static TcpConversationListCursor? MapCursor(ConversationListCursor? cursor) =>
        cursor is null
            ? null
            : new TcpConversationListCursor
            {
                IsPinned = cursor.IsPinned,
                PinnedAtMs = cursor.PinnedAtMs,
                LastMessageAtMs = cursor.LastMessageAtMs,
                ConversationId = cursor.ConversationId
            };

    private static ConversationListCursor? MapCursor(TcpConversationListCursor? cursor) =>
        cursor is null
            ? null
            : new ConversationListCursor(
                cursor.IsPinned,
                cursor.PinnedAtMs,
                cursor.LastMessageAtMs,
                cursor.ConversationId);

    private static TcpConversationMemberItem[]? MapMembers(IReadOnlyList<ConversationMemberItem>? members)
    {
        if (members is null)
        {
            return null;
        }

        var mapped = new TcpConversationMemberItem[members.Count];
        for (var i = 0; i < members.Count; i++)
        {
            var m = members[i];
            mapped[i] = new TcpConversationMemberItem
            {
                UserId = m.UserId,
                Role = ToSharedRole(m.Role),
                JoinedAtMs = m.JoinedAtMs
            };
        }

        return mapped;
    }

    private static ConversationMemberItem[]? MapMembers(IReadOnlyList<TcpConversationMemberItem>? members)
    {
        if (members is null)
        {
            return null;
        }

        var mapped = new ConversationMemberItem[members.Count];
        for (var i = 0; i < members.Count; i++)
        {
            var m = members[i];
            mapped[i] = new ConversationMemberItem
            {
                UserId = m.UserId,
                Role = ToLocalRole(m.Role),
                JoinedAtMs = m.JoinedAtMs
            };
        }

        return mapped;
    }

    private static SharedMessageReadReceiptItem[]? MapReadReceiptItems(
        IReadOnlyList<MessageReadReceiptItem>? readers)
    {
        if (readers is null)
        {
            return null;
        }

        var mapped = new SharedMessageReadReceiptItem[readers.Count];
        for (var i = 0; i < readers.Count; i++)
        {
            mapped[i] = new SharedMessageReadReceiptItem
            {
                UserId = readers[i].UserId,
                ReadAtMs = readers[i].ReadAtMs
            };
        }

        return mapped;
    }

    private static MessageReadReceiptItem[]? MapReadReceiptItems(
        IReadOnlyList<SharedMessageReadReceiptItem>? readers)
    {
        if (readers is null)
        {
            return null;
        }

        var mapped = new MessageReadReceiptItem[readers.Count];
        for (var i = 0; i < readers.Count; i++)
        {
            mapped[i] = new MessageReadReceiptItem
            {
                UserId = readers[i].UserId,
                ReadAtMs = readers[i].ReadAtMs
            };
        }

        return mapped;
    }

    private static SharedPresenceSnapshotItem[] MapPresenceItems(IReadOnlyList<PresenceSnapshotItem> items)
    {
        var mapped = new SharedPresenceSnapshotItem[items.Count];
        for (var i = 0; i < items.Count; i++)
        {
            mapped[i] = new SharedPresenceSnapshotItem
            {
                UserId = items[i].UserId,
                IsOnline = items[i].IsOnline
            };
        }

        return mapped;
    }

    private static PresenceSnapshotItem[] MapPresenceItems(IReadOnlyList<SharedPresenceSnapshotItem> items)
    {
        var mapped = new PresenceSnapshotItem[items.Count];
        for (var i = 0; i < items.Count; i++)
        {
            mapped[i] = new PresenceSnapshotItem
            {
                UserId = items[i].UserId,
                IsOnline = items[i].IsOnline
            };
        }

        return mapped;
    }
}
