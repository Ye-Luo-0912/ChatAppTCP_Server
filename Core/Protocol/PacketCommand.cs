namespace ChatApp.TcpGateway.Core.Protocol;

public enum PacketCommand : ushort
{
    Heartbeat = 0,
    AuthenticationRequest = 1,
    AuthenticationResponse = 2,
    // 协议握手与连接管理
    ClientHello = 3,
    ServerHello = 4,
    GoAway = 5,
    ResumeRequest = 6,
    ResumeResponse = 7,
    ChatMessage = 101,
    MessageAcknowledgement = 102,
    MessageReceipt = 103,
    MessageReceiptAcknowledgement = 104,
    MessageReceiptUpdated = 105,
    MessageHistoryRequest = 106,
    MessageHistoryPage = 107,
    ConversationListRequest = 108,
    ConversationListPage = 109,
    ConversationMarkReadRequest = 110,
    ConversationMarkReadResponse = 111,
    ConversationChanged = 112,
    UnreadCountChanged = 113,
    SyncBootstrapRequest = 114,
    SyncBootstrapResponse = 115,
    ConversationSetPrefsRequest = 116,
    ConversationSetPrefsResponse = 117,
    MessageRecallRequest = 118,
    MessageRecallAck = 119,
    MessageRecalled = 120,
    TypingNotify = 121,
    TypingUpdate = 122,
    PresenceQuery = 123,
    PresenceSnapshot = 124,
    PresenceChanged = 125,
    PresenceUnwatch = 126,
    MessageEditRequest = 127,
    MessageEditAck = 128,
    MessageEdited = 129,
    AddReactionRequest = 130,
    AddReactionAck = 131,
    ReactionAdded = 132,
    RemoveReactionRequest = 133,
    RemoveReactionAck = 134,
    ReactionRemoved = 135,
    CreateGroupRequest = 136,
    CreateGroupResponse = 137,
    AddGroupMembersRequest = 138,
    AddGroupMembersResponse = 139,
    RemoveGroupMemberRequest = 140,
    RemoveGroupMemberResponse = 141,
    LeaveGroupRequest = 142,
    LeaveGroupResponse = 143,
    ChangeMemberRoleRequest = 144,
    ChangeMemberRoleResponse = 145,
    ListGroupMembersRequest = 146,
    ListGroupMembersResponse = 147,
    MemberJoined = 148,
    MemberLeft = 149,
    MemberRemoved = 150,
    RoleChanged = 151,
    ConversationRead = 152,
    RelationshipListChanged = 153,
    AttachmentLifecycleChanged = 154,
    RegisterPushTokenRequest = 155,
    RegisterPushTokenResponse = 156,
    UnregisterPushTokenRequest = 157,
    UnregisterPushTokenResponse = 158,
    /// <summary>主线四：客户端确认附件上传完成（C2S）。触发 Realtime 侧 Ticketed→Uploaded 转换。</summary>
    AttachmentFinalizeRequest = 159,
    /// <summary>主线四：附件上传确认响应（S2C）。</summary>
    AttachmentFinalizeResponse = 160,
    /// <summary>主线四：关系命令请求（C2S）：发送好友请求、接受/拒绝、删除好友、拉黑/取消拉黑。</summary>
    RelationshipCommandRequest = 161,
    /// <summary>主线四：关系命令响应（S2C）。</summary>
    RelationshipCommandResponse = 162,
    /// <summary>主线四：关系列表查询请求（C2S）：好友列表、好友请求列表、黑名单列表。</summary>
    RelationshipListRequest = 163,
    /// <summary>主线四：关系列表查询响应（S2C）。</summary>
    RelationshipListResponse = 164,
    /// <summary>P0-6：群成员批量加入通知（S2C），替代逐成员 MemberJoined 的聚合事件。</summary>
    MembersAddedUpdate = 165,
    /// <summary>P0-6：会话解散通知（S2C），客户端据此明确识别群已解散。</summary>
    ConversationDissolvedUpdate = 166,
    /// <summary>P1-3：附件下载授权请求（C2S）。客户端请求为附件签发短时有效的签名下载 URL。</summary>
    AttachmentDownloadAuthorizeRequest = 167,
    /// <summary>P1-3：附件下载授权响应（S2C）。返回签发的下载 URL / 令牌、过期时间或错误。</summary>
    AttachmentDownloadAuthorizeResponse = 168,
    /// <summary>P1-4：群消息已读回执查询请求（C2S）。仅消息发送者可查询。</summary>
    MessageReadReceiptQueryRequest = 169,
    /// <summary>P1-4：群消息已读回执查询响应（S2C）。小群返回 reader 列表，大群返回已读人数聚合。</summary>
    MessageReadReceiptQueryResponse = 170,
    HeartbeatAcknowledgement = 1000,
    Error = 500
}
