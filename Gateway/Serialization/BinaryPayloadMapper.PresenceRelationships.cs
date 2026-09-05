using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Attachments;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using SharedAttachmentDownloadAuthorizeRequest = ChatApp.Shared.Protocol.Tcp.AttachmentDownloadAuthorizeRequest;
using SharedAttachmentDownloadAuthorizeResponse = ChatApp.Shared.Protocol.Tcp.AttachmentDownloadAuthorizeResponse;
using SharedAttachmentFinalizeRequest = ChatApp.Shared.Protocol.Tcp.AttachmentFinalizeRequest;
using SharedAttachmentFinalizeResponse = ChatApp.Shared.Protocol.Tcp.AttachmentFinalizeResponse;
using SharedAttachmentLifecycleChanged = ChatApp.Shared.Protocol.Tcp.AttachmentLifecycleChanged;
using SharedPresenceChanged = ChatApp.Shared.Protocol.Tcp.TcpPresenceChanged;
using SharedPresenceQueryRequest = ChatApp.Shared.Protocol.Tcp.TcpPresenceQueryRequest;
using SharedPresenceSnapshotResponse = ChatApp.Shared.Protocol.Tcp.TcpPresenceSnapshotResponse;
using SharedPresenceUnwatchRequest = ChatApp.Shared.Protocol.Tcp.TcpPresenceUnwatchRequest;
using SharedRegisterPushTokenRequest = ChatApp.Shared.Protocol.Tcp.TcpRegisterPushTokenRequest;
using SharedRegisterPushTokenResponse = ChatApp.Shared.Protocol.Tcp.TcpRegisterPushTokenResponse;
using SharedRelationshipCommandRequest = ChatApp.Shared.Protocol.Tcp.TcpRelationshipCommandRequest;
using SharedRelationshipCommandResponse = ChatApp.Shared.Protocol.Tcp.TcpRelationshipCommandResponse;
using SharedRelationshipListChangedUpdate = ChatApp.Shared.Protocol.Tcp.TcpRelationshipListChangedUpdate;
using SharedTcpTypingNotify = ChatApp.Shared.Protocol.Tcp.TcpTypingNotify;
using SharedTcpTypingUpdate = ChatApp.Shared.Protocol.Tcp.TcpTypingUpdate;
using SharedUnregisterPushTokenRequest = ChatApp.Shared.Protocol.Tcp.TcpUnregisterPushTokenRequest;
using SharedUnregisterPushTokenResponse = ChatApp.Shared.Protocol.Tcp.TcpUnregisterPushTokenResponse;

namespace ChatApp.TcpGateway.Gateway.Serialization;

/// <summary>
/// 关系 / 在线 / 输入 / 推送 / 附件命令的本地 ↔ 共享映射。
/// </summary>
internal static partial class BinaryPayloadMapper
{
    // ──────────── 关系命令 ────────────

    private static SharedRelationshipCommandRequest ToShared(RelationshipCommandRequest request) => new()
    {
        RequestId = request.RequestId,
        Operation = ToSharedOperation(request.Operation),
        TargetUserId = request.TargetUserId,
        Message = request.Message,
        RequestIdToRespond = request.RequestIdToRespond
    };

    private static RelationshipCommandRequest ToLocal(SharedRelationshipCommandRequest request) => new()
    {
        RequestId = request.RequestId,
        Operation = ToLocalOperation(request.Operation),
        TargetUserId = request.TargetUserId,
        Message = request.Message,
        RequestIdToRespond = request.RequestIdToRespond
    };

    private static SharedRelationshipCommandResponse ToShared(RelationshipCommandResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        Operation = response.Operation is { } operation ? ToSharedOperation(operation) : null,
        TargetUserId = response.TargetUserId,
        ResourceId = response.ResourceId
    };

    private static RelationshipCommandResponse ToLocal(SharedRelationshipCommandResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        Operation = response.Operation is { } operation ? ToLocalOperation(operation) : null,
        TargetUserId = response.TargetUserId,
        ResourceId = response.ResourceId
    };

    /// <summary>关系列表变更通知：两侧字段完全同名同语义（Resource/Action/ResourceId/Actor/Message/时间）。</summary>
    private static SharedRelationshipListChangedUpdate ToShared(RelationshipListChangedUpdate update) => new()
    {
        Resource = update.Resource,
        Action = update.Action,
        ResourceId = update.ResourceId,
        ActorUserId = update.ActorUserId,
        Message = update.Message,
        OccurredAtMs = update.OccurredAtMs
    };

    private static RelationshipListChangedUpdate ToLocal(SharedRelationshipListChangedUpdate update) => new()
    {
        Resource = update.Resource,
        Action = update.Action,
        ResourceId = update.ResourceId,
        ActorUserId = update.ActorUserId,
        Message = update.Message,
        OccurredAtMs = update.OccurredAtMs
    };

    // ──────────── 输入状态 ────────────

    /// <summary>
    /// 共享 TcpTypingNotify 必须携带 TargetUserId，但网关安全模型以 ConversationId 为权威源
    /// 推导目标并校验发送方成员资格，客户端提交的目标用户恒被忽略（TypingCommandHandler 注释），
    /// 因此编码侧置 0、解码侧丢弃该字段。
    /// </summary>
    private static SharedTcpTypingNotify ToShared(TypingNotify notify) => new()
    {
        TargetUserId = 0,
        ConversationId = notify.ConversationId,
        IsTyping = notify.IsTyping
    };

    private static TypingNotify ToLocal(SharedTcpTypingNotify notify) => new()
    {
        ConversationId = notify.ConversationId,
        IsTyping = notify.IsTyping
    };

    private static SharedTcpTypingUpdate ToShared(TypingUpdate update) => new()
    {
        SenderUserId = update.SenderUserId,
        ConversationId = update.ConversationId,
        IsTyping = update.IsTyping
    };

    private static TypingUpdate ToLocal(SharedTcpTypingUpdate update) => new()
    {
        SenderUserId = update.SenderUserId,
        ConversationId = update.ConversationId,
        IsTyping = update.IsTyping
    };

    // ──────────── 在线状态 ────────────

    private static SharedPresenceQueryRequest ToShared(PresenceQueryRequest request) => new()
    {
        RequestId = request.RequestId,
        UserIds = request.UserIds
    };

    private static PresenceQueryRequest ToLocal(SharedPresenceQueryRequest request) => new()
    {
        RequestId = request.RequestId,
        UserIds = request.UserIds
    };

    private static SharedPresenceUnwatchRequest ToShared(PresenceUnwatchRequest request) => new()
    {
        UserIds = request.UserIds
    };

    private static PresenceUnwatchRequest ToLocal(SharedPresenceUnwatchRequest request) => new()
    {
        UserIds = request.UserIds
    };

    private static SharedPresenceSnapshotResponse ToShared(PresenceSnapshotResponse response) => new()
    {
        RequestId = response.RequestId,
        Items = MapPresenceItems(response.Items)
    };

    private static PresenceSnapshotResponse ToLocal(SharedPresenceSnapshotResponse response) => new()
    {
        RequestId = response.RequestId,
        Items = MapPresenceItems(response.Items)
    };

    private static SharedPresenceChanged ToShared(PresenceChanged update) => new()
    {
        UserId = update.UserId,
        IsOnline = update.IsOnline
    };

    private static PresenceChanged ToLocal(SharedPresenceChanged update) => new()
    {
        UserId = update.UserId,
        IsOnline = update.IsOnline
    };

    // ──────────── 推送令牌 ────────────

    private static SharedRegisterPushTokenRequest ToShared(RegisterPushTokenRequest request) => new()
    {
        RequestId = request.RequestId,
        Platform = ToSharedPlatform(request.Platform),
        Token = request.Token,
        AppDeviceLabel = request.AppDeviceLabel
    };

    private static RegisterPushTokenRequest ToLocal(SharedRegisterPushTokenRequest request) => new()
    {
        RequestId = request.RequestId,
        Platform = ToLocalPlatform(request.Platform),
        Token = request.Token,
        AppDeviceLabel = request.AppDeviceLabel
    };

    private static SharedRegisterPushTokenResponse ToShared(RegisterPushTokenResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ActiveTokenCount = response.ActiveTokenCount
    };

    private static RegisterPushTokenResponse ToLocal(SharedRegisterPushTokenResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ActiveTokenCount = response.ActiveTokenCount
    };

    private static SharedUnregisterPushTokenRequest ToShared(UnregisterPushTokenRequest request) => new()
    {
        RequestId = request.RequestId,
        Token = request.Token
    };

    private static UnregisterPushTokenRequest ToLocal(SharedUnregisterPushTokenRequest request) => new()
    {
        RequestId = request.RequestId,
        Token = request.Token
    };

    private static SharedUnregisterPushTokenResponse ToShared(UnregisterPushTokenResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ActiveTokenCount = response.ActiveTokenCount
    };

    private static UnregisterPushTokenResponse ToLocal(SharedUnregisterPushTokenResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        ActiveTokenCount = response.ActiveTokenCount
    };

    // ──────────── 附件 ────────────

    /// <summary>本地 AttachmentLifecycleUpdate ↔ 共享 AttachmentLifecycleChanged：字段同名，仅类型名不同。</summary>
    private static SharedAttachmentLifecycleChanged ToShared(AttachmentLifecycleUpdate update) => new()
    {
        AttachmentId = update.AttachmentId,
        Status = update.Status,
        OccurredAtMs = update.OccurredAtMs,
        RejectReason = update.RejectReason,
        ThumbnailApiHint = update.ThumbnailApiHint,
        DownloadToken = update.DownloadToken
    };

    private static AttachmentLifecycleUpdate ToLocal(SharedAttachmentLifecycleChanged update) => new()
    {
        AttachmentId = update.AttachmentId,
        Status = update.Status,
        OccurredAtMs = update.OccurredAtMs,
        RejectReason = update.RejectReason,
        ThumbnailApiHint = update.ThumbnailApiHint,
        DownloadToken = update.DownloadToken
    };

    private static SharedAttachmentFinalizeRequest ToShared(AttachmentFinalizeRequest request) => new()
    {
        RequestId = request.RequestId,
        AttachmentId = request.AttachmentId,
        SizeBytes = request.SizeBytes,
        ContentHash = request.ContentHash
    };

    private static AttachmentFinalizeRequest ToLocal(SharedAttachmentFinalizeRequest request) => new()
    {
        RequestId = request.RequestId,
        AttachmentId = request.AttachmentId,
        SizeBytes = request.SizeBytes,
        ContentHash = request.ContentHash
    };

    private static SharedAttachmentFinalizeResponse ToShared(AttachmentFinalizeResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        AttachmentId = response.AttachmentId,
        Status = response.Status
    };

    private static AttachmentFinalizeResponse ToLocal(SharedAttachmentFinalizeResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        AttachmentId = response.AttachmentId,
        Status = response.Status
    };

    private static SharedAttachmentDownloadAuthorizeRequest ToShared(AttachmentDownloadAuthorizeRequest request) => new()
    {
        RequestId = request.RequestId,
        AttachmentId = request.AttachmentId,
        ConversationId = request.ConversationId
    };

    private static AttachmentDownloadAuthorizeRequest ToLocal(SharedAttachmentDownloadAuthorizeRequest request) => new()
    {
        RequestId = request.RequestId,
        AttachmentId = request.AttachmentId,
        ConversationId = request.ConversationId
    };

    private static SharedAttachmentDownloadAuthorizeResponse ToShared(AttachmentDownloadAuthorizeResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        AttachmentId = response.AttachmentId,
        DownloadUrl = response.DownloadUrl,
        DownloadToken = response.DownloadToken,
        ExpiresAtMs = response.ExpiresAtMs
    };

    private static AttachmentDownloadAuthorizeResponse ToLocal(SharedAttachmentDownloadAuthorizeResponse response) => new()
    {
        RequestId = response.RequestId,
        Succeeded = response.Succeeded,
        ErrorCode = response.ErrorCode,
        ErrorMessage = response.ErrorMessage,
        AttachmentId = response.AttachmentId,
        DownloadUrl = response.DownloadUrl,
        DownloadToken = response.DownloadToken,
        ExpiresAtMs = response.ExpiresAtMs
    };
}
