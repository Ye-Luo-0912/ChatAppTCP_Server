using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Push;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using Xunit;

namespace ChatApp.TcpGateway.Tests.Architecture;

/// <summary>
/// JSON Source-Generation Context 覆盖完整性检查。
/// <para>
/// AGENTS.md 要求：所有协议/store JSON 必须使用 <see cref="GatewayJsonSerializerContext"/> / <see cref="JsonTypeInfo"/>，
/// 禁止反射 <c>JsonSerializerOptions</c> 用于 wire 或 Redis 值。
/// 本测试通过运行时 <see cref="JsonSerializerOptions.GetTypeInfo"/> 校验关键协议 DTO 类型
/// 在 <see cref="GatewayJsonSerializerContext.Default"/> 中可解析，等价于已注册 <c>[JsonSerializable]</c>。
/// </para>
/// <para>
/// 新增协议 DTO 时必须同步在 <see cref="GatewayJsonSerializerContext"/> 追加 <c>[JsonSerializable]</c>，
/// 否则 AOT/trim 场景下运行时反序列化失败。
/// </para>
/// </summary>
public sealed class JsonSerializerContextCoverageTests
{
    private static readonly JsonSerializerOptions ContextOptions =
        GatewayJsonSerializerContext.Default.Options;

    /// <summary>
    /// 协议核心 DTO 必须在 JSON context 中可解析。
    /// 此处列出"必须有"的关键类型；新增时同步追加。
    /// </summary>
    private static readonly Type[] RequiredProtocolTypes =
    [
        typeof(AuthenticationRequest),
        typeof(AuthenticationResponse),
        typeof(ChatMessage),
        typeof(AttachmentRef),
        typeof(MessageAcknowledgement),
        typeof(MessageReceiptRequest),
        typeof(MessageReceiptAcknowledgement),
        typeof(MessageReceiptUpdate),
        typeof(MessageHistoryRequest),
        typeof(MessageHistoryResponse),
        typeof(ConversationListRequest),
        typeof(ConversationListResponse),
        typeof(ConversationMarkReadRequest),
        typeof(ConversationMarkReadResponse),
        typeof(ConversationSetPrefsRequest),
        typeof(ConversationSetPrefsResponse),
        typeof(CreateGroupRequest),
        typeof(CreateGroupResponse),
        typeof(MessageRecallRequest),
        typeof(MessageRecalledUpdate),
        typeof(MessageEditRequest),
        typeof(MessageEditedUpdate),
        typeof(AddReactionRequest),
        typeof(ReactionAddedUpdate),
        typeof(RemoveReactionRequest),
        typeof(ReactionRemovedUpdate),
        typeof(TypingNotify),
        typeof(TypingUpdate),
        typeof(PresenceQueryRequest),
        typeof(PresenceSnapshotResponse),
        typeof(PresenceChanged),
        typeof(ConversationChanged),
        typeof(UnreadCountChanged),
        typeof(ConversationReadUpdate),
        typeof(SyncBootstrapRequest),
        typeof(SyncBootstrapResponse),
        typeof(ClientHello),
        typeof(ServerHello),
        typeof(GoAway),
        typeof(ResumeResponse),
        typeof(RelationshipListChangedUpdate),
        typeof(AttachmentLifecycleUpdate),
        typeof(RegisterPushTokenRequest),
        typeof(UnregisterPushTokenRequest),
        typeof(PushTokenRecord),
        typeof(ResumeContext),
    ];

    /// <summary>
    /// 所有关键协议 DTO 必须在 <see cref="GatewayJsonSerializerContext.Default"/> 中可解析。
    /// <c>GetTypeInfo</c> 返回非 null 表示源生成器已为该类型生成元数据，
    /// AOT/trim 场景下可安全使用。
    /// </summary>
    [Fact]
    public void Required_Protocol_Types_Are_Resolved_By_JsonContext()
    {
        var missing = new List<Type>();

        foreach (var type in RequiredProtocolTypes)
        {
            var typeInfo = ContextOptions.GetTypeInfo(type);
            if (typeInfo is null)
                missing.Add(type);
        }

        Assert.Empty(missing);
    }
}
