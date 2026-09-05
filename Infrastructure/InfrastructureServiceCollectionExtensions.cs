using ChatApp.Realtime.Abstractions.Push;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Push;
using ChatApp.TcpGateway.Core.Authentication;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Messaging.Attachments;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Push;
using ChatApp.TcpGateway.Core.Messaging.Relationships;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Core.Server;
using ChatApp.TcpGateway.Infrastructure.Authentication;
using ChatApp.TcpGateway.Infrastructure.Caching;
using ChatApp.TcpGateway.Infrastructure.GroupIdempotency;
using ChatApp.TcpGateway.Infrastructure.Push;
using ChatApp.TcpGateway.Infrastructure.Routing;
using ChatApp.TcpGateway.Infrastructure.Serialization.Json;
using ChatApp.TcpGateway.Infrastructure.Server;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Infrastructure;

public static class InfrastructureServiceCollectionExtensions
{
    public static IServiceCollection AddGatewayInfrastructure(
        this IServiceCollection services)
    {
        services.AddSingleton<RedisConnectionProvider>();
        services.AddSingleton<IHostedService>(
            static provider =>
                provider.GetRequiredService<RedisConnectionProvider>());

        // P0-6：按 Redis 域拆分熔断器，避免 Group 幂等 / DeviceLease 故障耦合到 ResumeToken。
        // ResumeToken 域：TcpGatewayService / SessionLifecycleCoordinator + RedisResumeTokenStore 共享。
        services.AddSingleton<IRedisCircuitBreaker>(
            static provider => CreateDomainCircuitBreaker(
                provider
                    .GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisOptions>>()
                    .Value));

        services.AddSingleton<IAccessTokenStore, RedisAccessTokenStore>();
        // P0-6：DeviceLease 域使用独立熔断器，与 ResumeToken 域隔离。
        services.AddSingleton<IDeviceSessionLeaseStore>(
            static provider =>
            {
                var options = provider
                    .GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisOptions>>()
                    .Value;
                return new RedisDeviceSessionLeaseStore(
                    provider.GetRequiredService<RedisConnectionProvider>(),
                    provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RedisDeviceSessionLeaseStore>>(),
                    CreateDomainCircuitBreaker(options));
            });
        services.AddSingleton<IResumeTokenStore, RedisResumeTokenStore>();
        services.AddSingleton<IRealtimeAuthenticator, RealtimeAuthenticator>();
        services.AddSingleton<IServerIdentity, ServerIdentity>();
        services.AddSingleton<IDirectConversationAuthorizer, CachedDirectConversationAuthorizer>();
        services.AddSingleton<IPushTokenStore, RedisPushTokenStore>();
        // 主线一10 + 门禁3：Push Token 加密保护器。
        // 优先级：密钥环（TokenEncryptionKeys，支持旧 Key 读取 + 当前 Key 写入 + 渐进重加密）
        //   → 单密钥（TokenEncryptionKey，AES-GCM）→ 明文（NullPushTokenProtector，向后兼容）。
        services.AddSingleton<IPushTokenProtector>(static provider =>
        {
            var options = provider
                .GetRequiredService<Microsoft.Extensions.Options.IOptions<PushOptions>>()
                .Value;

            // 门禁3：密钥环配置优先。KeyId 解析为 uint，写入使用 KeyId 最大的密钥（当前 Key）。
            if (options.TokenEncryptionKeys is { Count: > 0 })
            {
                var keyRing = new Dictionary<uint, byte[]>();
                foreach (var config in options.TokenEncryptionKeys)
                {
                    if (string.IsNullOrEmpty(config.Key) ||
                        !uint.TryParse(config.KeyId, out var keyId))
                        continue;
                    keyRing[keyId] = Convert.FromBase64String(config.Key);
                }

                if (keyRing.Count > 0)
                {
                    var currentKeyId = keyRing.Keys.Max();
                    return new RotatingPushTokenProtector(keyRing, currentKeyId);
                }
            }

            if (!string.IsNullOrEmpty(options.TokenEncryptionKey))
                return new AesGcmPushTokenProtector(options.TokenEncryptionKey);
            return new NullPushTokenProtector();
        });
        services.AddSingleton<IGatewayDirectory, RedisGatewayDirectory>();
        services.AddSingleton<IWatcherGatewayDirectory, RedisWatcherGatewayDirectory>();

        // 三-3：冻结用户缓存（fail-open + 后台刷新）。由 UserLifecycleChanged 事件驱动更新，
        // 供 SessionLifecycleCoordinator 认证/Resume 路径快速拒绝冻结用户。
        // 实现 IDisposable（清理定时器），DI 容器在停机时自动 Dispose。
        services.AddSingleton<IFrozenUserCache, FrozenUserCache>();

        // 群组命令幂等 L2（Redis）存储。具体类型注册——Composite 在 Program.cs 中组装。
        // P0-1：离线推送 Provider 与 IPushDispatcher 不在此处无条件注册。
        // 由 Program.cs 根据 PushOptions.Enabled / ProviderMode 决定：
        //   Disabled     — 不注册 Consumer / Provider（推送留在 JetStream 等待 Worker）。
        //   TestNoop     — 注册 3 个 NoopPushProvider + Consumer（Noop 返回 provider_unavailable 不会静默 ACK）。
        //   Production   — 注册 Consumer；启动校验三个平台均非 Noop，否则 fail-fast。
        // P0-6：GroupIdempotency 域使用独立熔断器（fail-open 缓存不能影响安全关键的 fail-closed 设备 fencing）。
        services.AddSingleton<RedisGroupIdempotencyStore>(
            static provider =>
            {
                var options = provider
                    .GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisOptions>>()
                    .Value;
                return new RedisGroupIdempotencyStore(
                    provider.GetRequiredService<RedisConnectionProvider>(),
                    provider.GetRequiredService<Observability.Metrics.GatewayMetrics>(),
                    provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RedisGroupIdempotencyStore>>(),
                    CreateDomainCircuitBreaker(options));
            });

        services.AddSingleton<IPayloadCodec<AuthenticationRequest>>(
            static _ => new JsonPayloadCodec<AuthenticationRequest>(
                GatewayJsonSerializerContext.Default.AuthenticationRequest));
        services.AddSingleton<IPayloadCodec<AuthenticationResponse>>(
            static _ => new JsonPayloadCodec<AuthenticationResponse>(
                GatewayJsonSerializerContext.Default.AuthenticationResponse));
        services.AddSingleton<IPayloadCodec<ChatMessage>>(
            static _ => new JsonPayloadCodec<ChatMessage>(
                GatewayJsonSerializerContext.Default.ChatMessage));
        services.AddSingleton<IPayloadCodec<MessageAcknowledgement>>(
            static _ => new JsonPayloadCodec<MessageAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageAcknowledgement));
        services.AddSingleton<IPayloadCodec<MessageReceiptRequest>>(
            static _ => new JsonPayloadCodec<MessageReceiptRequest>(
                GatewayJsonSerializerContext.Default.MessageReceiptRequest));
        services.AddSingleton<IPayloadCodec<MessageReceiptAcknowledgement>>(
            static _ => new JsonPayloadCodec<MessageReceiptAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageReceiptAcknowledgement));
        services.AddSingleton<IPayloadCodec<MessageReceiptUpdate>>(
            static _ => new JsonPayloadCodec<MessageReceiptUpdate>(
                GatewayJsonSerializerContext.Default.MessageReceiptUpdate));
        services.AddSingleton<IPayloadCodec<MessageHistoryRequest>>(
            static _ => new JsonPayloadCodec<MessageHistoryRequest>(
                GatewayJsonSerializerContext.Default.MessageHistoryRequest));
        services.AddSingleton<IPayloadCodec<MessageHistoryResponse>>(
            static _ => new JsonPayloadCodec<MessageHistoryResponse>(
                GatewayJsonSerializerContext.Default.MessageHistoryResponse));
        services.AddSingleton<IPayloadCodec<MessageHistoryItem[]>>(
            static _ => new JsonPayloadCodec<MessageHistoryItem[]>(
                GatewayJsonSerializerContext.Default.MessageHistoryItemArray));
        services.AddSingleton<IPayloadCodec<ConversationListRequest>>(
            static _ => new JsonPayloadCodec<ConversationListRequest>(
                GatewayJsonSerializerContext.Default.ConversationListRequest));
        services.AddSingleton<IPayloadCodec<ConversationListResponse>>(
            static _ => new JsonPayloadCodec<ConversationListResponse>(
                GatewayJsonSerializerContext.Default.ConversationListResponse));
        services.AddSingleton<IPayloadCodec<ConversationListItem[]>>(
            static _ => new JsonPayloadCodec<ConversationListItem[]>(
                GatewayJsonSerializerContext.Default.ConversationListItemArray));
        services.AddSingleton<IPayloadCodec<ConversationMarkReadRequest>>(
            static _ => new JsonPayloadCodec<ConversationMarkReadRequest>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadRequest));
        services.AddSingleton<IPayloadCodec<ConversationMarkReadResponse>>(
            static _ => new JsonPayloadCodec<ConversationMarkReadResponse>(
                GatewayJsonSerializerContext.Default.ConversationMarkReadResponse));
        services.AddSingleton<IPayloadCodec<ConversationSetPrefsRequest>>(
            static _ => new JsonPayloadCodec<ConversationSetPrefsRequest>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsRequest));
        services.AddSingleton<IPayloadCodec<ConversationSetPrefsResponse>>(
            static _ => new JsonPayloadCodec<ConversationSetPrefsResponse>(
                GatewayJsonSerializerContext.Default.ConversationSetPrefsResponse));
        services.AddSingleton<IPayloadCodec<MemberJoinedUpdate>>(
            static _ => new JsonPayloadCodec<MemberJoinedUpdate>(
                GatewayJsonSerializerContext.Default.MemberJoinedUpdate));
        services.AddSingleton<IPayloadCodec<MemberLeftUpdate>>(
            static _ => new JsonPayloadCodec<MemberLeftUpdate>(
                GatewayJsonSerializerContext.Default.MemberLeftUpdate));
        services.AddSingleton<IPayloadCodec<MemberRemovedUpdate>>(
            static _ => new JsonPayloadCodec<MemberRemovedUpdate>(
                GatewayJsonSerializerContext.Default.MemberRemovedUpdate));
        services.AddSingleton<IPayloadCodec<RoleChangedUpdate>>(
            static _ => new JsonPayloadCodec<RoleChangedUpdate>(
                GatewayJsonSerializerContext.Default.RoleChangedUpdate));
        services.AddSingleton<IPayloadCodec<MessageRecallRequest>>(
            static _ => new JsonPayloadCodec<MessageRecallRequest>(
                GatewayJsonSerializerContext.Default.MessageRecallRequest));
        services.AddSingleton<IPayloadCodec<MessageRecallAcknowledgement>>(
            static _ => new JsonPayloadCodec<MessageRecallAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageRecallAcknowledgement));
        services.AddSingleton<IPayloadCodec<MessageRecalledUpdate>>(
            static _ => new JsonPayloadCodec<MessageRecalledUpdate>(
                GatewayJsonSerializerContext.Default.MessageRecalledUpdate));
        services.AddSingleton<IPayloadCodec<MessageEditRequest>>(
            static _ => new JsonPayloadCodec<MessageEditRequest>(
                GatewayJsonSerializerContext.Default.MessageEditRequest));
        services.AddSingleton<IPayloadCodec<MessageEditAcknowledgement>>(
            static _ => new JsonPayloadCodec<MessageEditAcknowledgement>(
                GatewayJsonSerializerContext.Default.MessageEditAcknowledgement));
        services.AddSingleton<IPayloadCodec<MessageEditedUpdate>>(
            static _ => new JsonPayloadCodec<MessageEditedUpdate>(
                GatewayJsonSerializerContext.Default.MessageEditedUpdate));
        services.AddSingleton<IPayloadCodec<AddReactionRequest>>(
            static _ => new JsonPayloadCodec<AddReactionRequest>(
                GatewayJsonSerializerContext.Default.AddReactionRequest));
        services.AddSingleton<IPayloadCodec<AddReactionAcknowledgement>>(
            static _ => new JsonPayloadCodec<AddReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.AddReactionAcknowledgement));
        services.AddSingleton<IPayloadCodec<ReactionAddedUpdate>>(
            static _ => new JsonPayloadCodec<ReactionAddedUpdate>(
                GatewayJsonSerializerContext.Default.ReactionAddedUpdate));
        services.AddSingleton<IPayloadCodec<RemoveReactionRequest>>(
            static _ => new JsonPayloadCodec<RemoveReactionRequest>(
                GatewayJsonSerializerContext.Default.RemoveReactionRequest));
        services.AddSingleton<IPayloadCodec<RemoveReactionAcknowledgement>>(
            static _ => new JsonPayloadCodec<RemoveReactionAcknowledgement>(
                GatewayJsonSerializerContext.Default.RemoveReactionAcknowledgement));
        services.AddSingleton<IPayloadCodec<ReactionRemovedUpdate>>(
            static _ => new JsonPayloadCodec<ReactionRemovedUpdate>(
                GatewayJsonSerializerContext.Default.ReactionRemovedUpdate));
        services.AddSingleton<IPayloadCodec<ConversationChanged>>(
            static _ => new JsonPayloadCodec<ConversationChanged>(
                GatewayJsonSerializerContext.Default.ConversationChanged));
        services.AddSingleton<IPayloadCodec<UnreadCountChanged>>(
            static _ => new JsonPayloadCodec<UnreadCountChanged>(
                GatewayJsonSerializerContext.Default.UnreadCountChanged));
        services.AddSingleton<IPayloadCodec<ConversationReadUpdate>>(
            static _ => new JsonPayloadCodec<ConversationReadUpdate>(
                GatewayJsonSerializerContext.Default.ConversationReadUpdate));
        services.AddSingleton<IPayloadCodec<SyncBootstrapRequest>>(
            static _ => new JsonPayloadCodec<SyncBootstrapRequest>(
                GatewayJsonSerializerContext.Default.SyncBootstrapRequest));
        services.AddSingleton<IPayloadCodec<SyncBootstrapResponse>>(
            static _ => new JsonPayloadCodec<SyncBootstrapResponse>(
                GatewayJsonSerializerContext.Default.SyncBootstrapResponse));

        // 离线推送令牌（PushTokenCommandHandler 使用）
        services.AddSingleton<IPayloadCodec<RegisterPushTokenRequest>>(
            static _ => new JsonPayloadCodec<RegisterPushTokenRequest>(
                GatewayJsonSerializerContext.Default.RegisterPushTokenRequest));
        services.AddSingleton<IPayloadCodec<RegisterPushTokenResponse>>(
            static _ => new JsonPayloadCodec<RegisterPushTokenResponse>(
                GatewayJsonSerializerContext.Default.RegisterPushTokenResponse));
        services.AddSingleton<IPayloadCodec<UnregisterPushTokenRequest>>(
            static _ => new JsonPayloadCodec<UnregisterPushTokenRequest>(
                GatewayJsonSerializerContext.Default.UnregisterPushTokenRequest));
        services.AddSingleton<IPayloadCodec<UnregisterPushTokenResponse>>(
            static _ => new JsonPayloadCodec<UnregisterPushTokenResponse>(
                GatewayJsonSerializerContext.Default.UnregisterPushTokenResponse));

        // 群组命令（GroupCommandHandler 使用）
        services.AddSingleton<IPayloadCodec<CreateGroupRequest>>(
            static _ => new JsonPayloadCodec<CreateGroupRequest>(
                GatewayJsonSerializerContext.Default.CreateGroupRequest));
        services.AddSingleton<IPayloadCodec<CreateGroupResponse>>(
            static _ => new JsonPayloadCodec<CreateGroupResponse>(
                GatewayJsonSerializerContext.Default.CreateGroupResponse));
        services.AddSingleton<IPayloadCodec<AddGroupMembersRequest>>(
            static _ => new JsonPayloadCodec<AddGroupMembersRequest>(
                GatewayJsonSerializerContext.Default.AddGroupMembersRequest));
        services.AddSingleton<IPayloadCodec<AddGroupMembersResponse>>(
            static _ => new JsonPayloadCodec<AddGroupMembersResponse>(
                GatewayJsonSerializerContext.Default.AddGroupMembersResponse));
        services.AddSingleton<IPayloadCodec<RemoveGroupMemberRequest>>(
            static _ => new JsonPayloadCodec<RemoveGroupMemberRequest>(
                GatewayJsonSerializerContext.Default.RemoveGroupMemberRequest));
        services.AddSingleton<IPayloadCodec<RemoveGroupMemberResponse>>(
            static _ => new JsonPayloadCodec<RemoveGroupMemberResponse>(
                GatewayJsonSerializerContext.Default.RemoveGroupMemberResponse));
        services.AddSingleton<IPayloadCodec<LeaveGroupRequest>>(
            static _ => new JsonPayloadCodec<LeaveGroupRequest>(
                GatewayJsonSerializerContext.Default.LeaveGroupRequest));
        services.AddSingleton<IPayloadCodec<LeaveGroupResponse>>(
            static _ => new JsonPayloadCodec<LeaveGroupResponse>(
                GatewayJsonSerializerContext.Default.LeaveGroupResponse));
        services.AddSingleton<IPayloadCodec<ChangeMemberRoleRequest>>(
            static _ => new JsonPayloadCodec<ChangeMemberRoleRequest>(
                GatewayJsonSerializerContext.Default.ChangeMemberRoleRequest));
        services.AddSingleton<IPayloadCodec<ChangeMemberRoleResponse>>(
            static _ => new JsonPayloadCodec<ChangeMemberRoleResponse>(
                GatewayJsonSerializerContext.Default.ChangeMemberRoleResponse));
        services.AddSingleton<IPayloadCodec<ListGroupMembersRequest>>(
            static _ => new JsonPayloadCodec<ListGroupMembersRequest>(
                GatewayJsonSerializerContext.Default.ListGroupMembersRequest));
        services.AddSingleton<IPayloadCodec<ListGroupMembersResponse>>(
            static _ => new JsonPayloadCodec<ListGroupMembersResponse>(
                GatewayJsonSerializerContext.Default.ListGroupMembersResponse));
        services.AddSingleton<IPayloadCodec<MessageReadReceiptQueryRequest>>(
            static _ => new JsonPayloadCodec<MessageReadReceiptQueryRequest>(
                GatewayJsonSerializerContext.Default.MessageReadReceiptQueryRequest));
        services.AddSingleton<IPayloadCodec<MessageReadReceiptQueryResponse>>(
            static _ => new JsonPayloadCodec<MessageReadReceiptQueryResponse>(
                GatewayJsonSerializerContext.Default.MessageReadReceiptQueryResponse));
        services.AddSingleton<IPayloadCodec<DissolveGroupRequest>>(
            static _ => new JsonPayloadCodec<DissolveGroupRequest>(
                GatewayJsonSerializerContext.Default.DissolveGroupRequest));
        services.AddSingleton<IPayloadCodec<DissolveGroupResponse>>(
            static _ => new JsonPayloadCodec<DissolveGroupResponse>(
                GatewayJsonSerializerContext.Default.DissolveGroupResponse));

        // Typing / Presence（TypingCommandHandler / PresenceCommandHandler 使用）
        services.AddSingleton<IPayloadCodec<TypingNotify>>(
            static _ => new JsonPayloadCodec<TypingNotify>(
                GatewayJsonSerializerContext.Default.TypingNotify));
        services.AddSingleton<IPayloadCodec<PresenceQueryRequest>>(
            static _ => new JsonPayloadCodec<PresenceQueryRequest>(
                GatewayJsonSerializerContext.Default.PresenceQueryRequest));
        services.AddSingleton<IPayloadCodec<PresenceUnwatchRequest>>(
            static _ => new JsonPayloadCodec<PresenceUnwatchRequest>(
                GatewayJsonSerializerContext.Default.PresenceUnwatchRequest));
        services.AddSingleton<IPayloadCodec<PresenceSnapshotResponse>>(
            static _ => new JsonPayloadCodec<PresenceSnapshotResponse>(
                GatewayJsonSerializerContext.Default.PresenceSnapshotResponse));

        // SessionLifecycleCoordinator 与 TypingFanout 消费路径使用：
        // PresenceChanged 用于本机 watcher 扇出；TypingUpdate 用于本机 Typing 扇出。
        services.AddSingleton<IPayloadCodec<PresenceChanged>>(
            static _ => new JsonPayloadCodec<PresenceChanged>(
                GatewayJsonSerializerContext.Default.PresenceChanged));
        services.AddSingleton<IPayloadCodec<TypingUpdate>>(
            static _ => new JsonPayloadCodec<TypingUpdate>(
                GatewayJsonSerializerContext.Default.TypingUpdate));

        // 协议握手与连接管理
        services.AddSingleton<IPayloadCodec<ClientHello>>(
            static _ => new JsonPayloadCodec<ClientHello>(
                GatewayJsonSerializerContext.Default.ClientHello));
        services.AddSingleton<IPayloadCodec<ServerHello>>(
            static _ => new JsonPayloadCodec<ServerHello>(
                GatewayJsonSerializerContext.Default.ServerHello));
        services.AddSingleton<IPayloadCodec<GoAway>>(
            static _ => new JsonPayloadCodec<GoAway>(
                GatewayJsonSerializerContext.Default.GoAway));
        services.AddSingleton<IPayloadCodec<ResumeResponse>>(
            static _ => new JsonPayloadCodec<ResumeResponse>(
                GatewayJsonSerializerContext.Default.ResumeResponse));
        services.AddSingleton<IPayloadCodec<ProtocolErrorFrame>>(
            static _ => new JsonPayloadCodec<ProtocolErrorFrame>(
                GatewayJsonSerializerContext.Default.ProtocolErrorFrame));
        services.AddSingleton<IPayloadCodec<RelationshipListChangedUpdate>>(
            static _ => new JsonPayloadCodec<RelationshipListChangedUpdate>(
                GatewayJsonSerializerContext.Default.RelationshipListChangedUpdate));
        services.AddSingleton<IPayloadCodec<AttachmentLifecycleUpdate>>(
            static _ => new JsonPayloadCodec<AttachmentLifecycleUpdate>(
                GatewayJsonSerializerContext.Default.AttachmentLifecycleUpdate));

        // 主线四：附件上传确认协议
        services.AddSingleton<IPayloadCodec<AttachmentFinalizeRequest>>(
            static _ => new JsonPayloadCodec<AttachmentFinalizeRequest>(
                GatewayJsonSerializerContext.Default.AttachmentFinalizeRequest));
        services.AddSingleton<IPayloadCodec<AttachmentFinalizeResponse>>(
            static _ => new JsonPayloadCodec<AttachmentFinalizeResponse>(
                GatewayJsonSerializerContext.Default.AttachmentFinalizeResponse));

        // P1-3：附件下载授权协议
        services.AddSingleton<IPayloadCodec<AttachmentDownloadAuthorizeRequest>>(
            static _ => new JsonPayloadCodec<AttachmentDownloadAuthorizeRequest>(
                GatewayJsonSerializerContext.Default.AttachmentDownloadAuthorizeRequest));
        services.AddSingleton<IPayloadCodec<AttachmentDownloadAuthorizeResponse>>(
            static _ => new JsonPayloadCodec<AttachmentDownloadAuthorizeResponse>(
                GatewayJsonSerializerContext.Default.AttachmentDownloadAuthorizeResponse));

        // 主线四：关系命令协议
        services.AddSingleton<IPayloadCodec<RelationshipCommandRequest>>(
            static _ => new JsonPayloadCodec<RelationshipCommandRequest>(
                GatewayJsonSerializerContext.Default.RelationshipCommandRequest));
        services.AddSingleton<IPayloadCodec<RelationshipCommandResponse>>(
            static _ => new JsonPayloadCodec<RelationshipCommandResponse>(
                GatewayJsonSerializerContext.Default.RelationshipCommandResponse));
        services.AddSingleton<IPayloadCodec<ChatApp.Shared.Protocol.Tcp.TcpRelationshipListRequest>>(
            static _ => new JsonPayloadCodec<ChatApp.Shared.Protocol.Tcp.TcpRelationshipListRequest>(
                GatewayJsonSerializerContext.Default.TcpRelationshipListRequest));
        services.AddSingleton<IPayloadCodec<ChatApp.Shared.Protocol.Tcp.TcpRelationshipListResponse>>(
            static _ => new JsonPayloadCodec<ChatApp.Shared.Protocol.Tcp.TcpRelationshipListResponse>(
                GatewayJsonSerializerContext.Default.TcpRelationshipListResponse));
        // CALL-E2E-2：通话信令控制面协议
        services.AddSingleton<IPayloadCodec<TcpCallCommandRequest>>(
            static _ => new JsonPayloadCodec<TcpCallCommandRequest>(
                GatewayJsonSerializerContext.Default.TcpCallCommandRequest));
        services.AddSingleton<IPayloadCodec<TcpCallCommandResponse>>(
            static _ => new JsonPayloadCodec<TcpCallCommandResponse>(
                GatewayJsonSerializerContext.Default.TcpCallCommandResponse));
        services.AddSingleton<IPayloadCodec<TcpCallSignal>>(
            static _ => new JsonPayloadCodec<TcpCallSignal>(
                GatewayJsonSerializerContext.Default.TcpCallSignal));
        services.AddSingleton<IPayloadCodec<MembersAddedUpdate>>(
            static _ => new JsonPayloadCodec<MembersAddedUpdate>(
                GatewayJsonSerializerContext.Default.MembersAddedUpdate));
        services.AddSingleton<IPayloadCodec<ConversationDissolvedUpdate>>(
            static _ => new JsonPayloadCodec<ConversationDissolvedUpdate>(
                GatewayJsonSerializerContext.Default.ConversationDissolvedUpdate));

        return services;
    }

    /// <summary>
    /// P0-6：为每个 Redis 域创建独立的熔断器实例。
    /// 阈值 0 = 关闭（int.MaxValue 阈值 + 1s 开路窗口，实际永不触发）。
    /// </summary>
    private static RedisCircuitBreaker CreateDomainCircuitBreaker(RedisOptions options)
    {
        if (options.CircuitBreakerFailureThreshold <= 0)
            return new RedisCircuitBreaker(
                failureThreshold: int.MaxValue,
                openDuration: TimeSpan.FromSeconds(1));
        return new RedisCircuitBreaker(
            failureThreshold: options.CircuitBreakerFailureThreshold,
            openDuration: options.CircuitBreakerOpenDuration);
    }

    /// <summary>
    /// 主线一9：注册离线推送相关服务（PushDispatcher / PushProvider / Consumer / IdempotencyStore）。
    /// <para>
    /// 根据 <see cref="PushOptions.Enabled"/> 与 <see cref="PushOptions.ProviderMode"/> 门控注册：
    /// <list type="bullet">
    /// <item>Disabled / Enabled=false：不注册 Consumer 与 Provider，推送命令留在 JetStream 等待独立 Push Worker 消费。</item>
    /// <item>TestNoop：注册 3 个 NoopPushProvider + Consumer + Dispatcher + IdempotencyStore。</item>
    /// <item>Production：注册 Consumer + Dispatcher + IdempotencyStore；启动校验三个平台均非 Noop。</item>
    /// </list>
    /// </para>
    /// <para>
    /// 主线一5：同时注册 <see cref="IPushIdempotencyStore"/>（Redis 实现），防止 JetStream NAK 重投导致重复推送。
    /// </para>
    /// </summary>
    public static IServiceCollection AddPushServices(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var pushOptions = configuration
            .GetSection(PushOptions.SectionName)
            .Get<PushOptions>() ?? new PushOptions();

        if (!pushOptions.Enabled || pushOptions.ProviderMode == PushProviderMode.Disabled)
            return services;

        services.AddSingleton<IPushDispatcher, PushDispatcher>();
        services.AddSingleton<IPushIdempotencyStore, RedisPushIdempotencyStore>();
        services.AddSingleton<IPushDlqStore, RedisPushDlqStore>();

        // 门禁4：无效 Token 可靠清理——有界队列 + 后台 worker（指数退避重试注销），
        // 与请求生命周期解耦，非 fire-and-forget。
        services.AddSingleton<PushInvalidTokenCleanupQueue>();
        services.AddHostedService<PushInvalidTokenCleanupWorker>();

        // 门禁3：密钥轮换启用时（配置了密钥环），启动后台渐进式重加密 worker。
        if (pushOptions.TokenEncryptionKeys is { Count: > 0 })
        {
            services.AddHostedService<PushTokenReencryptionWorker>();
        }

        if (pushOptions.ProviderMode == PushProviderMode.TestNoop)
        {
            services.AddSingleton<IPushProvider>(static sp =>
                new NoopPushProvider(PushPlatform.Fcm,
                    sp.GetRequiredService<ILogger<NoopPushProvider>>()));
            services.AddSingleton<IPushProvider>(static sp =>
                new NoopPushProvider(PushPlatform.Apns,
                    sp.GetRequiredService<ILogger<NoopPushProvider>>()));
            services.AddSingleton<IPushProvider>(static sp =>
                new NoopPushProvider(PushPlatform.WebPush,
                    sp.GetRequiredService<ILogger<NoopPushProvider>>()));
        }
        // Production 模式：部署须在此前（或通过外部 DI 覆盖）注册真实 FcmPushProvider /
        // ApnsPushProvider / WebPushProvider。PushProviderStartupValidator 启动时校验非 Noop。

        services.AddHostedService<PushDeliveryConsumerService>();

        if (pushOptions.ProviderMode == PushProviderMode.Production)
        {
            services.AddHostedService<PushProviderStartupValidator>();
        }

        return services;
    }
}
