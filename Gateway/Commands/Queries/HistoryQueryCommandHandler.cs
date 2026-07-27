using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging.Conversations;
using ChatApp.TcpGateway.Core.Messaging.History;
using ChatApp.TcpGateway.Core.Messaging.Sync;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Commands.Queries;

/// <summary>
/// 查询类命令处理器（MessageHistoryRequest / ConversationListRequest / SyncBootstrapRequest）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec、<see cref="IRealtimeMessageBus"/>，不再依赖 service 私有字段。
/// 行为与原内联 handler 完全等价（校验顺序、错误码、metric 与日志事件、字节预算截断逻辑）。
/// </para>
/// <para>
/// 各查询的校验、调用与响应逻辑拆分至 partial 文件：
/// <list type="bullet">
/// <item><see cref="HistoryQueryCommandHandler.History"/> — MessageHistoryRequest + 字节预算截断</item>
/// <item><see cref="HistoryQueryCommandHandler.ConversationList"/> — ConversationListRequest + 字节预算截断</item>
/// <item><see cref="HistoryQueryCommandHandler.SyncBootstrap"/> — SyncBootstrapRequest + 分段字节预算截断</item>
/// </list>
/// </para>
/// </summary>
internal sealed partial class HistoryQueryCommandHandler : ICommandHandler
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly IPayloadCodec<MessageHistoryRequest> _messageHistoryRequestCodec;
    private readonly IPayloadCodec<MessageHistoryResponse> _messageHistoryResponseCodec;
    private readonly IPayloadCodec<MessageHistoryItem[]> _messageHistoryItemCodec;
    private readonly IPayloadCodec<ConversationListRequest> _conversationListRequestCodec;
    private readonly IPayloadCodec<ConversationListResponse> _conversationListResponseCodec;
    private readonly IPayloadCodec<ConversationListItem[]> _conversationListItemCodec;
    private readonly IPayloadCodec<SyncBootstrapRequest> _syncBootstrapRequestCodec;
    private readonly IPayloadCodec<SyncBootstrapResponse> _syncBootstrapResponseCodec;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger<HistoryQueryCommandHandler> _logger;

    public HistoryQueryCommandHandler(
        IRealtimeMessageBus messageBus,
        IPayloadCodec<MessageHistoryRequest> messageHistoryRequestCodec,
        IPayloadCodec<MessageHistoryResponse> messageHistoryResponseCodec,
        IPayloadCodec<MessageHistoryItem[]> messageHistoryItemCodec,
        IPayloadCodec<ConversationListRequest> conversationListRequestCodec,
        IPayloadCodec<ConversationListResponse> conversationListResponseCodec,
        IPayloadCodec<ConversationListItem[]> conversationListItemCodec,
        IPayloadCodec<SyncBootstrapRequest> syncBootstrapRequestCodec,
        IPayloadCodec<SyncBootstrapResponse> syncBootstrapResponseCodec,
        GatewayMetrics metrics,
        ILogger<HistoryQueryCommandHandler> logger)
    {
        _messageBus = messageBus;
        _messageHistoryRequestCodec = messageHistoryRequestCodec;
        _messageHistoryResponseCodec = messageHistoryResponseCodec;
        _messageHistoryItemCodec = messageHistoryItemCodec;
        _conversationListRequestCodec = conversationListRequestCodec;
        _conversationListResponseCodec = conversationListResponseCodec;
        _conversationListItemCodec = conversationListItemCodec;
        _syncBootstrapRequestCodec = syncBootstrapRequestCodec;
        _syncBootstrapResponseCodec = syncBootstrapResponseCodec;
        _metrics = metrics;
        _logger = logger;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.MessageHistoryRequest => HandleMessageHistoryRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.ConversationListRequest => HandleConversationListRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.SyncBootstrapRequest => HandleSyncBootstrapRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };
}
