using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Integration;
using ChatApp.TcpGateway.Core.Messaging;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Core.Serialization;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Dispatching;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.TcpGateway.Gateway.Commands.Messaging;

/// <summary>
/// 消息类命令处理器（ChatMessage / MessageReceipt / MessageRecallRequest / MessageEditRequest）。
/// <para>
/// 从 <c>TcpGatewayService</c> 抽取，自带 codec、<see cref="IRealtimeMessageBus"/>、
/// <see cref="TimeProvider"/> 与 <see cref="TcpGatewayOptions"/>，不再依赖 service 私有字段。
/// 行为与原内联 handler 完全等价（校验顺序、错误码、metric 与日志事件）。
/// </para>
/// <para>
/// 各命令的校验、发布与 ACK 逻辑拆分至 partial 文件：
/// <list type="bullet">
/// <item><see cref="MessagingCommandHandler.ChatMessage"/> — ChatMessage + 辅助方法</item>
/// <item><see cref="MessagingCommandHandler.Receipt"/> — MessageReceipt + ReceiptCommandId</item>
/// <item><see cref="MessagingCommandHandler.Recall"/> — MessageRecallRequest</item>
/// <item><see cref="MessagingCommandHandler.Edit"/> — MessageEditRequest + 控制字符检查</item>
/// </list>
/// </para>
/// </summary>
internal sealed partial class MessagingCommandHandler : ICommandHandler
{
    private readonly IRealtimeMessageBus _messageBus;
    private readonly IPayloadCodec<ChatMessage> _chatMessageCodec;
    private readonly IPayloadCodec<MessageAcknowledgement> _messageAcknowledgementCodec;
    private readonly IPayloadCodec<MessageReceiptRequest> _messageReceiptRequestCodec;
    private readonly IPayloadCodec<MessageReceiptAcknowledgement> _messageReceiptAcknowledgementCodec;
    private readonly IPayloadCodec<MessageRecallRequest> _messageRecallRequestCodec;
    private readonly IPayloadCodec<MessageRecallAcknowledgement> _messageRecallAcknowledgementCodec;
    private readonly IPayloadCodec<MessageEditRequest> _messageEditRequestCodec;
    private readonly IPayloadCodec<MessageEditAcknowledgement> _messageEditAcknowledgementCodec;
    private readonly GatewayMetrics _metrics;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<MessagingCommandHandler> _logger;
    private readonly TcpGatewayOptions _options;

    public MessagingCommandHandler(
        IRealtimeMessageBus messageBus,
        IPayloadCodec<ChatMessage> chatMessageCodec,
        IPayloadCodec<MessageAcknowledgement> messageAcknowledgementCodec,
        IPayloadCodec<MessageReceiptRequest> messageReceiptRequestCodec,
        IPayloadCodec<MessageReceiptAcknowledgement> messageReceiptAcknowledgementCodec,
        IPayloadCodec<MessageRecallRequest> messageRecallRequestCodec,
        IPayloadCodec<MessageRecallAcknowledgement> messageRecallAcknowledgementCodec,
        IPayloadCodec<MessageEditRequest> messageEditRequestCodec,
        IPayloadCodec<MessageEditAcknowledgement> messageEditAcknowledgementCodec,
        GatewayMetrics metrics,
        TimeProvider timeProvider,
        ILogger<MessagingCommandHandler> logger,
        IOptions<TcpGatewayOptions> options)
    {
        _messageBus = messageBus;
        _chatMessageCodec = chatMessageCodec;
        _messageAcknowledgementCodec = messageAcknowledgementCodec;
        _messageReceiptRequestCodec = messageReceiptRequestCodec;
        _messageReceiptAcknowledgementCodec = messageReceiptAcknowledgementCodec;
        _messageRecallRequestCodec = messageRecallRequestCodec;
        _messageRecallAcknowledgementCodec = messageRecallAcknowledgementCodec;
        _messageEditRequestCodec = messageEditRequestCodec;
        _messageEditAcknowledgementCodec = messageEditAcknowledgementCodec;
        _metrics = metrics;
        _timeProvider = timeProvider;
        _logger = logger;
        _options = options.Value;
    }

    public ValueTask ExecuteAsync(
        PacketFrame frame,
        CommandContext context,
        CancellationToken cancellationToken) => frame.Command switch
    {
        PacketCommand.ChatMessage => HandleChatMessageAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.MessageReceipt => HandleMessageReceiptAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.MessageRecallRequest => HandleMessageRecallRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        PacketCommand.MessageEditRequest => HandleMessageEditRequestAsync(
            frame.Payload, context.Session, cancellationToken),
        _ => default
    };
}
