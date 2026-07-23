using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Serialization;
using Microsoft.Extensions.Logging.Abstractions;

var natsUrl = args.Length == 0
    ? "nats://127.0.0.1:4222"
    : args[0];
using var timeout = new CancellationTokenSource(
    TimeSpan.FromSeconds(30));

var clientMessageId = Guid.CreateVersion7().ToString("N");
const long senderUserId = 9_000_000_001;
const long receiverUserId = 9_000_000_002;
var conversationId = ConversationId.CreateDirect(senderUserId, receiverUserId);
var commandId = CreateMessageCommandId(
    senderUserId,
    clientMessageId);
var command = new IncomingMessageCommand
{
    CommandId = commandId,
    ClientMessageId = clientMessageId,
    SenderUserId = senderUserId,
    SenderSessionId = "pipeline-probe-sender",
    ReceiverUserId = receiverUserId,
    Content = $"pipeline-probe-{clientMessageId}",
    ReceivedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
};

await using var messageBus = new NatsRealtimeMessageBus(
    new RealtimeIntegrationOptions
    {
        Url = natsUrl,
        ClientName = "chatapp-realtime-pipeline-probe",
        InstanceId = "pipeline-probe",
        GatewayConsumerPrefix = "chatapp-pipeline-probe",
        ManageStreams = false
    },
    NullLogger<NatsRealtimeMessageBus>.Instance);

var ping = await messageBus.PingAsync(timeout.Token);
var messageStartedAt = Stopwatch.GetTimestamp();
var messageDeliveryTask = WaitForEventAsync(
    messageBus,
    commandId,
    RealtimeEventType.MessageReceived,
    timeout.Token);

await Task.Delay(
    TimeSpan.FromMilliseconds(250),
    timeout.Token);
await messageBus.PublishIncomingMessageAsync(
    command,
    timeout.Token);

var receivedEvent = await messageDeliveryTask;
var messageElapsed = Stopwatch.GetElapsedTime(messageStartedAt);

var receiptCommand = new MessageReceiptCommand
{
    CommandId = CreateReceiptCommandId(
        receiverUserId,
        commandId,
        MessageReceiptType.Read),
    MessageId = commandId,
    ReceiverUserId = receiverUserId,
    ReceiverSessionId = "pipeline-probe-receiver",
    ReceiptType = MessageReceiptType.Read,
    OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
};
var receiptStartedAt = Stopwatch.GetTimestamp();
var receiptDeliveryTask = WaitForEventAsync(
    messageBus,
    commandId,
    RealtimeEventType.MessageReceiptUpdated,
    timeout.Token);

await Task.Delay(
    TimeSpan.FromMilliseconds(250),
    timeout.Token);
await messageBus.PublishMessageReceiptAsync(
    receiptCommand,
    timeout.Token);

var receiptEvent = await receiptDeliveryTask;
var receiptElapsed = Stopwatch.GetElapsedTime(receiptStartedAt);
var receiptPayload = RealtimeWireSerializer
    .DeserializeMessageReceipt(receiptEvent.PayloadJson!);
if (receiptPayload is null ||
    receiptPayload.ReceiptType != MessageReceiptType.Read ||
    receiptPayload.ReceiverUserId != receiverUserId)
{
    throw new InvalidOperationException(
        "Receipt event payload did not match the probe command.");
}

var historyStartedAt = Stopwatch.GetTimestamp();
var historyPage = await messageBus.QueryMessageHistoryAsync(
    new MessageHistoryQuery
    {
        RequestId = Guid.CreateVersion7().ToString("N"),
        UserId = receiverUserId,
        ConversationId = conversationId,
        Limit = 20
    },
    timeout.Token);
var historyElapsed = Stopwatch.GetElapsedTime(historyStartedAt);
var historyMessage = historyPage.Items.SingleOrDefault(
    item => string.Equals(
        item.MessageId,
        commandId,
        StringComparison.Ordinal));
if (!historyPage.Succeeded ||
    historyMessage?.DeliveredAtMs is null ||
    historyMessage.ReadAtMs is null)
{
    throw new InvalidOperationException(
        "Conversation history query did not return the persisted read message.");
}

var listStartedAt = Stopwatch.GetTimestamp();
var listPage = await messageBus.QueryConversationListAsync(
    new ConversationListQuery
    {
        RequestId = Guid.CreateVersion7().ToString("N"),
        UserId = receiverUserId,
        Limit = 20
    },
    timeout.Token);
var listElapsed = Stopwatch.GetElapsedTime(listStartedAt);
var listed = listPage.Items.SingleOrDefault(
    item => string.Equals(item.ConversationId, conversationId, StringComparison.Ordinal));
if (!listPage.Succeeded ||
    listed is null ||
    !string.Equals(listed.LastMessageId, commandId, StringComparison.Ordinal))
{
    throw new InvalidOperationException(
        "Conversation list did not contain the probe conversation summary.");
}

var markReadStartedAt = Stopwatch.GetTimestamp();
var markRead = await messageBus.MarkConversationReadAsync(
    new ConversationMarkReadCommand
    {
        RequestId = Guid.CreateVersion7().ToString("N"),
        UserId = receiverUserId,
        ConversationId = conversationId,
        ReadAtMs = historyMessage.ReceivedAtMs,
        ReadMessageId = commandId
    },
    timeout.Token);
var markReadElapsed = Stopwatch.GetElapsedTime(markReadStartedAt);
if (!markRead.Succeeded || markRead.UnreadCount != 0)
{
    throw new InvalidOperationException(
        $"Conversation mark-read failed or left unread={markRead.UnreadCount}.");
}

var syncStartedAt = Stopwatch.GetTimestamp();
var sync = await messageBus.QuerySyncBootstrapAsync(
    new SyncBootstrapQuery
    {
        RequestId = Guid.CreateVersion7().ToString("N"),
        UserId = receiverUserId,
        ListLimit = 20,
        HistoryLimitPerConversation = 10,
        MaxConversationsWithHistory = 5
    },
    timeout.Token);
var syncElapsed = Stopwatch.GetElapsedTime(syncStartedAt);
if (!sync.Succeeded ||
    sync.Conversations.All(item =>
        !string.Equals(item.ConversationId, conversationId, StringComparison.Ordinal)))
{
    throw new InvalidOperationException(
        "SyncBootstrap did not include the probe conversation.");
}

Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"NATS ping: {ping.TotalMilliseconds:F2} ms"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Message {receivedEvent.MessageId} completed persistence and Outbox in {messageElapsed.TotalMilliseconds:F2} ms"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Read receipt completed persistence and Outbox in {receiptElapsed.TotalMilliseconds:F2} ms"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Conversation history query returned the read message in {historyElapsed.TotalMilliseconds:F2} ms"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Conversation list query completed in {listElapsed.TotalMilliseconds:F2} ms"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"Conversation mark-read completed in {markReadElapsed.TotalMilliseconds:F2} ms"));
Console.WriteLine(
    string.Create(
        CultureInfo.InvariantCulture,
        $"SyncBootstrap completed in {syncElapsed.TotalMilliseconds:F2} ms"));
return 0;

static async Task<RealtimeEvent> WaitForEventAsync(
    IRealtimeMessageBus messageBus,
    string messageId,
    RealtimeEventType eventType,
    CancellationToken cancellationToken)
{
    await foreach (var delivery in messageBus
                       .ConsumeEventsAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
        await delivery.AckAsync(cancellationToken);

        if (delivery.Event.Type == eventType &&
            string.Equals(
                delivery.Event.MessageId,
                messageId,
                StringComparison.Ordinal))
        {
            return delivery.Event;
        }
    }

    throw new InvalidOperationException(
        $"Timed out waiting for {eventType} event for message {messageId}.");
}

static string CreateMessageCommandId(long senderUserId, string clientMessageId)
{
    var source = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}");
    return Convert.ToHexStringLower(SHA256.HashData(source));
}

static string CreateReceiptCommandId(
    long receiverUserId,
    string messageId,
    MessageReceiptType receiptType)
{
    var source = Encoding.UTF8.GetBytes(
        $"{receiverUserId}:{messageId}:{(byte)receiptType}");
    return Convert.ToHexStringLower(SHA256.HashData(source));
}
