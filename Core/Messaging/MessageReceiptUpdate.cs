namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageReceiptUpdate
{
    public string? MessageId { get; set; }
    public long ReceiverUserId { get; set; }
    public MessageReceiptState State { get; set; }
    public DateTime OccurredUtc { get; set; }
}