namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageReceiptRequest
{
    public string? MessageId { get; set; }
    public MessageReceiptState State { get; set; }
}