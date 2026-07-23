namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageReceiptAcknowledgement
{
    public string? CommandId { get; set; }
    public string? MessageId { get; set; }
    public MessageReceiptState State { get; set; }
    public bool Accepted { get; set; }
    public string? ErrorCode { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTime AcknowledgedUtc { get; set; }
}