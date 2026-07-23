namespace ChatApp.TcpGateway.Core.Messaging;

public sealed class MessageAcknowledgement
{
    public string? ClientMessageId { get; set; }
    public string? CommandId { get; set; }
    public bool Accepted { get; set; }
    public string? ErrorCode { get; set; }
    public string? ErrorMessage { get; set; }
    public DateTime AcknowledgedUtc { get; set; }
}
