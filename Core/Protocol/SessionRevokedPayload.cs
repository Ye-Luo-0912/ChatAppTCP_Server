using System.Text.Json.Serialization;

namespace ChatApp.TcpGateway.Core.Protocol;

/// <summary>
/// P0-6: SessionRevoked 事件的结构化 payload。
/// 替代旧实现中 PayloadJson 携带裸 ConnectionLeaseId 字符串的做法。
/// 后续将拆分 TransportId（可广播）与 LeaseOwnerToken（仅 Redis CAS），
/// 当前阶段 TransportId 仍使用 ConnectionLeaseId 值，但通过正式契约承载。
/// </summary>
public sealed class SessionRevokedPayload
{
    [JsonPropertyName("transportId")]
    public string? TransportId { get; init; }
}