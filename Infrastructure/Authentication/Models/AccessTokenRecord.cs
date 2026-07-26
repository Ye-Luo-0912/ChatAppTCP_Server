using System.Text.Json.Serialization;

namespace ChatApp.TcpGateway.Infrastructure.Authentication.Models;

public sealed class AccessTokenRecord
{
    [JsonPropertyName("u")]
    public required long UserId { get; set; }

    [JsonPropertyName("n")]
    public required string UserName { get; set; }

    [JsonPropertyName("r")]
    public string[]? Roles { get; set; }

    [JsonPropertyName("e")]
    public required long ExpiresAtMs { get; set; }

    [JsonPropertyName("s")]
    public string? SessionId { get; set; }

    /// <summary>
    /// 服务器签发的设备标识（权威身份），客户端不可篡改。
    /// </summary>
    [JsonPropertyName("did")]
    public string? DeviceId { get; set; }

    [JsonPropertyName("d")]
    public ulong? DeviceIdHash { get; set; }
}
