using System.Text.Json;
using System.Text.Json.Serialization;

namespace ChatApp.PushWorker.Providers.WebPush;

/// <summary>
/// WebPush 订阅信息（客户端 subscribe() 返回的 PushSubscription JSON）。
/// <para>
/// 存储在 <see cref="PushTokenRecord"/> 的 Token 字段中（JSON 字符串）。
/// </para>
/// </summary>
internal sealed class WebPushSubscription
{
    [JsonPropertyName("endpoint")]
    public required string Endpoint { get; init; }

    [JsonPropertyName("keys")]
    public required WebPushSubscriptionKeys Keys { get; init; }

    /// <summary>
    /// 解析订阅 JSON。JSON 格式：
    /// <c>{"endpoint":"https://...","keys":{"p256dh":"...","auth":"..."}}</c>
    /// </summary>
    public static WebPushSubscription Parse(string json)
        => JsonSerializer.Deserialize(json, WebPushPayloadContext.Default.WebPushSubscription)
            ?? throw new JsonException("WebPush subscription JSON 解析为 null。");

    /// <summary>解码 p256dh 公钥（Base64Url → ECDsa P-256 公钥原始字节）。</summary>
    public byte[] DecodeP256dh() => Base64UrlDecode(Keys.P256dh);

    /// <summary>解码 auth secret（Base64Url → 16 字节）。</summary>
    public byte[] DecodeAuth() => Base64UrlDecode(Keys.Auth);

    private static byte[] Base64UrlDecode(string s)
    {
        var padded = s.Replace('-', '+').Replace('_', '/');
        padded = padded.PadRight(padded.Length + (4 - padded.Length % 4) % 4, '=');
        return Convert.FromBase64String(padded);
    }
}

internal sealed class WebPushSubscriptionKeys
{
    [JsonPropertyName("p256dh")]
    public required string P256dh { get; init; }

    [JsonPropertyName("auth")]
    public required string Auth { get; init; }
}
