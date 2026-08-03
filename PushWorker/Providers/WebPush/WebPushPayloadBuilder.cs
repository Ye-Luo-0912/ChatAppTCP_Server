using System.Text.Json.Serialization;

namespace ChatApp.PushWorker.Providers.WebPush;

/// <summary>
/// WebPush 通知 payload 构造器。
/// <para>
/// 构造 JSON payload，加密后发送到 push service。
/// 浏览器 Service Worker 通过 <c>event.data.json()</c> 读取。
/// </para>
/// </summary>
internal static class WebPushPayloadBuilder
{
    /// <summary>
    /// 构造 WebPush 通知 payload。
    /// <para>
    /// 结构：<c>{"title":"...","body":"...","data":{"key":"value"}}</c>
    /// </para>
    /// </summary>
    public static WebPushNotificationPayload BuildPayload(
        string title,
        string body,
        IReadOnlyDictionary<string, string>? customData)
    {
        var payload = new WebPushNotificationPayload
        {
            Title = title,
            Body = body
        };

        if (customData is { Count: > 0 })
        {
            payload.Data = new Dictionary<string, string>(customData);
        }

        return payload;
    }
}

internal sealed class WebPushNotificationPayload
{
    [JsonPropertyName("title")]
    public required string Title { get; init; }

    [JsonPropertyName("body")]
    public required string Body { get; init; }

    [JsonPropertyName("data")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, string>? Data { get; set; }
}

[JsonSerializable(typeof(WebPushNotificationPayload))]
[JsonSerializable(typeof(WebPushSubscription))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
internal sealed partial class WebPushPayloadContext : JsonSerializerContext;
