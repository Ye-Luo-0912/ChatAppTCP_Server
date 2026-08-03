using System.Text.Json.Serialization;

namespace ChatApp.PushWorker.Providers.Apns;

/// <summary>
/// APNs payload 构造器。
/// <para>
/// 构造 <c>POST /3/device/{token}</c> 请求体（JSON）。
/// 参考：<see href="https://developer.apple.com/documentation/usernotifications/sending-notification-requests-to-apns"/>
/// </para>
/// </summary>
internal static class ApnsPayloadBuilder
{
    /// <summary>
    /// 构造 APNs 推送 payload。
    /// <para>
    /// 结构：<c>{"aps":{"alert":{"title":"...","body":"..."}},"customKey":"customValue"}</c>
    /// customData 的 key-value 合并到顶层（与 aps 平级），供客户端自定义处理。
    /// </para>
    /// </summary>
    public static ApnsPayload BuildPayload(
        string title,
        string body,
        IReadOnlyDictionary<string, string>? customData)
    {
        var payload = new ApnsPayload
        {
            Aps = new ApnsAps
            {
                Alert = new ApnsAlert { Title = title, Body = body }
            }
        };

        if (customData is { Count: > 0 })
        {
            var dict = new Dictionary<string, object>(customData.Count);
            foreach (var (key, value) in customData)
            {
                dict[key] = value;
            }
            payload.CustomData = dict;
        }

        return payload;
    }
}

// ── APNs payload DTOs ──

internal sealed class ApnsPayload
{
    [JsonPropertyName("aps")]
    public required ApnsAps Aps { get; init; }

    /// <summary>自定义数据（与 aps 平级，供客户端 app 读取）。</summary>
    [JsonExtensionData]
    public Dictionary<string, object>? CustomData { get; set; }
}

internal sealed class ApnsAps
{
    [JsonPropertyName("alert")]
    public required ApnsAlert Alert { get; init; }

    /// <summary>可选：badge 计数。</summary>
    [JsonPropertyName("badge")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Badge { get; init; }

    /// <summary>可选：推送声音（默认 "default"）。</summary>
    [JsonPropertyName("sound")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Sound { get; init; }
}

internal sealed class ApnsAlert
{
    [JsonPropertyName("title")]
    public required string Title { get; init; }

    [JsonPropertyName("body")]
    public required string Body { get; init; }
}

[JsonSerializable(typeof(ApnsPayload))]
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal sealed partial class ApnsPayloadContext : JsonSerializerContext;
