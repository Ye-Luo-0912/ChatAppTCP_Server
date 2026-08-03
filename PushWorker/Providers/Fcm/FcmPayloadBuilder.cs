using System.Text.Json.Serialization;

namespace ChatApp.PushWorker.Providers.Fcm;

/// <summary>
/// FCM HTTP v1 API 消息构造器。
/// <para>
/// 构造 <c>POST /v1/projects/{projectId}/messages:send</c> 请求体。
/// 参考：<see href="https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages/send"/>
/// </para>
/// </summary>
internal static class FcmPayloadBuilder
{
    /// <summary>
    /// 构造 FCM send 请求体。
    /// </summary>
    public static FcmSendRequest BuildMessage(
        string token,
        string title,
        string body,
        string? collapseKey,
        IReadOnlyDictionary<string, string>? customData)
    {
        var message = new FcmMessage
        {
            Token = token,
            Notification = new FcmNotification { Title = title, Body = body }
        };

        if (customData is { Count: > 0 })
        {
            message.Data = new Dictionary<string, string>(customData);
        }

        // collapseKey：同一会话的推送折叠，避免锁屏刷屏。
        // Android 用 android.collapse_key；iOS 用 apns.headers["apns-collapse-id"]。
        if (!string.IsNullOrWhiteSpace(collapseKey))
        {
            message.Android = new FcmAndroidConfig { CollapseKey = collapseKey };
            message.Apns = new FcmApnsConfig
            {
                Headers = new Dictionary<string, string> { ["apns-collapse-id"] = collapseKey }
            };
        }

        return new FcmSendRequest { Message = message };
    }
}

// ── FCM REST API DTOs ──

internal sealed class FcmSendRequest
{
    [JsonPropertyName("message")]
    public required FcmMessage Message { get; init; }
}

internal sealed class FcmMessage
{
    [JsonPropertyName("token")]
    public required string Token { get; init; }

    [JsonPropertyName("notification")]
    public FcmNotification? Notification { get; init; }

    [JsonPropertyName("data")]
    public Dictionary<string, string>? Data { get; set; }

    [JsonPropertyName("android")]
    public FcmAndroidConfig? Android { get; set; }

    [JsonPropertyName("apns")]
    public FcmApnsConfig? Apns { get; set; }
}

internal sealed class FcmNotification
{
    [JsonPropertyName("title")]
    public required string Title { get; init; }

    [JsonPropertyName("body")]
    public required string Body { get; init; }
}

internal sealed class FcmAndroidConfig
{
    [JsonPropertyName("collapse_key")]
    public required string CollapseKey { get; init; }
}

internal sealed class FcmApnsConfig
{
    [JsonPropertyName("headers")]
    public required Dictionary<string, string> Headers { get; init; }
}

internal sealed class FcmOAuthToken
{
    [JsonPropertyName("access_token")]
    public required string AccessToken { get; init; }

    [JsonPropertyName("expires_in")]
    public required int ExpiresIn { get; init; }

    [JsonPropertyName("token_type")]
    public string? TokenType { get; init; }
}

/// <summary>
/// Google Service Account JSON（仅提取推送所需字段）。
/// </summary>
internal sealed class FcmServiceAccount
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("project_id")]
    public string? ProjectId { get; set; }

    [JsonPropertyName("private_key_id")]
    public string? PrivateKeyId { get; set; }

    [JsonPropertyName("private_key")]
    public required string PrivateKey { get; set; }

    [JsonPropertyName("client_email")]
    public required string ClientEmail { get; set; }

    [JsonPropertyName("client_id")]
    public string? ClientId { get; set; }

    [JsonPropertyName("auth_uri")]
    public string? AuthUri { get; set; }

    [JsonPropertyName("token_uri")]
    public string? TokenUri { get; set; }

    public static FcmServiceAccount Load(FcmOptions options)
    {
        string json;
        if (!string.IsNullOrWhiteSpace(options.ServiceAccountJson))
        {
            json = options.ServiceAccountJson!;
        }
        else if (!string.IsNullOrWhiteSpace(options.ServiceAccountKeyPath))
        {
            json = File.ReadAllText(options.ServiceAccountKeyPath!);
        }
        else
        {
            throw new InvalidOperationException(
                "FCM Service Account 未配置：需设置 ServiceAccountJson 或 ServiceAccountKeyPath。");
        }

        var sa = System.Text.Json.JsonSerializer.Deserialize(
            json, FcmPayloadContext.Default.FcmServiceAccount);
        return sa ?? throw new InvalidOperationException("FCM Service Account JSON 解析失败。");
    }
}

[JsonSerializable(typeof(FcmSendRequest))]
[JsonSerializable(typeof(FcmOAuthToken))]
[JsonSerializable(typeof(FcmServiceAccount))]
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal sealed partial class FcmPayloadContext : JsonSerializerContext;
