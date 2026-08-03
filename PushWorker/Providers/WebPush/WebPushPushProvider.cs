using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.TcpGateway.Infrastructure.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.PushWorker.Providers.WebPush;

/// <summary>
/// Web Push API（浏览器 Service Worker）推送 Provider。
/// <para>
/// 使用 RFC 8291 AES128GCM 加密 + VAPID JWT 认证。
/// 推送目标由客户端订阅 endpoint 提供，Token 字段存储订阅 JSON：
/// <c>{"endpoint":"https://...","keys":{"p256dh":"...","auth":"..."}}</c>
/// </para>
/// </summary>
internal sealed partial class WebPushPushProvider : IPushProvider, IDisposable
{
    private readonly WebPushOptions _options;
    private readonly HttpClient _httpClient;
    private readonly ILogger<WebPushPushProvider> _logger;

    private ECDsa? _vapidKey;
    private readonly SemaphoreSlim _keyLock = new(1, 1);

    public PushPlatform Platform => PushPlatform.WebPush;

    public WebPushPushProvider(
        WebPushOptions options,
        HttpClient httpClient,
        ILogger<WebPushPushProvider> logger)
    {
        _options = options;
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task<PushProviderResult> SendAsync(
        string token,
        string title,
        string body,
        string? collapseKey,
        IReadOnlyDictionary<string, string>? customData,
        CancellationToken cancellationToken = default)
    {
        // Token 是订阅 JSON
        WebPushSubscription subscription;
        try
        {
            subscription = WebPushSubscription.Parse(token);
        }
        catch (JsonException ex)
        {
            LogInvalidSubscription(_logger, ex, token.Length);
            return PushProviderResult.Fail("invalid_token");
        }

        // 构造明文 payload
        var payload = WebPushPayloadBuilder.BuildPayload(title, body, customData);
        var plaintext = JsonSerializer.SerializeToUtf8Bytes(payload, WebPushPayloadContext.Default.WebPushNotificationPayload);

        // RFC 8291 加密
        byte[] encryptedBody;
        try
        {
            encryptedBody = WebPushEncryptor.Encrypt(plaintext, subscription);
        }
        catch (CryptographicException ex)
        {
            LogEncryptionFailed(_logger, ex);
            return PushProviderResult.Fail("invalid_token");
        }

        // VAPID JWT
        var vapidKey = await GetVapidKeyAsync(cancellationToken).ConfigureAwait(false);
        var endpointUri = new Uri(subscription.Endpoint);
        var jwt = BuildVapidJwt(vapidKey, _options.VapidSubject!, endpointUri);

        using var request = new HttpRequestMessage(HttpMethod.Post, subscription.Endpoint);
        request.Version = System.Net.HttpVersion.Version20;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        request.Headers.TryAddWithoutValidation(
            "Authorization",
            $"vapid t={jwt},k={_options.VapidPublicKey}");
        request.Headers.TryAddWithoutValidation("Content-Encoding", "aes128gcm");
        request.Headers.TryAddWithoutValidation("TTL", "2419200");

        // Topic（用于折叠同一 topic 的推送，类似 collapseKey）
        if (!string.IsNullOrWhiteSpace(collapseKey))
            request.Headers.TryAddWithoutValidation("Topic", collapseKey);

        request.Content = new ByteArrayContent(encryptedBody);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

        try
        {
            using var response = await _httpClient
                .SendAsync(request, cancellationToken)
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode)
                return PushProviderResult.Ok();

            return ParseErrorResponse(response);
        }
        catch (HttpRequestException ex)
        {
            LogSendFailed(_logger, ex, subscription.Endpoint.Length);
            return PushProviderResult.Fail("provider_unavailable");
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            LogSendTimeout(_logger, ex);
            return PushProviderResult.Fail("provider_unavailable");
        }
    }

    private static PushProviderResult ParseErrorResponse(HttpResponseMessage response)
    {
        var statusCode = (int)response.StatusCode;
        var retryAfter = response.Headers.RetryAfter?.Delta;

        // 404/410 → 订阅已过期/失效
        if (statusCode is 404 or 410)
            return PushProviderResult.Fail("invalid_token");

        // 429 → rate_limited
        if (statusCode == 429)
            return PushProviderResult.Fail("rate_limited", retryAfter);

        // 5xx → provider_unavailable
        if (statusCode >= 500)
            return PushProviderResult.Fail("provider_unavailable", retryAfter);

        // 400/413 → payload 过大或格式错误
        if (statusCode is 400 or 413)
            return PushProviderResult.Fail("payload_too_large");

        // 403 → VAPID 认证失败
        if (statusCode == 403)
            return PushProviderResult.Fail("provider_unavailable");

        return PushProviderResult.Fail("unknown");
    }

    private async Task<ECDsa> GetVapidKeyAsync(CancellationToken cancellationToken)
    {
        if (_vapidKey is not null)
            return _vapidKey;

        await _keyLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _vapidKey ??= LoadVapidKey();
            return _vapidKey;
        }
        finally
        {
            _keyLock.Release();
        }
    }

    private ECDsa LoadVapidKey()
    {
        var key = ECDsa.Create();
        key.ImportFromPem(_options.VapidPrivateKeyPem);
        return key;
    }

    /// <summary>
    /// 构建 VAPID JWT（ES256 签名）。
    /// <para>
    /// Header: <c>{"typ":"JWT","alg":"ES256"}</c>
    /// Payload: <c>{"aud":"{endpoint-origin}","exp":{now+12h},"sub":"{vapidSubject}"}</c>
    /// </para>
    /// </summary>
    private static string BuildVapidJwt(ECDsa key, string subject, Uri endpoint)
    {
        var now = DateTimeOffset.UtcNow;
        var origin = $"{endpoint.Scheme}://{endpoint.Host}:{endpoint.Port}";

        var header = new { typ = "JWT", alg = "ES256" };
        var payload = new
        {
            aud = origin,
            exp = now.AddHours(12).ToUnixTimeSeconds(),
            sub = subject
        };

        var headerJson = JsonSerializer.Serialize(header);
        var payloadJson = JsonSerializer.Serialize(payload);
        var headerB64 = Base64UrlEncode(headerJson);
        var payloadB64 = Base64UrlEncode(payloadJson);
        var signingInput = $"{headerB64}.{payloadB64}";

        var signature = key.SignData(
            Encoding.UTF8.GetBytes(signingInput),
            HashAlgorithmName.SHA256);

        return $"{signingInput}.{Base64UrlEncode(signature)}";
    }

    private static string Base64UrlEncode(string text)
        => Base64UrlEncode(Encoding.UTF8.GetBytes(text));

    private static string Base64UrlEncode(byte[] data)
        => Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    public void Dispose()
    {
        _keyLock.Dispose();
        _vapidKey?.Dispose();
    }

    [LoggerMessage(LogLevel.Warning, "WebPush: invalid subscription JSON (len={TokenLen}).")]
    static partial void LogInvalidSubscription(ILogger logger, Exception ex, int tokenLen);

    [LoggerMessage(LogLevel.Warning, "WebPush: payload encryption failed.")]
    static partial void LogEncryptionFailed(ILogger logger, Exception ex);

    [LoggerMessage(LogLevel.Warning, "WebPush send failed for endpoint (len={EndpointLen}).")]
    static partial void LogSendFailed(ILogger logger, Exception ex, int endpointLen);

    [LoggerMessage(LogLevel.Warning, "WebPush send timed out.")]
    static partial void LogSendTimeout(ILogger logger, Exception ex);
}
