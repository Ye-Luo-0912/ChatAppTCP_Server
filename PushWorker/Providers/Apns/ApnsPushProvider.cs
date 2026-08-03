using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.TcpGateway.Infrastructure.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.PushWorker.Providers.Apns;

/// <summary>
/// Apple Push Notification service (HTTP/2 API) 推送 Provider。
/// <para>
/// 使用 Provider JWT（p8 私钥 ES256 签名，最长 1h 有效）认证。
/// 发送 <c>POST https://api.push.apple.com/3/device/{token}</c>。
/// </para>
/// </summary>
internal sealed partial class ApnsPushProvider : IPushProvider, IDisposable
{
    private readonly ApnsOptions _options;
    private readonly HttpClient _httpClient;
    private readonly ILogger<ApnsPushProvider> _logger;

    private readonly SemaphoreSlim _jwtLock = new(1, 1);
    private ECDsa? _ecdsaKey;
    private string? _cachedJwt;
    private DateTimeOffset _jwtExpiresAt;

    public PushPlatform Platform => PushPlatform.Apns;

    public ApnsPushProvider(
        ApnsOptions options,
        HttpClient httpClient,
        ILogger<ApnsPushProvider> logger)
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
        var jwt = await GetProviderJwtAsync(cancellationToken).ConfigureAwait(false);
        var payload = ApnsPayloadBuilder.BuildPayload(title, body, customData);

        // APNs token 是 hex 字符串，URL 安全
        var requestUrl = $"/3/device/{token}";

        using var request = new HttpRequestMessage(HttpMethod.Post, requestUrl);
        request.Version = System.Net.HttpVersion.Version20;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        request.Headers.Authorization = new AuthenticationHeaderValue("bearer", jwt);
        request.Headers.TryAddWithoutValidation("apns-topic", _options.BundleId);
        request.Headers.TryAddWithoutValidation("apns-push-type", "alert");
        request.Headers.TryAddWithoutValidation("apns-priority", "5");

        if (!string.IsNullOrWhiteSpace(collapseKey))
            request.Headers.TryAddWithoutValidation("apns-collapse-id", collapseKey);

        request.Content = JsonContent.Create(payload, ApnsPayloadContext.Default.ApnsPayload);

        try
        {
            using var response = await _httpClient
                .SendAsync(request, cancellationToken)
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode)
                return PushProviderResult.Ok();

            return await ParseErrorResponseAsync(response, cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            LogSendFailed(_logger, ex, token.Length);
            return PushProviderResult.Fail("provider_unavailable");
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            LogSendTimeout(_logger, ex);
            return PushProviderResult.Fail("provider_unavailable");
        }
    }

    private async Task<PushProviderResult> ParseErrorResponseAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        var retryAfter = response.Headers.RetryAfter?.Delta;
        var statusCode = (int)response.StatusCode;

        // 429 → rate_limited
        if (statusCode == 429)
            return PushProviderResult.Fail("rate_limited", retryAfter);

        // 5xx → provider_unavailable
        if (statusCode >= 500)
            return PushProviderResult.Fail("provider_unavailable", retryAfter);

        // 410 Gone → 令牌已注销
        if (statusCode == 410)
            return PushProviderResult.Fail("invalid_token");

        // 400 → 可能是 BadDeviceToken / Unregistered
        if (statusCode == 400)
        {
            var body = await response.Content
                .ReadAsStringAsync(cancellationToken)
                .ConfigureAwait(false);
            var isInvalidToken = body.Contains("Unregistered", StringComparison.OrdinalIgnoreCase)
                || body.Contains("BadDeviceToken", StringComparison.OrdinalIgnoreCase)
                || body.Contains("DeviceTokenNotForTopic", StringComparison.OrdinalIgnoreCase);
            return PushProviderResult.Fail(isInvalidToken ? "invalid_token" : "payload_too_large");
        }

        // 403 → 认证问题（JWT 过期/无效），清除缓存触发重新生成
        if (statusCode == 403)
        {
            _cachedJwt = null;
            return PushProviderResult.Fail("provider_unavailable");
        }

        return PushProviderResult.Fail("unknown");
    }

    /// <summary>
    /// 获取 Provider JWT（ES256 签名，带缓存）。APNs JWT 最长 1h 有效，提前 5min 刷新。
    /// </summary>
    private async Task<string> GetProviderJwtAsync(CancellationToken cancellationToken)
    {
        if (_cachedJwt is not null && DateTimeOffset.UtcNow < _jwtExpiresAt)
            return _cachedJwt;

        await _jwtLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_cachedJwt is not null && DateTimeOffset.UtcNow < _jwtExpiresAt)
                return _cachedJwt;

            _ecdsaKey ??= LoadEcdsaKey();
            _cachedJwt = BuildProviderJwt(_ecdsaKey, _options.TeamId!, _options.KeyId!);
            // APNs JWT 最长 1h，提前 5min 刷新
            _jwtExpiresAt = DateTimeOffset.UtcNow.AddMinutes(55);
            return _cachedJwt;
        }
        finally
        {
            _jwtLock.Release();
        }
    }

    private ECDsa LoadEcdsaKey()
    {
        var key = ECDsa.Create();
        key.ImportFromPem(_options.PrivateKeyPem);
        return key;
    }

    /// <summary>
    /// 构建 APNs Provider JWT（ES256 签名）。
    /// <para>
    /// Header: <c>{"alg":"ES256","kid":"{keyId}","typ":"JWT"}</c>
    /// Payload: <c>{"iss":"{teamId}","iat":{timestamp}}</c>
    /// </para>
    /// </summary>
    private static string BuildProviderJwt(ECDsa key, string teamId, string keyId)
    {
        var now = DateTimeOffset.UtcNow;
        var header = new { alg = "ES256", kid = keyId, typ = "JWT" };
        var payload = new { iss = teamId, iat = now.ToUnixTimeSeconds() };

        var headerJson = JsonSerializer.Serialize(header);
        var payloadJson = JsonSerializer.Serialize(payload);
        var headerB64 = Base64UrlEncode(headerJson);
        var payloadB64 = Base64UrlEncode(payloadJson);
        var signingInput = $"{headerB64}.{payloadB64}";

        var signature = key.SignData(
            System.Text.Encoding.UTF8.GetBytes(signingInput),
            HashAlgorithmName.SHA256);

        return $"{signingInput}.{Base64UrlEncode(signature)}";
    }

    private static string Base64UrlEncode(string text)
        => Base64UrlEncode(System.Text.Encoding.UTF8.GetBytes(text));

    private static string Base64UrlEncode(byte[] data)
        => Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    public void Dispose()
    {
        _jwtLock.Dispose();
        _ecdsaKey?.Dispose();
    }

    [LoggerMessage(LogLevel.Warning, "APNs send failed for token (len={TokenLen}).")]
    static partial void LogSendFailed(ILogger logger, Exception ex, int tokenLen);

    [LoggerMessage(LogLevel.Warning, "APNs send timed out.")]
    static partial void LogSendTimeout(ILogger logger, Exception ex);
}
