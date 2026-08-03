using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using ChatApp.Realtime.Abstractions.Push;
using ChatApp.TcpGateway.Infrastructure.Push;
using Microsoft.Extensions.Logging;

namespace ChatApp.PushWorker.Providers.Fcm;

/// <summary>
/// Firebase Cloud Messaging (HTTP v1 API) 推送 Provider。
/// <para>
/// 使用 Service Account JWT 获取 OAuth2 access token（RS256 签名，1h 有效，5min 提前刷新）。
/// HTTP/2 长连接复用，发送 <c>POST /v1/projects/{projectId}/messages:send</c>。
/// </para>
/// </summary>
internal sealed partial class FcmPushProvider : IPushProvider, IDisposable
{
    private readonly FcmOptions _options;
    private readonly HttpClient _httpClient;
    private readonly ILogger<FcmPushProvider> _logger;

    private readonly SemaphoreSlim _tokenLock = new(1, 1);
    private FcmServiceAccount? _serviceAccount;
    private string? _cachedAccessToken;
    private DateTimeOffset _tokenExpiresAt;

    public PushPlatform Platform => PushPlatform.Fcm;

    public FcmPushProvider(
        FcmOptions options,
        HttpClient httpClient,
        ILogger<FcmPushProvider> logger)
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
        var accessToken = await GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);

        var message = FcmPayloadBuilder.BuildMessage(token, title, body, collapseKey, customData);
        var requestUrl = $"/v1/projects/{_options.ProjectId}/messages:send";

        using var request = new HttpRequestMessage(HttpMethod.Post, requestUrl);
        request.Version = System.Net.HttpVersion.Version20;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        request.Content = JsonContent.Create(message, FcmPayloadContext.Default.FcmSendRequest);

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

        // 400 → 可能是 invalid_token（令牌失效/格式错误）
        if (statusCode == 400)
        {
            var body = await response.Content
                .ReadAsStringAsync(cancellationToken)
                .ConfigureAwait(false);
            // FCM 返回 INVALID_ARGUMENT；若 message 含 UNREGISTERED / registration-token → 令牌无效
            var isInvalidToken = body.Contains("UNREGISTERED", StringComparison.OrdinalIgnoreCase)
                || body.Contains("registration-token", StringComparison.OrdinalIgnoreCase)
                || body.Contains("invalid registration", StringComparison.OrdinalIgnoreCase);
            return PushProviderResult.Fail(isInvalidToken ? "invalid_token" : "payload_too_large");
        }

        // 401/403 → 认证失败（可能是 access token 过期，暂记 provider_unavailable 触发重试）
        if (statusCode is 401 or 403)
        {
            // 清除缓存的 token 以便下次重新获取
            _cachedAccessToken = null;
            return PushProviderResult.Fail("provider_unavailable");
        }

        return PushProviderResult.Fail("unknown");
    }

    /// <summary>
    /// 获取 OAuth2 access token（带缓存）。Service Account JWT RS256 签名，
    /// 换取 Google OAuth2 token（1h 有效，提前 5min 刷新）。
    /// </summary>
    private async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken)
    {
        // 快速路径：缓存未过期
        if (_cachedAccessToken is not null && DateTimeOffset.UtcNow < _tokenExpiresAt)
            return _cachedAccessToken;

        await _tokenLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Double-check
            if (_cachedAccessToken is not null && DateTimeOffset.UtcNow < _tokenExpiresAt)
                return _cachedAccessToken;

            _serviceAccount ??= FcmServiceAccount.Load(_options);
            var jwt = BuildServiceAccountJwt(_serviceAccount);
            var token = await RequestOAuthTokenAsync(jwt, cancellationToken).ConfigureAwait(false);

            _cachedAccessToken = token.AccessToken;
            // 提前 5 分钟过期，避免边界竞争
            _tokenExpiresAt = DateTimeOffset.UtcNow.AddSeconds(token.ExpiresIn - 300);
            return _cachedAccessToken;
        }
        finally
        {
            _tokenLock.Release();
        }
    }

    /// <summary>
    /// 构建 Service Account JWT（RS256 签名），用于 OAuth2 token 交换。
    /// </summary>
    private static string BuildServiceAccountJwt(FcmServiceAccount sa)
    {
        var now = DateTimeOffset.UtcNow;
        var header = new { typ = "JWT", alg = "RS256" };
        var payload = new
        {
            iss = sa.ClientEmail,
            scope = "https://www.googleapis.com/auth/firebase.messaging",
            aud = "https://oauth2.googleapis.com/token",
            iat = now.ToUnixTimeSeconds(),
            exp = now.AddHours(1).ToUnixTimeSeconds()
        };

        var headerJson = JsonSerializer.Serialize(header);
        var payloadJson = JsonSerializer.Serialize(payload);
        var headerB64 = Base64UrlEncode(headerJson);
        var payloadB64 = Base64UrlEncode(payloadJson);
        var signingInput = $"{headerB64}.{payloadB64}";

        using var rsa = RSA.Create();
        rsa.ImportFromPem(sa.PrivateKey);
        var signature = rsa.SignData(
            System.Text.Encoding.UTF8.GetBytes(signingInput),
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);

        return $"{signingInput}.{Base64UrlEncode(signature)}";
    }

    private static string Base64UrlEncode(string text)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(text);
        return Base64UrlEncode(bytes);
    }

    private static string Base64UrlEncode(byte[] data)
    {
        return Convert.ToBase64String(data)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    private async Task<FcmOAuthToken> RequestOAuthTokenAsync(
        string jwt,
        CancellationToken cancellationToken)
    {
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:jwt-bearer",
            ["assertion"] = jwt
        });

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            "https://oauth2.googleapis.com/token")
        {
            Version = System.Net.HttpVersion.Version20,
            Content = content
        };

        using var response = await _httpClient
            .SendAsync(request, cancellationToken)
            .ConfigureAwait(false);

        response.EnsureSuccessStatusCode();

        var token = await response.Content
            .ReadFromJsonAsync(FcmPayloadContext.Default.FcmOAuthToken, cancellationToken)
            .ConfigureAwait(false);
        return token ?? throw new InvalidOperationException("FCM OAuth2 token response was null.");
    }

    public void Dispose() => _tokenLock.Dispose();

    [LoggerMessage(LogLevel.Warning, "FCM send failed for token (len={TokenLen}).")]
    static partial void LogSendFailed(ILogger logger, Exception ex, int tokenLen);

    [LoggerMessage(LogLevel.Warning, "FCM send timed out.")]
    static partial void LogSendTimeout(ILogger logger, Exception ex);
}
