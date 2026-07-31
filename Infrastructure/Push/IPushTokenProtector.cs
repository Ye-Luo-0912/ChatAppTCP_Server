namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 主线一10：Push Token 加密保护器。
/// <para>
/// Redis 中存储的 Push Token 明文需加密或最小权限保护。
/// 本接口抽象加/解密逻辑，使 <see cref="RedisPushTokenStore"/> 与具体加密实现解耦。
/// </para>
/// <para>
/// 实现选择：
/// <list type="bullet">
/// <item><see cref="AesGcmPushTokenProtector"/>：AES-GCM 256 位加密，需配置密钥。</item>
/// <item><see cref="NullPushTokenProtector"/>：不加密（向后兼容，明文存储）。</item>
/// </list>
/// </para>
/// </summary>
public interface IPushTokenProtector
{
    /// <summary>
    /// 加密明文数据（如 PushTokenRecord JSON）。
    /// 返回 Base64 编码的密文（含 nonce + ciphertext + tag）。
    /// </summary>
    string Protect(string plaintext);

    /// <summary>
    /// 解密 <see cref="Protect"/> 返回的密文，还原明文。
    /// </summary>
    string Unprotect(string protectedData);
}
