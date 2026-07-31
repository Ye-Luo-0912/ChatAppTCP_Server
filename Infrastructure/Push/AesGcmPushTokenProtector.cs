using System.Security.Cryptography;
using System.Text;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 主线一10：AES-GCM 256 位加密的 Push Token 保护器。
/// <para>
/// 加密格式：Base64( nonce[12] || ciphertext[N] || tag[16] )。
/// 每次加密生成随机 nonce，确保相同明文加密后密文不同。
/// </para>
/// <para>
/// 密钥来源：<see cref="PushOptions.TokenEncryptionKey"/>（Base64 编码的 32 字节密钥）。
/// 未配置密钥时应使用 <see cref="NullPushTokenProtector"/>（明文存储）。
/// </para>
/// <para>
/// AES-GCM 提供机密性 + 完整性：篡改密文会导致 <see cref="CryptographicException"/>，
/// 防止攻击者修改 Redis 中的 token 数据。
/// </para>
/// </summary>
internal sealed class AesGcmPushTokenProtector : IPushTokenProtector
{
    private const int NonceSize = 12;  // AES-GCM 推荐 96-bit nonce
    private const int TagSize = 16;    // AES-GCM 推荐 128-bit tag
    private const int KeySize = 32;    // AES-256

    private readonly byte[] _key;

    public AesGcmPushTokenProtector(byte[] key)
    {
        if (key.Length != KeySize)
            throw new ArgumentException(
                $"AES-256 key must be {KeySize} bytes, got {key.Length}.",
                nameof(key));
        _key = key;
    }

    /// <summary>
    /// 从 Base64 编码的密钥字符串构造保护器。
    /// </summary>
    public AesGcmPushTokenProtector(string base64Key)
    {
        _key = Convert.FromBase64String(base64Key);
        if (_key.Length != KeySize)
            throw new ArgumentException(
                $"AES-256 key must be {KeySize} bytes after Base64 decode, got {_key.Length}.",
                nameof(base64Key));
    }

    public string Protect(string plaintext)
    {
        var plaintextBytes = Encoding.UTF8.GetBytes(plaintext);
        var nonce = RandomNumberGenerator.GetBytes(NonceSize);
        var ciphertext = new byte[plaintextBytes.Length];
        var tag = new byte[TagSize];

        using var aes = new AesGcm(_key, TagSize);
        aes.Encrypt(nonce, plaintextBytes, ciphertext, tag);

        // 拼接 nonce + ciphertext + tag，Base64 编码。
        var result = new byte[NonceSize + ciphertext.Length + TagSize];
        Buffer.BlockCopy(nonce, 0, result, 0, NonceSize);
        Buffer.BlockCopy(ciphertext, 0, result, NonceSize, ciphertext.Length);
        Buffer.BlockCopy(tag, 0, result, NonceSize + ciphertext.Length, TagSize);

        return Convert.ToBase64String(result);
    }

    public string Unprotect(string protectedData)
    {
        var rawData = Convert.FromBase64String(protectedData);
        if (rawData.Length < NonceSize + TagSize)
            throw new CryptographicException(
                $"Protected data too short: {rawData.Length} bytes " +
                $"(minimum {NonceSize + TagSize}).");

        var nonce = new byte[NonceSize];
        Buffer.BlockCopy(rawData, 0, nonce, 0, NonceSize);

        var tag = new byte[TagSize];
        Buffer.BlockCopy(rawData, rawData.Length - TagSize, tag, 0, TagSize);

        var ciphertext = new byte[rawData.Length - NonceSize - TagSize];
        Buffer.BlockCopy(rawData, NonceSize, ciphertext, 0, ciphertext.Length);

        var plaintext = new byte[ciphertext.Length];

        using var aes = new AesGcm(_key, TagSize);
        aes.Decrypt(nonce, ciphertext, tag, plaintext);

        return Encoding.UTF8.GetString(plaintext);
    }
}

/// <summary>
/// 空保护器：不加密，直接返回明文。用于未配置加密密钥时的向后兼容。
/// <para>
/// 生产环境应配置 <see cref="PushOptions.TokenEncryptionKey"/> 启用加密。
/// </para>
/// </summary>
internal sealed class NullPushTokenProtector : IPushTokenProtector
{
    public string Protect(string plaintext) => plaintext;
    public string Unprotect(string protectedData) => protectedData;
}
