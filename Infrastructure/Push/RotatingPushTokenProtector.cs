using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace ChatApp.TcpGateway.Infrastructure.Push;

/// <summary>
/// 密钥环信息接口（门禁3）。
/// <para>
/// 供 <see cref="RedisPushTokenStore"/> 的渐进式重加密任务判断某条加密数据是否已使用当前密钥。
/// </para>
/// </summary>
public interface ITokenKeyRing
{
    /// <summary>当前用于写入的密钥 Id。</summary>
    uint CurrentKeyId { get; }

    /// <summary>
    /// 判断给定密文是否已使用当前密钥加密。
    /// 对旧 Key 加密的数据或旧明文数据返回 true（需重加密）。
    /// </summary>
    bool NeedsReencryption(string protectedData);
}

/// <summary>
/// 门禁3：支持密钥轮换的 AES-GCM 256 位 Push Token 保护器。
/// <para>
/// 加密格式（Base64）：<c>cipher_version(1B) || key_id(4B BE) || nonce(12B) || ciphertext || tag(16B)</c>。
/// </para>
/// <para>
/// 写入使用当前 Key（<see cref="CurrentKeyId"/>）；读取按 key_id 从密钥环查找对应 Key，
/// 支持用旧 Key 解密历史数据。配合 <see cref="PushTokenReencryptionWorker"/> 后台渐进重加密，
/// 避免密钥轮换时一次性迁移或丢失旧 Key 数据。
/// </para>
/// </summary>
internal sealed class RotatingPushTokenProtector : IPushTokenProtector, ITokenKeyRing
{
    private const byte CipherVersion = 1;
    private const int NonceSize = 12;
    private const int TagSize = 16;
    private const int KeySize = 32;
    private const int HeaderSize = 1 + 4; // cipher_version + key_id

    private readonly IReadOnlyDictionary<uint, byte[]> _keyRing;
    private readonly uint _currentKeyId;

    public RotatingPushTokenProtector(
        IReadOnlyDictionary<uint, byte[]> keyRing,
        uint currentKeyId)
    {
        if (keyRing is null || keyRing.Count == 0)
            throw new ArgumentException("Key ring must not be empty.", nameof(keyRing));
        if (!keyRing.ContainsKey(currentKeyId))
            throw new ArgumentException(
                $"Current key id {currentKeyId} not present in key ring.", nameof(currentKeyId));

        foreach (var kv in keyRing)
        {
            if (kv.Value.Length != KeySize)
                throw new ArgumentException(
                    $"AES-256 key for id {kv.Key} must be {KeySize} bytes, got {kv.Value.Length}.",
                    nameof(keyRing));
        }

        _keyRing = keyRing;
        _currentKeyId = currentKeyId;
    }

    public uint CurrentKeyId => _currentKeyId;

    public string Protect(string plaintext)
    {
        var plaintextBytes = Encoding.UTF8.GetBytes(plaintext);
        var nonce = RandomNumberGenerator.GetBytes(NonceSize);
        var ciphertext = new byte[plaintextBytes.Length];
        var tag = new byte[TagSize];

        using var aes = new AesGcm(_keyRing[_currentKeyId], TagSize);
        aes.Encrypt(nonce, plaintextBytes, ciphertext, tag);

        var result = new byte[HeaderSize + NonceSize + ciphertext.Length + TagSize];
        result[0] = CipherVersion;
        BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(1, 4), _currentKeyId);
        Buffer.BlockCopy(nonce, 0, result, HeaderSize, NonceSize);
        Buffer.BlockCopy(ciphertext, 0, result, HeaderSize + NonceSize, ciphertext.Length);
        Buffer.BlockCopy(tag, 0, result, HeaderSize + NonceSize + ciphertext.Length, TagSize);

        return Convert.ToBase64String(result);
    }

    public string Unprotect(string protectedData)
    {
        var rawData = Convert.FromBase64String(protectedData);
        if (rawData.Length < HeaderSize + NonceSize + TagSize)
            throw new CryptographicException(
                $"Protected data too short: {rawData.Length} bytes " +
                $"(minimum {HeaderSize + NonceSize + TagSize}).");

        // 旧版 AesGcmPushTokenProtector 格式：nonce(12) || ciphertext || tag(16)，无头部。
        // 首个字节为 nonce 首字节（随机），几乎不可能等于 CipherVersion。据此识别旧格式并回退。
        if (rawData[0] != CipherVersion)
            return TryLegacyDecrypt(rawData);

        var keyId = BinaryPrimitives.ReadUInt32BigEndian(rawData.AsSpan(1, 4));
        if (!_keyRing.TryGetValue(keyId, out var key))
            throw new CryptographicException($"Unknown key id: {keyId}. Key rotation omitted it.");

        var nonce = new byte[NonceSize];
        Buffer.BlockCopy(rawData, HeaderSize, nonce, 0, NonceSize);

        var tag = new byte[TagSize];
        Buffer.BlockCopy(rawData, rawData.Length - TagSize, tag, 0, TagSize);

        var ciphertext = new byte[rawData.Length - HeaderSize - NonceSize - TagSize];
        Buffer.BlockCopy(rawData, HeaderSize + NonceSize, ciphertext, 0, ciphertext.Length);

        var plaintext = new byte[ciphertext.Length];
        using var aes = new AesGcm(key, TagSize);
        aes.Decrypt(nonce, ciphertext, tag, plaintext);

        return Encoding.UTF8.GetString(plaintext);
    }

    /// <summary>
    /// 旧版格式（nonce || ciphertext || tag）解密回退：优先用当前 Key，失败则遍历密钥环。
    /// </summary>
    private string TryLegacyDecrypt(byte[] rawData)
    {
        var nonce = new byte[NonceSize];
        Buffer.BlockCopy(rawData, 0, nonce, 0, NonceSize);
        var tag = new byte[TagSize];
        Buffer.BlockCopy(rawData, rawData.Length - TagSize, tag, 0, TagSize);
        var ciphertext = new byte[rawData.Length - NonceSize - TagSize];
        Buffer.BlockCopy(rawData, NonceSize, ciphertext, 0, ciphertext.Length);

        var plaintext = new byte[ciphertext.Length];
        // 当前 Key 优先，随后遍历其余密钥。
        foreach (var keyId in LegacyKeyOrder())
        {
            try
            {
                using var aes = new AesGcm(_keyRing[keyId], TagSize);
                aes.Decrypt(nonce, ciphertext, tag, plaintext);
                return Encoding.UTF8.GetString(plaintext);
            }
            catch (CryptographicException)
            {
                // 尝试下一个 Key。
            }
        }

        throw new CryptographicException("Failed to decrypt legacy push token with any configured key.");
    }

    private IEnumerable<uint> LegacyKeyOrder()
    {
        yield return _currentKeyId;
        foreach (var keyId in _keyRing.Keys)
        {
            if (keyId != _currentKeyId)
                yield return keyId;
        }
    }

    public bool NeedsReencryption(string protectedData)
    {
        try
        {
            var rawData = Convert.FromBase64String(protectedData);
            if (rawData.Length < HeaderSize || rawData[0] != CipherVersion)
                return true;
            var keyId = BinaryPrimitives.ReadUInt32BigEndian(rawData.AsSpan(1, 4));
            return keyId != _currentKeyId;
        }
        catch (Exception)
        {
            // 旧明文数据或损坏数据：无法识别为当前密钥加密，需重加密。
            return true;
        }
    }
}