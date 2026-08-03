using System.Security.Cryptography;

namespace ChatApp.PushWorker.Providers.WebPush;

/// <summary>
/// RFC 8291 WebPush payload 加密器（AES128GCM + ECDH P-256 + HKDF-SHA256）。
/// <para>
/// 加密流程：
/// <list type="number">
/// <item>生成临时 ECDH P-256 密钥对（as_private / as_public）。</item>
/// <item>ECDH 计算共享密钥：shared_secret = ECDH(as_private, ua_public)。</item>
/// <item>IKM = auth_secret || shared_secret；PRK = HKDF-Extract(salt, IKM)。</item>
/// <item>key_info = "WebPush: info\0" || ua_public || as_public。</item>
/// <item>CEK = HKDF-Expand(PRK, "Content-Encoding: aes128gcm\0" || key_info, 16)。</item>
/// <item>NONCE = HKDF-Expand(PRK, "Content-Encoding: nonce\0" || key_info, 12)。</item>
/// <item>明文 = data || 0x01（单记录 delimiter）。</item>
/// <item>AES128GCM 加密，输出 ciphertext || tag。</item>
/// <item>结果 = salt(16) || rs(4,BE) || idlen(1) || as_public(65) || ciphertext_with_tag。</item>
/// </list>
/// </para>
/// </summary>
internal static class WebPushEncryptor
{
    private const int RecordSize = 4096;
    private const int SaltSize = 16;
    private const int KeyIdSize = 65; // P-256 uncompressed: 0x04 || X(32) || Y(32)
    private const int TagSize = 16;
    private const int NonceSize = 12;
    private const int CekSize = 16;

    /// <summary>
    /// 加密 payload，返回 RFC 8291 aes128gcm 格式的字节序列。
    /// </summary>
    public static byte[] Encrypt(byte[] plaintext, WebPushSubscription subscription)
    {
        var uaPublicKey = subscription.DecodeP256dh();
        var authSecret = subscription.DecodeAuth();

        // 1. 生成临时 ECDH P-256 密钥对
        using var ecdh = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
        var asParams = ecdh.ExportParameters(false);
        var asPublicKeyUncompressed = EncodeUncompressedPoint(asParams.Q);

        // 2. ECDH 共享密钥
        var uaParams = DecodeUncompressedPoint(uaPublicKey);
        using var uaEcdh = ECDiffieHellman.Create(uaParams);
        var sharedSecret = ecdh.DeriveKeyMaterial(uaEcdh.PublicKey);

        // 3. 生成随机 salt
        var salt = RandomNumberGenerator.GetBytes(SaltSize);

        // 4. IKM = auth_secret || shared_secret
        var ikm = new byte[authSecret.Length + sharedSecret.Length];
        authSecret.CopyTo(ikm, 0);
        sharedSecret.CopyTo(ikm, authSecret.Length);

        // 5. PRK = HKDF-Extract(salt, IKM)
        var prk = HKDF.Extract(HashAlgorithmName.SHA256, ikm, salt);

        // 6. key_info = "WebPush: info\0" || ua_public || as_public
        var keyInfo = BuildKeyInfo(uaPublicKey, asPublicKeyUncompressed);

        // 7. CEK = HKDF-Expand(PRK, cek_info, 16)
        var cekInfo = BuildInfoString("Content-Encoding: aes128gcm", keyInfo);
        var cek = HKDF.Expand(HashAlgorithmName.SHA256, prk, CekSize, cekInfo);

        // 8. NONCE = HKDF-Expand(PRK, nonce_info, 12)
        var nonceInfo = BuildInfoString("Content-Encoding: nonce", keyInfo);
        var nonce = HKDF.Expand(HashAlgorithmName.SHA256, prk, NonceSize, nonceInfo);

        // 9. 明文 padding：data || 0x01（单记录 delimiter）
        var padded = new byte[plaintext.Length + 1];
        plaintext.CopyTo(padded, 0);
        padded[^1] = 0x01;

        // 10. AES128GCM 加密
        var ciphertext = new byte[padded.Length];
        var tag = new byte[TagSize];
        using var aes = new AesGcm(cek, TagSize);
        aes.Encrypt(nonce, padded, ciphertext, tag);

        // 11. 拼装结果：salt(16) || rs(4) || idlen(1) || as_public(65) || ciphertext || tag
        var headerSize = SaltSize + 4 + 1 + KeyIdSize;
        var result = new byte[headerSize + ciphertext.Length + TagSize];

        Buffer.BlockCopy(salt, 0, result, 0, SaltSize);
        // rs = RecordSize (4 bytes big-endian)
        WriteUInt32BE(result, SaltSize, RecordSize);
        result[SaltSize + 4] = KeyIdSize; // idlen
        Buffer.BlockCopy(asPublicKeyUncompressed, 0, result, SaltSize + 5, KeyIdSize);
        Buffer.BlockCopy(ciphertext, 0, result, headerSize, ciphertext.Length);
        Buffer.BlockCopy(tag, 0, result, headerSize + ciphertext.Length, TagSize);

        return result;
    }

    /// <summary>
    /// key_info = "WebPush: info" || 0x00 || ua_public || as_public
    /// </summary>
    private static byte[] BuildKeyInfo(byte[] uaPublicKey, byte[] asPublicKey)
    {
        const string prefix = "WebPush: info";
        var prefixBytes = System.Text.Encoding.UTF8.GetBytes(prefix);
        var result = new byte[prefixBytes.Length + 1 + uaPublicKey.Length + asPublicKey.Length];
        var offset = 0;
        Buffer.BlockCopy(prefixBytes, 0, result, offset, prefixBytes.Length);
        offset += prefixBytes.Length;
        result[offset++] = 0x00; // null terminator
        Buffer.BlockCopy(uaPublicKey, 0, result, offset, uaPublicKey.Length);
        offset += uaPublicKey.Length;
        Buffer.BlockCopy(asPublicKey, 0, result, offset, asPublicKey.Length);
        return result;
    }

    /// <summary>
    /// info = "{infoString}" || 0x00 || key_info
    /// </summary>
    private static byte[] BuildInfoString(string infoString, byte[] keyInfo)
    {
        var infoBytes = System.Text.Encoding.UTF8.GetBytes(infoString);
        var result = new byte[infoBytes.Length + 1 + keyInfo.Length];
        var offset = 0;
        Buffer.BlockCopy(infoBytes, 0, result, offset, infoBytes.Length);
        offset += infoBytes.Length;
        result[offset++] = 0x00; // null terminator
        Buffer.BlockCopy(keyInfo, 0, result, offset, keyInfo.Length);
        return result;
    }

    /// <summary>ECPoint → 未压缩格式 (0x04 || X || Y)，65 字节。</summary>
    private static byte[] EncodeUncompressedPoint(ECPoint point)
    {
        var result = new byte[KeyIdSize];
        result[0] = 0x04;
        point.X!.CopyTo(result, 1);
        point.Y!.CopyTo(result, 33);
        return result;
    }

    /// <summary>未压缩格式 (0x04 || X || Y) → ECParameters。</summary>
    private static ECParameters DecodeUncompressedPoint(byte[] uncompressed)
    {
        if (uncompressed.Length != KeyIdSize || uncompressed[0] != 0x04)
            throw new CryptographicException(
                $"无效的 P-256 公钥格式：期望 {KeyIdSize} 字节未压缩格式 (0x04||X||Y)。");

        return new ECParameters
        {
            Curve = ECCurve.NamedCurves.nistP256,
            Q = new ECPoint
            {
                X = uncompressed[1..33],
                Y = uncompressed[33..65]
            }
        };
    }

    private static void WriteUInt32BE(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte)(value >> 24);
        buffer[offset + 1] = (byte)(value >> 16);
        buffer[offset + 2] = (byte)(value >> 8);
        buffer[offset + 3] = (byte)value;
    }
}
