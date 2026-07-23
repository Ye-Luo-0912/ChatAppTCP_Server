using System.Security.Cryptography;
using System.Text;

namespace ChatApp.TcpGateway.Infrastructure.Caching;

internal static class AccessTokenCacheKey
{
    private const string Prefix = "cache:AT:";

    public static string Create(string token)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(token);

        var byteCount = Encoding.UTF8.GetByteCount(token);
        byte[]? rented = null;
        Span<byte> tokenBytes = byteCount <= 256
            ? stackalloc byte[byteCount]
            : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount));

        try
        {
            Encoding.UTF8.GetBytes(token, tokenBytes);
            Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(tokenBytes[..byteCount], hash);
            return string.Concat(Prefix, Convert.ToHexString(hash));
        }
        finally
        {
            if (rented is not null)
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rented, clearArray: true);
            }
        }
    }
}
