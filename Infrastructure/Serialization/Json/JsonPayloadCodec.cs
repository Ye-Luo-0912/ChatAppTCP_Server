using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using ChatApp.TcpGateway.Core.Serialization;

namespace ChatApp.TcpGateway.Infrastructure.Serialization.Json;

public sealed class JsonPayloadCodec<T>(JsonTypeInfo<T> typeInfo)
    : IPayloadCodec<T>
{
    private readonly JsonTypeInfo<T> _typeInfo = typeInfo;

    public PayloadFormat Format => PayloadFormat.Json;

    public void Serialize(IBufferWriter<byte> destination, T value)
    {
        using var writer = new Utf8JsonWriter(destination);
        JsonSerializer.Serialize(writer, value, _typeInfo);
    }

    public T? Deserialize(ReadOnlySequence<byte> payload)
    {
        if (payload.IsEmpty)
        {
            return default;
        }

        var reader = new Utf8JsonReader(
            payload,
            isFinalBlock: true,
            state: default);

        return JsonSerializer.Deserialize(ref reader, _typeInfo);
    }
}
