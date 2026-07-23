using System.Buffers;

namespace ChatApp.TcpGateway.Core.Serialization;

public interface IPayloadCodec<T>
{
    PayloadFormat Format { get; }

    void Serialize(IBufferWriter<byte> destination, T value);

    T? Deserialize(ReadOnlySequence<byte> payload);
}
