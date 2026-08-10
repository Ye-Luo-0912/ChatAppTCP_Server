using System.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;

namespace ChatApp.TcpGateway.Tests.Networking;

public sealed class EphemeralMailboxConditionalRemoveTests
{
    [Fact]
    public void TryRemove_OlderExpectedEntry_DoesNotRemoveReplacement()
    {
        var mailbox = new EphemeralMailbox();
        var key = EphemeralKey.Presence(userId: 42);
        var first = CreateFrame(32);
        var second = CreateFrame(48);

        try
        {
            var old = mailbox.TryStore(
                key,
                new EphemeralEntry(first, first.Length),
                out var firstRejected,
                out var firstStored);
            Assert.Null(old);
            Assert.False(firstRejected);

            old = mailbox.TryStore(
                key,
                new EphemeralEntry(second, second.Length),
                out var secondRejected,
                out var secondStored);
            Assert.Equal(firstStored, old);
            Assert.False(secondRejected);

            Assert.False(mailbox.TryRemove(key, firstStored, out _));
            Assert.True(mailbox.TryRemove(key, secondStored, out var removed));
            Assert.Equal(secondStored, removed);
            Assert.True(mailbox.IsEmpty);
        }
        finally
        {
            first.Dispose();
            second.Dispose();
        }
    }

    private static SharedOutboundFrame CreateFrame(int length) =>
        new(ArrayPool<byte>.Shared.Rent(length), length);
}
