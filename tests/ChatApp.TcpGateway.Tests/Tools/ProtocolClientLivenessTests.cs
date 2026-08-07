using System.Net;
using System.Net.Sockets;
using ChatApp.TcpGateway.LoadGenerator;

namespace ChatApp.TcpGateway.Tests.Tools;

public sealed class ProtocolClientLivenessTests
{
    [Fact]
    public async Task ConnectionObserverReturnsWhenThePeerCloses()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = (IPEndPoint)listener.LocalEndpoint;
        await using var client = new ProtocolClient();
        var accept = listener.AcceptTcpClientAsync(
            TestContext.Current.CancellationToken);
        await client.ConnectAsync(
            IPAddress.Loopback.ToString(),
            endpoint.Port,
            constrainReceiveBuffer: false,
            TestContext.Current.CancellationToken);
        using var peer = await accept;

        peer.Dispose();

        await client.WaitForRemoteCloseAsync(
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ConnectionObserverRejectsUnexpectedProtocolData()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var endpoint = (IPEndPoint)listener.LocalEndpoint;
        await using var client = new ProtocolClient();
        var accept = listener.AcceptTcpClientAsync(
            TestContext.Current.CancellationToken);
        await client.ConnectAsync(
            IPAddress.Loopback.ToString(),
            endpoint.Port,
            constrainReceiveBuffer: false,
            TestContext.Current.CancellationToken);
        using var peer = await accept;
        await peer.GetStream().WriteAsync(
            new byte[] { 0x42 },
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await client.WaitForRemoteCloseAsync(
                TestContext.Current.CancellationToken));
    }
}
