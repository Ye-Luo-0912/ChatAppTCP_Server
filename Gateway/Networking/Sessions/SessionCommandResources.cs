using System.Buffers;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// SessionCommand 的单一资源所有权出口。所有执行、拒绝、替换和停机路径都调用这里，
/// 避免 ArrayPool 缓冲区或 GlobalInboundBudget 租约泄漏。
/// </summary>
internal static class SessionCommandResources
{
    public static void Release(in SessionCommand command)
    {
        if (command.IsPooled && command.RentedBuffer.Length > 0)
            ArrayPool<byte>.Shared.Return(command.RentedBuffer);

        if (command.ReservedInboundBytes > 0 &&
            command.InboundBudget is not null)
        {
            command.InboundBudget.Release(command.ReservedInboundBytes);
        }
    }
}
