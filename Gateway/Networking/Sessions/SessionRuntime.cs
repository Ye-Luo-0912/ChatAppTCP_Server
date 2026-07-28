using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using ChatApp.TcpGateway.Core.Protocol;
using ChatApp.TcpGateway.Gateway.Configuration;
using ChatApp.TcpGateway.Gateway.Networking.Buffers;
using ChatApp.TcpGateway.Gateway.Networking.Executor;
using ChatApp.TcpGateway.Gateway.Networking.Ephemeral;
using ChatApp.TcpGateway.Observability.Logging;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging;

namespace ChatApp.TcpGateway.Gateway.Networking.Sessions;

/// <summary>
/// 每连接数据面运行时：封装 DirectSocket/Pipelines 入站读取 +
/// 全局 <see cref="SessionCommandExecutor"/>（OrderedWrite/Query/Ephemeral）调度，
/// 从 <see cref="Networking.TcpGatewayService"/> 抽取以消除 God Service 中散落的 per-connection 数据路径。
/// <para>
/// 职责边界：
/// <list type="bullet">
/// <item>构造每会话 CTS（链接 session lifetime + host stopping）；</item>
/// <item>注册连接到全局 <see cref="SessionCommandExecutor"/>（OrderedWrite/Query/Ephemeral 各一份）；</item>
/// <item>按配置驱动 DirectSocket 增量解析，或构造 Pipe 并运行 fill/read 双任务；</item>
/// <item>Inline 命令在读循环内同步处理（仅 Heartbeat 等无 I/O 的轻量命令）；</item>
/// <item>Ephemeral/Query/OrderedWrite 转发到全局执行器异步处理；</item>
/// <item>数据面清理：入站预算归还、session.Close、executor 注销、session.DisposeAsync。</item>
/// </list>
/// 协议级内联命令处理与早投拒绝（SendProtocolError / RejectOversizedPayload / ProcessPacketAsync）
/// 通过构造函数注入的委托回调到 <see cref="Networking.TcpGatewayService"/>，
/// 避免将大量协议 codec/依赖注入本类型。
/// 服务级清理（Presence 下线、admission 释放、session 注册表移除）由调用方在 RunAsync 返回后处理。
/// </para>
/// <para>
/// 单例注册：所有连接共享同一实例，通过 <see cref="RunAsync"/> 的 session 参数区分连接。
/// </para>
/// <para>
/// V2 重构：消除每连接 OrderedWrite/Query/Ephemeral 三个 Channel + 三个 Consumer Task。
/// OrderedWrite/Query/Ephemeral 转发到全局 <see cref="SessionCommandExecutor"/>（共享 worker 池）。
/// Ephemeral 命令（TypingNotify）异步处理，避免 Typing 授权器缓存 Miss 时的远程 I/O 阻塞 TCP Read Loop。
/// Inline lane 仅保留无外部 I/O 的轻量命令（Heartbeat/握手/认证状态变更）。
/// </para>
/// </summary>
internal sealed partial class SessionRuntime
{
    private readonly TcpGatewayOptions _options;
    private readonly PipeOptions _pipeOptions;
    private readonly GlobalInboundBudget _globalInboundBudget;
    private readonly SessionCommandExecutor _orderedWriteExecutor;
    private readonly SessionCommandExecutor _queryExecutor;
    private readonly EphemeralCommandPipeline _ephemeralPipeline;
    private readonly TypingActorPipeline? _typingActorPipeline;
    private readonly GatewayMetrics _metrics;
    private readonly ILogger _logger;

    private readonly Func<PacketFrame, TcpClientSession, string, CancellationToken, ValueTask> _processPacketAsync;
    private readonly Action<TcpClientSession, ProtocolErrorCode, string?, bool, int?, ushort?> _sendProtocolError;
    private readonly Action<TcpClientSession, PacketCommand> _rejectOversizedPayload;

    public SessionRuntime(
        TcpGatewayOptions options,
        PipeOptions pipeOptions,
        GlobalInboundBudget globalInboundBudget,
        SessionCommandExecutor orderedWriteExecutor,
        SessionCommandExecutor queryExecutor,
        EphemeralCommandPipeline ephemeralPipeline,
        TypingActorPipeline? typingActorPipeline,
        GatewayMetrics metrics,
        ILogger logger,
        Func<PacketFrame, TcpClientSession, string, CancellationToken, ValueTask> processPacketAsync,
        Action<TcpClientSession, ProtocolErrorCode, string?, bool, int?, ushort?> sendProtocolError,
        Action<TcpClientSession, PacketCommand> rejectOversizedPayload)
    {
        _options = options;
        _pipeOptions = pipeOptions;
        _globalInboundBudget = globalInboundBudget;
        _orderedWriteExecutor = orderedWriteExecutor;
        _queryExecutor = queryExecutor;
        _ephemeralPipeline = ephemeralPipeline;
        _typingActorPipeline = typingActorPipeline;
        _metrics = metrics;
        _logger = logger;
        _processPacketAsync = processPacketAsync;
        _sendProtocolError = sendProtocolError;
        _rejectOversizedPayload = rejectOversizedPayload;
    }

    /// <summary>
    /// 驱动单连接数据面：注册到全局 executor，按配置选择 DirectSocket 或 Pipelines。
    /// 返回时已完成数据面清理（入站预算/session）；服务级清理由调用方处理。
    /// </summary>
    public async Task RunAsync(
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        // 链接 Session lifetime token 与宿主 stopping token。
        // 连接关闭时取消所有业务调用，避免后端资源继续被占用。
        using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(
            session.LifetimeToken, cancellationToken);
        var sessionToken = sessionCts.Token;

        // 连接注册到全局执行器由 TcpGatewayService.OnConnectionAccepted 完成
        // （在 session 暴露到 _sessions 字典前注册，避免命令到达时未注册）。
        // 注销由 HandleClientAsync finally 块在 SessionRuntime 返回后统一处理。

        try
        {
            if (_options.InboundTransportMode ==
                InboundTransportMode.DirectSocket)
            {
                await RunDirectSocketAsync(
                        session,
                        remoteIp,
                        sessionToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await RunPipelinesAsync(
                        session,
                        remoteIp,
                        sessionToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested ||
                  !session.IsConnected)
        {
            // Expected shutdown path.
        }
        catch (SocketException)
        {
            session.Close(SessionCloseReason.TransportError);
        }
        catch (ObjectDisposedException)
        {
            session.Close(SessionCloseReason.TransportError);
        }
        catch (Exception exception)
        {
            _logger.TransportFailed(
                GatewayTransportOperation.ClientProcessing,
                session.ConnectionId,
                exception);
            session.Close(SessionCloseReason.TransportError);
        }
        finally
        {
            session.Close(
                cancellationToken.IsCancellationRequested
                    ? SessionCloseReason.ApplicationStopping
                    : SessionCloseReason.RemoteClosed);

            // 执行器注销由 TcpGatewayService.HandleClientAsync finally 统一处理，
            // 避免此处与 service 级清理的顺序耦合。

            await session.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task RunPipelinesAsync(
        TcpClientSession session,
        string remoteIp,
        CancellationToken cancellationToken)
    {
        var pipe = new Pipe(_pipeOptions);
        var pipeLease =
            new SessionInboundPipeLease(_globalInboundBudget);
        var fillTask = FillPipeAsync(
            session,
            pipe.Writer,
            pipeLease,
            cancellationToken);
        var readTask = ReadPipeAsync(
            pipe.Reader,
            session,
            remoteIp,
            pipeLease,
            cancellationToken);

        try
        {
            await Task.WhenAll(fillTask, readTask)
                .ConfigureAwait(false);
        }
        finally
        {
            pipeLease.ReleaseAll();
        }
    }

    private async Task FillPipeAsync(
        TcpClientSession session,
        PipeWriter writer,
        SessionInboundPipeLease pipeLease,
        CancellationToken cancellationToken)
    {
        Exception? completionError = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   session.IsConnected)
            {
                var memory = writer.GetMemory(
                    _options.ReceiveBufferSize);
                var bytesRead = await session
                    .ReceiveAsync(memory, cancellationToken)
                    .ConfigureAwait(false);

                if (bytesRead == 0)
                {
                    session.Close(SessionCloseReason.RemoteClosed);
                    break;
                }

                if (!pipeLease.TryReserve(bytesRead))
                {
                    session.Close(SessionCloseReason.InboundBudgetExceeded);
                    break;
                }

                writer.Advance(bytesRead);
                var result = await writer
                    .FlushAsync(cancellationToken)
                    .ConfigureAwait(false);

                if (result.IsCanceled || result.IsCompleted)
                {
                    break;
                }
            }
        }
        catch (Exception exception)
        {
            completionError = exception;
            throw;
        }
        finally
        {
            await writer.CompleteAsync(completionError)
                .ConfigureAwait(false);
        }
    }

    private async Task ReadPipeAsync(
        PipeReader reader,
        TcpClientSession session,
        string remoteIp,
        SessionInboundPipeLease pipeLease,
        CancellationToken cancellationToken)
    {
        Exception? completionError = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   session.IsConnected)
            {
                var result = await reader
                    .ReadAsync(cancellationToken)
                    .ConfigureAwait(false);
                var readBuffer = result.Buffer;
                var buffer = readBuffer;

                // 跟踪已消费位置。Inline 命令处理后更新此位置；
                // 入队命令在复制 payload 后也更新此位置，使 Pipe 可立即回收内存。
                var consumed = buffer.Start;

                while (session.IsConnected)
                {
                    if (!session.IsAuthenticated &&
                        PacketParser.TryPeekCommand(
                            buffer,
                            out var peekedCommand) &&
                        !ValidatePreAuthenticationCommand(
                            session,
                            peekedCommand))
                    {
                        return;
                    }

                    var parseStatus = PacketParser.TryParse(
                        ref buffer,
                        out var frame);

                    if (parseStatus == PacketParseStatus.NeedMoreData)
                    {
                        break;
                    }

                    if (parseStatus == PacketParseStatus.InvalidPacket)
                    {
                        RejectInvalidPacket(session);
                        return;
                    }

                    if (!await DispatchFrameAsync(
                            frame,
                            session,
                            remoteIp,
                            cancellationToken)
                        .ConfigureAwait(false))
                    {
                        return;
                    }

                    // 标记此帧已消费（Pipe 可回收对应内存）。
                    consumed = buffer.Start;
                }

                var consumedBytes = (int)readBuffer
                    .Slice(readBuffer.Start, consumed)
                    .Length;
                pipeLease.Release(consumedBytes);
                reader.AdvanceTo(consumed, buffer.End);

                if (result.IsCanceled)
                {
                    break;
                }

                if (result.IsCompleted)
                {
                    if (!buffer.IsEmpty)
                    {
                        _metrics.ProtocolError();
                    }

                    break;
                }
            }
        }
        catch (Exception exception)
        {
            completionError = exception;
            throw;
        }
        finally
        {
            await reader.CompleteAsync(completionError)
                .ConfigureAwait(false);
        }
    }
}
