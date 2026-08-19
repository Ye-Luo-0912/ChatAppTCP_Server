using System.Buffers;
using System.Net;
using System.Net.Sockets;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Infrastructure.Core.Calls;
using ChatApp.TcpGateway.Gateway.Commands.Calls;
using ChatApp.Shared.Protocol.Tcp;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// 跨仓联调：TCP Gateway 生产适配实现 <see cref="RealtimeCallBackend"/> 直驱 Realtime 真实
/// <see cref="DefaultCallControlProcessor"/> 的完整通话生命周期。
/// <para>
/// 与 <see cref="CallSignalingIntegrationTests"/>（脚本后端，验证 wire 映射）和 Realtime 侧
/// <c>CallControlLifecycleTests</c>（真实 NATS + 状态机）不同，本测试把两层在生产适配路径上
/// 打通：两端真实 TCP 连接 → <c>CallCommandHandler</c> → <c>RealtimeCallBackend</c>（生产映射）→
/// 真实状态机（内存仓储 + 默认 grant 校验 + 记录转发器）→ 经 TCP 回送权威响应并把对端信号
/// push 给被叫。验证 invite → ringing → accept → active → end 的终态收敛与 SDP 信令转发。
/// </para>
/// </summary>
public sealed class CallSignalingRealtimeIntegrationTests
{
    private const long CallerUserId = 42;
    private const long CalleeUserId = 43;

    [Fact(Timeout = 15_000)]
    public async Task FullLifecycle_OverRealProcessor_ConvergesAndForwardsSignals()
    {
        // 真实 Realtime 状态机：内存仓储 + 默认 grant 校验 + 记录转发器 + 审计。
        var forwarder = new RecordingCallSignalForwarder();
        var processor = new DefaultCallControlProcessor(
            new InMemoryCallStateStore(),
            new DefaultCallGrantVerifier(new CallPolicyOptions()),
            forwarder,
            new InMemoryCallAuditStore(),
            new CallPolicyOptions(),
            new CallMetrics(),
            TimeProvider.System,
            NullLogger<DefaultCallControlProcessor>.Instance);

        // 生产适配路径：RealtimeCallBackend 经 IRealtimeMessageBus 把命令送进真实处理器。
        var realBackend = new RealtimeCallBackend(new RealtimeTestCallBus(processor));

        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(realBackend);

        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);

        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();

        await harness.AuthenticateAsync(callerStream, "caller-token", CallerUserId);
        await harness.AuthenticateAsync(calleeStream, "callee-token", CalleeUserId);

        var callId = $"call-{Guid.NewGuid():N}";
        var grant = new TcpCallGrant
        {
            CallId = callId,
            CallerUserId = CallerUserId,
            CalleeUserId = CalleeUserId,
            ExpiresAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000,
            Nonce = Guid.NewGuid().ToString("N"),
            Signature = "fingerprint"
        };

        // ── 主叫 Invite（携带 offer SDP）→ 权威响应 Ringing + 对端 Invite 信令 ──
        var inviteId = Guid.CreateVersion7().ToString("N");
        await harness.WriteCallCommandAsync(
            callerStream,
            new TcpCallCommandRequest
            {
                RequestId = inviteId,
                CommandId = "cmd-invite",
                CallId = callId,
                Type = TcpCallCommandType.Invite,
                ActorUserId = CallerUserId,
                Revision = 1,
                Grant = grant,
                Sdp = OfferSdp,
                ClientOccurredAtMs = 1_900_000_000_000L
            });

        var inviteResponse = await ReadResponseAsync(harness, callerStream);
        Assert.True(inviteResponse.Succeeded, inviteResponse.ErrorMessage);
        Assert.Equal(TcpCallState.Ringing, inviteResponse.State);
        Assert.Equal(1L, inviteResponse.Revision);

        var inviteSignal = CallSignalingIntegrationTests.CallHarness.DeserializeSignal(
            (await harness.ReadFrameAsync(calleeStream)).Payload);
        Assert.NotNull(inviteSignal);
        Assert.Equal(TcpCallCommandType.Invite, inviteSignal.Kind);
        Assert.Equal(CalleeUserId, inviteSignal.ToUserId);
        Assert.Contains("o=caller", inviteSignal.Sdp);

        // ── 被叫 Accept（携带 answer SDP）→ 权威响应 Active + 对端 Accept 信令 ──
        var acceptId = Guid.CreateVersion7().ToString("N");
        await harness.WriteCallCommandAsync(
            calleeStream,
            new TcpCallCommandRequest
            {
                RequestId = acceptId,
                CommandId = "cmd-accept",
                CallId = callId,
                Type = TcpCallCommandType.Accept,
                ActorUserId = CalleeUserId,
                Revision = 2,
                Grant = grant,
                Sdp = AnswerSdp,
                ClientOccurredAtMs = 1_900_000_000_000L
            });

        var acceptResponse = await ReadResponseAsync(harness, calleeStream);
        Assert.True(acceptResponse.Succeeded, acceptResponse.ErrorMessage);
        Assert.Equal(TcpCallState.Active, acceptResponse.State);
        Assert.Equal(2L, acceptResponse.Revision);

        var acceptSignal = CallSignalingIntegrationTests.CallHarness.DeserializeSignal(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(acceptSignal);
        Assert.Equal(TcpCallCommandType.Accept, acceptSignal.Kind);
        Assert.Equal(CallerUserId, acceptSignal.ToUserId);
        Assert.Contains("o=callee", acceptSignal.Sdp);

        // ── 主叫 End → 双端终态 Ended(HungUp) ──
        var endId = Guid.CreateVersion7().ToString("N");
        await harness.WriteCallCommandAsync(
            callerStream,
            new TcpCallCommandRequest
            {
                RequestId = endId,
                CommandId = "cmd-end",
                CallId = callId,
                Type = TcpCallCommandType.End,
                ActorUserId = CallerUserId,
                Revision = 3,
                Grant = grant,
                ClientOccurredAtMs = 1_900_000_000_000L
            });

        var endResponse = await ReadResponseAsync(harness, callerStream);
        Assert.True(endResponse.Succeeded, endResponse.ErrorMessage);
        Assert.Equal(TcpCallState.Ended, endResponse.State);
        Assert.Equal(TcpCallEndReason.HungUp, endResponse.EndReason);
        Assert.Equal(3L, endResponse.Revision);

        // 非 silent 命令（Invite/Accept/Reconnect 携带 SDP；End/Reject/Cancel 为纯控制信号，
        // Sdp 为空）一律经临时信令路径转发给对端，对端靠 Kind 驱动本端收敛终态。
        var forwarded = forwarder.Snapshot();
        Assert.Equal(3, forwarded.Length);
        Assert.Contains(forwarded, s => s.Kind == CallCommandType.Invite && s.Sdp == OfferSdp);
        Assert.Contains(forwarded, s => s.Kind == CallCommandType.Accept && s.Sdp == AnswerSdp);
        Assert.Contains(forwarded, s => s.Kind == CallCommandType.End && s.ToUserId == CalleeUserId);
    }

    [Fact(Timeout = 15_000)]
    public async Task GrantExpired_OverRealProcessor_ReturnsStableError()
    {
        var processor = new DefaultCallControlProcessor(
            new InMemoryCallStateStore(),
            new DefaultCallGrantVerifier(new CallPolicyOptions()),
            new RecordingCallSignalForwarder(),
            new InMemoryCallAuditStore(),
            new CallPolicyOptions(),
            new CallMetrics(),
            TimeProvider.System,
            NullLogger<DefaultCallControlProcessor>.Instance);

        var realBackend = new RealtimeCallBackend(new RealtimeTestCallBus(processor));
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(realBackend);

        using var caller = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerUserId);

        var callId = $"call-{Guid.NewGuid():N}";
        var expiredGrant = new TcpCallGrant
        {
            CallId = callId,
            CallerUserId = CallerUserId,
            CalleeUserId = CalleeUserId,
            ExpiresAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000,
            Nonce = Guid.NewGuid().ToString("N"),
            Signature = "fingerprint"
        };

        await harness.WriteCallCommandAsync(
            callerStream,
            new TcpCallCommandRequest
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                CommandId = "cmd-invite",
                CallId = callId,
                Type = TcpCallCommandType.Invite,
                ActorUserId = CallerUserId,
                Revision = 1,
                Grant = expiredGrant,
                Sdp = OfferSdp
            });

        var response = await ReadResponseAsync(harness, callerStream);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpCallErrorCode.GrantExpired, response.ErrorCode);
    }

    private static string OfferSdp => "v=0\r\no=caller 1 1 IN IP4 127.0.0.1\r\ns=-\r\nm=audio 40000 RTP/AVP 0\r\n";
    private static string AnswerSdp => "v=0\r\no=callee 2 2 IN IP4 127.0.0.1\r\ns=-\r\nm=audio 40000 RTP/AVP 0\r\n";

    private static async Task<TcpCallCommandResponse> ReadResponseAsync(
        CallSignalingIntegrationTests.CallHarness harness, Stream stream)
    {
        var frame = await harness.ReadFrameAsync(stream);
        Assert.Equal(PacketCommand.CallCommandResponse, frame.Command);
        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(frame.Payload);
        Assert.NotNull(response);
        return response!;
    }

    /// <summary>
    /// 复用 noop 总线，仅把通话信令命令路由到真实 <see cref="ICallControlProcessor"/>。
    /// </summary>
    private sealed class RealtimeTestCallBus : CallSignalingIntegrationTests.NoopCallMessageBus
    {
        private readonly ICallControlProcessor _processor;

        public RealtimeTestCallBus(ICallControlProcessor processor) => _processor = processor;

        public override Task<CallProcessResult> SendCallCommandAsync(
            CallCommand command, CancellationToken ct = default)
            => _processor.ProcessAsync(command, ct);
    }

    /// <summary>记录真实状态机转发的对端信令。</summary>
    private sealed class RecordingCallSignalForwarder : ICallSignalForwarder
    {
        private readonly object _lock = new();
        private readonly List<CallSignalEnvelope> _signals = new();

        public Task<bool> ForwardAsync(CallSignalEnvelope signal, CancellationToken ct = default)
        {
            lock (_lock) _signals.Add(signal);
            return Task.FromResult(true);
        }

        public CallSignalEnvelope[] Snapshot()
        {
            lock (_lock) return _signals.ToArray();
        }
    }
}