using System.Net;
using System.Net.Sockets;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Shared.Protocol.Tcp;
using ChatApp.TcpGateway.Gateway.Commands.Calls;
using ChatApp.TcpGateway.Gateway.Networking.Sessions;
using ChatApp.TcpGateway.Observability.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using RealtimeCallCommand = ChatApp.Realtime.Abstractions.Calls.CallCommand;
using RealtimeCallGrant = ChatApp.Realtime.Abstractions.Calls.CallGrant;
using TcpGatewayService = ChatApp.TcpGateway.Gateway.Networking.TcpGatewayService;

namespace ChatApp.TcpGateway.Tests.Networking;

/// <summary>
/// GROUP-CALL-1：群通话（Mesh ≤4 人）无状态信令中继端到端验证——3 条真实 TCP 连接。
/// <para>
/// 验证：群组 grant（HMAC 覆盖全部参与者）校验通过后，invite 按参与者名单扇出到其余成员
/// （排除发起者）；成员离开（End）扇出 participant-left 事件；非成员 / 篡改名单 / 过期 /
/// 未配置密钥 / 非主叫 invite 全部 fail-closed；群组命令不触碰 Realtime 1:1 状态机后端。
/// </para>
/// </summary>
public sealed class GroupCallSignalingIntegrationTests
{
    private const string Secret = "group-call-e2e-signing-secret";
    private const long CallerId = 42;
    private static readonly long[] Participants = [42, 43, 44];
    private const string OfferSdp = "v=0\r\no=caller 1 1 IN IP4 127.0.0.1\r\ns=-\r\nm=audio 40000 RTP/AVP 0\r\n";

    private static TcpCallGrant SignedGroupGrant(
        long expiresAtMs,
        long[]? participants = null,
        long callerUserId = CallerId)
    {
        var grant = new TcpCallGrant
        {
            CallId = "call-group-1",
            CallerUserId = callerUserId,
            CalleeUserId = 0,
            ExpiresAtMs = expiresAtMs,
            Nonce = "nonce-group-e2e",
            CallKind = TcpCallKind.Group,
            Participants = participants ?? Participants
        };
        grant.Signature = TcpCallGrantSignature.Sign(Secret, grant);
        return grant;
    }

    private static TcpCallCommandRequest GroupRequest(
        TcpCallCommandType type,
        TcpCallGrant grant,
        long actorUserId,
        long revision = 1,
        long? inviteTarget = null) => new()
    {
        RequestId = $"req-{Guid.NewGuid():N}",
        CommandId = $"cmd-{(int)type}-{Guid.NewGuid():N}",
        CallId = grant.CallId,
        Type = type,
        ActorUserId = actorUserId,
        Revision = revision,
        Grant = grant,
        Sdp = type == TcpCallCommandType.Invite ? OfferSdp : null,
        ClientOccurredAtMs = 1_900_000_000_000L,
        ParticipantUserId = inviteTarget
    };

    [Fact(Timeout = 15_000)]
    public async Task GroupInvite_FansOutInviteToAllParticipantsExceptCaller()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        using var third = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await third.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();
        await using var thirdStream = third.GetStream();

        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);
        await harness.AuthenticateAsync(thirdStream, "third-token", 44);

        await harness.WriteCallCommandAsync(
            callerStream,
            GroupRequest(TcpCallCommandType.Invite, SignedGroupGrant(1_900_000_100_000L), CallerId));

        // 发起者收到成功响应（无状态回显 Ringing + 请求 revision）。
        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(response);
        Assert.True(response.Succeeded);
        Assert.Equal(TcpCallState.Ringing, response.State);
        Assert.Null(response.ErrorCode);

        // 其余两名成员各收到一条 invite 信号（按名单扇出，排除发起者）。
        foreach (var (stream, expectedToUserId) in new[] { (calleeStream, 43L), (thirdStream, 44L) })
        {
            var signal = CallSignalingIntegrationTests.CallHarness.DeserializeSignal(
                (await harness.ReadFrameAsync(stream)).Payload);
            Assert.NotNull(signal);
            Assert.Equal("call-group-1", signal.CallId);
            Assert.Equal(CallerId, signal.FromUserId);
            Assert.Equal(expectedToUserId, signal.ToUserId);
            Assert.Equal(TcpCallCommandType.Invite, signal.Kind);
            Assert.Contains("o=caller", signal.Sdp);
            Assert.Null(signal.Event);
        }

        // 无状态中继：不触碰 Realtime 1:1 状态机后端。
        Assert.False(backend.Called);
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupInvite_ActorNotInGrant_FailClosed()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var intruder = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        await intruder.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var intruderStream = intruder.GetStream();
        await using var calleeStream = callee.GetStream();
        await harness.AuthenticateAsync(intruderStream, "intruder-token", 99);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);

        await harness.WriteCallCommandAsync(
            intruderStream,
            GroupRequest(TcpCallCommandType.Invite, SignedGroupGrant(1_900_000_100_000L), 99));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(intruderStream)).Payload);
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpCallErrorCode.GrantInvalid, response.ErrorCode);

        await AssertNoSignalAsync(harness, calleeStream);
        Assert.False(backend.Called);
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupInvite_TamperedParticipants_FailClosed()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);

        // 签名覆盖名单 [42,43,44]，发送时替换为 [42,43,99] → 签名不匹配，fail-closed。
        var grant = SignedGroupGrant(1_900_000_100_000L);
        grant.Participants = [42, 43, 99];
        await harness.WriteCallCommandAsync(
            callerStream,
            GroupRequest(TcpCallCommandType.Invite, grant, CallerId));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpCallErrorCode.GrantInvalid, response.ErrorCode);

        await AssertNoSignalAsync(harness, calleeStream);
        Assert.False(backend.Called);
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupCommand_ExpiredGrant_ReturnsStableExpiredCode()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var caller = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);

        await harness.WriteCallCommandAsync(
            callerStream,
            GroupRequest(TcpCallCommandType.Invite, SignedGroupGrant(1_000L), CallerId));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpCallErrorCode.GrantExpired, response.ErrorCode);
        Assert.False(backend.Called);
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupCommand_SecretNotConfigured_FailClosed()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend);

        using var caller = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);

        await harness.WriteCallCommandAsync(
            callerStream,
            GroupRequest(TcpCallCommandType.Invite, SignedGroupGrant(1_900_000_100_000L), CallerId));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpCallErrorCode.GrantInvalid, response.ErrorCode);
        Assert.False(backend.Called);
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupInvite_ByNonCallerMember_FailClosed()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var callee = new TcpClient { NoDelay = true };
        using var third = new TcpClient { NoDelay = true };
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await third.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var calleeStream = callee.GetStream();
        await using var thirdStream = third.GetStream();
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);
        await harness.AuthenticateAsync(thirdStream, "third-token", 44);

        await harness.WriteCallCommandAsync(
            calleeStream,
            GroupRequest(TcpCallCommandType.Invite, SignedGroupGrant(1_900_000_100_000L), 43));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(calleeStream)).Payload);
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpCallErrorCode.GrantInvalid, response.ErrorCode);

        await AssertNoSignalAsync(harness, thirdStream);
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupEnd_ByMember_FansOutParticipantLeftToRemaining()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        using var third = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await third.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();
        await using var thirdStream = third.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);
        await harness.AuthenticateAsync(thirdStream, "third-token", 44);

        // 成员 43 主动离开（End 携带群组 grant）。
        await harness.WriteCallCommandAsync(
            calleeStream,
            GroupRequest(TcpCallCommandType.End, SignedGroupGrant(1_900_000_100_000L), 43, revision: 3));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(calleeStream)).Payload);
        Assert.NotNull(response);
        Assert.True(response.Succeeded);
        Assert.Equal(TcpCallState.Ended, response.State);
        Assert.Equal(TcpCallEndReason.HungUp, response.EndReason);
        Assert.Equal(3, response.Revision);

        // 其余成员收到 participant-left 事件（带离开者 Id）。
        foreach (var stream in new[] { callerStream, thirdStream })
        {
            var signal = CallSignalingIntegrationTests.CallHarness.DeserializeSignal(
                (await harness.ReadFrameAsync(stream)).Payload);
            Assert.NotNull(signal);
            Assert.Equal(TcpCallCommandType.End, signal.Kind);
            Assert.Equal(TcpCallConstants.SignalEventParticipantLeft, signal.Event);
            Assert.Equal(43, signal.ParticipantUserId);
            Assert.Equal(43, signal.FromUserId);
            Assert.Equal(3, signal.Revision);
        }

        Assert.False(backend.Called);
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupCancel_ByCaller_FansOutCancel()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        using var third = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await third.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();
        await using var thirdStream = third.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);
        await harness.AuthenticateAsync(thirdStream, "third-token", 44);

        // 主叫撤销（cancel）：按名单扇出，响应回显 Ended(Cancelled)。
        await harness.WriteCallCommandAsync(
            callerStream,
            GroupRequest(TcpCallCommandType.Cancel, SignedGroupGrant(1_900_000_100_000L), CallerId));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(response);
        Assert.True(response.Succeeded);
        Assert.Equal(TcpCallState.Ended, response.State);
        Assert.Equal(TcpCallEndReason.Cancelled, response.EndReason);

        foreach (var stream in new[] { calleeStream, thirdStream })
        {
            var signal = CallSignalingIntegrationTests.CallHarness.DeserializeSignal(
                (await harness.ReadFrameAsync(stream)).Payload);
            Assert.NotNull(signal);
            Assert.Equal(TcpCallCommandType.Cancel, signal.Kind);
            Assert.Equal(CallerId, signal.FromUserId);
            Assert.Null(signal.Event);
        }
    }

    /// <summary>断言目标会话短时间内不会收到任何帧（负向断言：未授权不扇出）。</summary>
    private static async Task AssertNoSignalAsync(
        CallSignalingIntegrationTests.CallHarness harness,
        Stream stream)
    {
        var readTask = harness.ReadFrameAsync(stream).AsTask();
        var completed = await Task.WhenAny(readTask, Task.Delay(400));
        Assert.NotSame(readTask, completed);
    }

    // ---- GROUP-CALL-SDP-1 / GAP-1：逐成员 invite 目标透传 + invite 随信令下发 grant ----

    [Fact(Timeout = 15_000)]
    public async Task GroupInvite_WithRosterTarget_SignalsCarryTargetAndGrant()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        using var third = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await third.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();
        await using var thirdStream = third.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);
        await harness.AuthenticateAsync(thirdStream, "third-token", 44);

        // 逐成员 invite：目标成员 43（grant 名单内）。
        var grant = SignedGroupGrant(1_900_000_100_000L);
        await harness.WriteCallCommandAsync(
            callerStream,
            GroupRequest(TcpCallCommandType.Invite, grant, CallerId, inviteTarget: 43));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(response);
        Assert.True(response.Succeeded);

        // 全部扇出信号携带目标成员 Id 与随信令下发的 grant（真实 wire 编解码往返）。
        foreach (var (stream, expectedToUserId) in new[] { (calleeStream, 43L), (thirdStream, 44L) })
        {
            var signal = CallSignalingIntegrationTests.CallHarness.DeserializeSignal(
                (await harness.ReadFrameAsync(stream)).Payload);
            Assert.NotNull(signal);
            Assert.Equal(TcpCallCommandType.Invite, signal.Kind);
            Assert.Equal(expectedToUserId, signal.ToUserId);
            Assert.Equal(43, signal.ParticipantUserId); // 被邀成员 Id（非 ToUserId）
            Assert.Null(signal.Event);
            Assert.NotNull(signal.Grant);
            Assert.Equal("call-group-1", signal.Grant.CallId);
            Assert.Equal(TcpCallKind.Group, signal.Grant.CallKind);
            Assert.Equal(Participants, signal.Grant.Participants);
            Assert.Equal(grant.Signature, signal.Grant.Signature);
        }
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupInvite_TargetOutsideRoster_FailClosed()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);

        // 邀请名单外用户 77（客户端未重签即邀人）→ fail-closed，不扇出。
        await harness.WriteCallCommandAsync(
            callerStream,
            GroupRequest(TcpCallCommandType.Invite, SignedGroupGrant(1_900_000_100_000L), CallerId, inviteTarget: 77));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(response);
        Assert.False(response.Succeeded);
        Assert.Equal(TcpCallErrorCode.GrantInvalid, response.ErrorCode);
        await AssertNoSignalAsync(harness, calleeStream);
    }

    [Fact(Timeout = 15_000)]
    public async Task GroupInvite_WithoutTarget_LegacyBroadcastSignalsCarryNoTarget()
    {
        var backend = new RecordingBackend();
        await using var harness = await CallSignalingIntegrationTests.CallHarness.StartAsync(backend, Secret);

        using var caller = new TcpClient { NoDelay = true };
        using var callee = new TcpClient { NoDelay = true };
        await caller.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await callee.ConnectAsync(IPAddress.Loopback, harness.Port, harness.Token);
        await using var callerStream = caller.GetStream();
        await using var calleeStream = callee.GetStream();
        await harness.AuthenticateAsync(callerStream, "caller-token", CallerId);
        await harness.AuthenticateAsync(calleeStream, "callee-token", 43);

        // 0.5.7 形态（目标缺省）：广播 invite 信号不携带目标成员（grant 仍加性下发——
        // 0.5.7 旧解码端忽略未知字段，零改动）。
        await harness.WriteCallCommandAsync(
            callerStream,
            GroupRequest(TcpCallCommandType.Invite, SignedGroupGrant(1_900_000_100_000L), CallerId));

        var response = CallSignalingIntegrationTests.CallHarness.DeserializeResponse(
            (await harness.ReadFrameAsync(callerStream)).Payload);
        Assert.NotNull(response);
        Assert.True(response.Succeeded);

        var signal = CallSignalingIntegrationTests.CallHarness.DeserializeSignal(
            (await harness.ReadFrameAsync(calleeStream)).Payload);
        Assert.NotNull(signal);
        Assert.Equal(TcpCallCommandType.Invite, signal.Kind);
        Assert.Null(signal.ParticipantUserId);
        Assert.Null(signal.Event);
    }

    /// <summary>记录型 1:1 后端：群组命令绝不应触达。</summary>
    private sealed class RecordingBackend : ICallBackend
    {
        public bool Called { get; private set; }

        public Task<CallCommandBackendResult> SendCommandAsync(
            string requestId,
            long actorUserId,
            string actorSessionId,
            string commandId,
            string callId,
            TcpCallCommandType type,
            long revision,
            TcpCallGrant? grant,
            string? sdp,
            long clientOccurredAtMs,
            CancellationToken cancellationToken = default)
        {
            Called = true;
            return Task.FromResult(CallCommandBackendResult.Failed(
                requestId, callId, TcpCallErrorCode.StateStoreUnavailable, "unavailable"));
        }
    }
}
