using ChatApp.ActorRuntime.Scheduling;

namespace ChatApp.TcpGateway.Tests.ActorRuntime;

public sealed class ShardDeadlineWheelTests
{
    [Fact]
    public void ExactFullWheelDelayDoesNotWaitAnExtraRound()
    {
        var time = new ManualTimestampProvider();
        var callback = new CapturingDeadlineCallback();
        var wheel = new ShardDeadlineWheel<int, TimerState, TimerMessage>(
            time,
            TimeSpan.FromMilliseconds(10),
            callback);
        var key = 1;
        var message = new TimerMessage(42);

        wheel.Schedule(
            TimeSpan.FromMilliseconds(2560),
            generation: 1,
            in key,
            in message);
        time.Advance(TimeSpan.FromMilliseconds(2550));
        wheel.PumpExpired();
        Assert.Empty(callback.Messages);

        time.Advance(TimeSpan.FromMilliseconds(10));
        wheel.PumpExpired();
        Assert.Equal(42, Assert.Single(callback.Messages));
    }

    [Fact]
    public void StopReleasesEveryScheduledMessage()
    {
        var time = new ManualTimestampProvider();
        var callback = new CapturingDeadlineCallback();
        var wheel = new ShardDeadlineWheel<int, TimerState, TimerMessage>(
            time,
            TimeSpan.FromMilliseconds(10),
            callback);
        var key = 1;
        for (var i = 0; i < 8; i++)
        {
            var message = new TimerMessage(i);
            wheel.Schedule(
                TimeSpan.FromSeconds(1 + i),
                generation: 1,
                in key,
                in message);
        }

        wheel.Stop();
        Assert.Equal(8, callback.Dropped);
        Assert.Equal(0, wheel.PendingCount);
    }

    private struct TimerState
    {
    }

    private readonly record struct TimerMessage(int Value);

    private sealed class CapturingDeadlineCallback :
        IDeadlineCallback<int, TimerMessage>
    {
        public List<int> Messages { get; } = new();
        public int Dropped { get; private set; }

        public bool TryPostExpired(
            in int key,
            uint generation,
            in TimerMessage message)
        {
            Messages.Add(message.Value);
            return true;
        }

        public void DropScheduled(in TimerMessage message)
        {
            Dropped++;
        }
    }

    private sealed class ManualTimestampProvider : TimeProvider
    {
        private long _timestamp;

        public override long TimestampFrequency => 1000;

        public override long GetTimestamp() =>
            Volatile.Read(ref _timestamp);

        public void Advance(TimeSpan duration)
        {
            Interlocked.Add(
                ref _timestamp,
                (long)(duration.TotalSeconds * TimestampFrequency));
        }
    }
}
