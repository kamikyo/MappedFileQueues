using System.Diagnostics;

namespace MappedFileQueues;

internal sealed class OffsetFlushCheckpoint
{
    private readonly OffsetFlushOptions _options;
    private readonly long _flushIntervalTimestampTicks;

    private long _pendingMessages;
    private long _lastFlushTimestamp;

    public OffsetFlushCheckpoint(OffsetFlushOptions? options)
    {
        _options = options ?? new OffsetFlushOptions { Enabled = false };
        _flushIntervalTimestampTicks = ToTimestampTicks(_options.FlushInterval);
        _lastFlushTimestamp = Stopwatch.GetTimestamp();
    }

    public bool ShouldFlushOnSegmentSwitch =>
        _options.Enabled && _options.FlushOnSegmentSwitch && _pendingMessages > 0;

    public bool ShouldFlushOnDispose =>
        _options.Enabled && _options.FlushOnDispose && _pendingMessages > 0;

    public bool RecordMessageAndShouldFlush()
    {
        if (!_options.Enabled)
        {
            return false;
        }

        _pendingMessages++;

        if (_options.FlushEveryMessages > 0 && _pendingMessages >= _options.FlushEveryMessages)
        {
            return true;
        }

        return _flushIntervalTimestampTicks > 0 &&
            Stopwatch.GetTimestamp() - _lastFlushTimestamp >= _flushIntervalTimestampTicks;
    }

    public void MarkDirty()
    {
        if (!_options.Enabled || _pendingMessages > 0)
        {
            return;
        }

        _pendingMessages = 1;
    }

    public void MarkFlushed()
    {
        _pendingMessages = 0;
        _lastFlushTimestamp = Stopwatch.GetTimestamp();
    }

    private static long ToTimestampTicks(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
        {
            return 0;
        }

        return Math.Max(1, (long)(interval.TotalSeconds * Stopwatch.Frequency));
    }
}
