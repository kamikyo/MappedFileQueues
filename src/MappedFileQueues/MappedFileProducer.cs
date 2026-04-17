using System.Runtime.CompilerServices;

namespace MappedFileQueues;

internal class MappedFileProducer<T> : IMappedFileProducer<T>, IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;

    private readonly MappedFilePersistenceOptions _persistenceOptions;

    // Memory mapped file to store the producer offset
    private readonly OffsetMappedFile _offsetFile;

    private readonly OffsetFlushCheckpoint _offsetFlushCheckpoint;

    private readonly int _payloadSize;

    private readonly string _segmentDirectory;

    private MappedFileSegment<T>? _segment;

    private bool _disposed;

    public MappedFileProducer(MappedFileQueueOptions options)
    {
        _options = options;
        _persistenceOptions = options.Persistence ?? MappedFilePersistenceOptions.ForMechanicalDisk();

        var offsetDir = Path.Combine(options.StorePath, Constants.OffsetDirectory);
        if (!Directory.Exists(offsetDir))
        {
            Directory.CreateDirectory(offsetDir);
        }

        var offsetPath = Path.Combine(offsetDir, Constants.ProducerOffsetFile);
        _offsetFile = new OffsetMappedFile(offsetPath);
        _offsetFlushCheckpoint = new OffsetFlushCheckpoint(_persistenceOptions.ProducerOffset);

        _payloadSize = Unsafe.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);

        RecoverOffsetFromCommitLogTail();
    }

    public long Offset => _offsetFile.Offset;

    public void Produce(ref T message)
    {
        if(_disposed) throw new ObjectDisposedException(nameof(MappedFileProducer<T>));

        _segment ??= FindOrCreateSegmentByOffset();

        _segment.Write(_offsetFile.Offset, ref message);

        Commit();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        FlushSegmentOnDispose();
        FlushOffsetOnDispose();
        _offsetFile.Dispose();
        _segment?.Dispose();
    }

    private void Commit()
    {
        if(_disposed) throw new ObjectDisposedException(nameof(MappedFileProducer<T>));

        if (_segment == null)
        {
            throw new InvalidOperationException("Segment is not initialized.");
        }

        _offsetFile.Advance(_payloadSize + Constants.EndMarkerSize);
        var shouldFlushOffset = _offsetFlushCheckpoint.RecordMessageAndShouldFlush();

        // Check if the segment has reached its limit
        if (_segment.AllowedLastOffsetToWrite < _offsetFile.Offset)
        {
            _segment.Flush();
            FlushOffsetOnSegmentSwitch(shouldFlushOffset);

            // Dispose the current segment and will create a new one on the next Produce call
            _segment.Dispose();
            _segment = null;
            return;
        }

        if (shouldFlushOffset)
        {
            FlushOffset();
        }
    }

    private void FlushOffsetOnSegmentSwitch(bool shouldFlushOffset)
    {
        if (shouldFlushOffset || _offsetFlushCheckpoint.ShouldFlushOnSegmentSwitch)
        {
            FlushOffset();
        }
    }

    private void FlushOffsetOnDispose()
    {
        if (_offsetFlushCheckpoint.ShouldFlushOnDispose)
        {
            FlushOffset();
        }
    }

    private void FlushOffset()
    {
        _offsetFile.Flush();
        _offsetFlushCheckpoint.MarkFlushed();
    }

    private void FlushSegmentOnDispose()
    {
        _segment?.Flush();
    }

    private void RecoverOffsetFromCommitLogTail()
    {
        var recoveryOptions = _persistenceOptions.Recovery;
        if (!recoveryOptions.RecoverProducerOffsetFromCommitLogTail)
        {
            return;
        }

        if (!MappedFileSegment<T>.TryFindTailOffset(_segmentDirectory, _options.SegmentSize, out var tailOffset))
        {
            return;
        }

        if (recoveryOptions.PreferCommitLogTailOverProducerOffset || tailOffset > _offsetFile.Offset)
        {
            _offsetFile.MoveTo(tailOffset);
            _offsetFlushCheckpoint.MarkDirty();
        }
    }

    private MappedFileSegment<T> FindOrCreateSegmentByOffset() =>
        MappedFileSegment<T>.FindOrCreate(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset);
}
