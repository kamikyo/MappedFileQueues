using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

namespace MappedFileQueues;

internal class MappedFileConsumer<T> : IMappedFileConsumer<T>, IDisposable where T : struct
{
    private readonly MappedFileQueueOptions _options;

    // Memory mapped file to store the consumer offset
    private readonly OffsetMappedFile _offsetFile;

    private readonly int _payloadSize;

    private readonly string _segmentDirectory;

    private readonly string _producerOffsetPath;

    private MappedFileSegment<T>? _segment;

    private bool _disposed;

    public MappedFileConsumer(MappedFileQueueOptions options)
    {
        _options = options;

        var offsetDir = Path.Combine(options.StorePath, Constants.OffsetDirectory);
        if (!Directory.Exists(offsetDir))
        {
            Directory.CreateDirectory(offsetDir);
        }

        var offsetPath = Path.Combine(offsetDir, Constants.ConsumerOffsetFile);
        _offsetFile = new OffsetMappedFile(offsetPath);

        _payloadSize = Unsafe.SizeOf<T>();

        _segmentDirectory = Path.Combine(options.StorePath, Constants.CommitLogDirectory);

        var producerOffsetPath = Path.Combine(offsetDir, Constants.ProducerOffsetFile);
        _producerOffsetPath = producerOffsetPath;
    }

    public long Offset => _offsetFile.Offset;

    public void AdjustOffset(long offset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater than or equal to zero.");
        }

        if (_segment != null)
        {
            throw new InvalidOperationException(
                "Cannot adjust offset while there is an active segment. Please adjust the offset before consuming any messages.");
        }

        _offsetFile.MoveTo(offset);
    }

    public void Consume(out T message)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var retryIntervalMs = (int)_options.ConsumerRetryInterval.TotalMilliseconds;
        var spinWaitDurationMs = (int)_options.ConsumerSpinWaitDuration.TotalMilliseconds;
RESET_TAG:
        while (_segment == null)
        {
            if (!TryFindSegmentByOffset(out _segment))
            {
                Thread.Sleep(retryIntervalMs);
            }
        }

        var spinWait = new SpinWait();
        var startTicks = DateTime.UtcNow.Ticks;
        var consumerOffsetUnchangedCount = 0;
        long? lastConsumerOffsetInSleep = null;
        bool shouldCheckProducerOffset = false;
        long? lastProducerOffset = null;

        while (!_segment.TryRead(_offsetFile.Offset, out message))
        {
            // Spin wait until the message is available or timeout
            if ((DateTime.UtcNow.Ticks - startTicks) / TimeSpan.TicksPerMillisecond > spinWaitDurationMs)
            {
                // Record current consumer offset before sleep
                var currentConsumerOffset = _offsetFile.Offset;

                // Check if consumer offset has changed since last sleep
                if (lastConsumerOffsetInSleep.HasValue && currentConsumerOffset == lastConsumerOffsetInSleep.Value)
                {
                    // Consumer offset hasn't changed, increment counter
                    consumerOffsetUnchangedCount++;
                    
                    // If consumer offset hasn't changed for UnMatchedCheckCount times, start checking producer offset
                    if (consumerOffsetUnchangedCount >= _options.UnMatchedCheckCount)
                    {
                        shouldCheckProducerOffset = true;
                    }
                }
                else
                {
                    // Consumer offset has changed, reset counter
                    consumerOffsetUnchangedCount = 0;
                    shouldCheckProducerOffset = false;
                    lastProducerOffset = null;
                }

                // Update last consumer offset
                lastConsumerOffsetInSleep = currentConsumerOffset;

                // If we should check producer offset, do it now
                if (shouldCheckProducerOffset)
                {
                    var currentProducerOffset = ReadProducerOffset();
                    if (currentProducerOffset.HasValue)
                    {
                        if (lastProducerOffset.HasValue && currentProducerOffset.Value > lastProducerOffset.Value)
                        {
                            _segment?.Dispose();
                            _segment = null;

                            // Producer offset is changing, immediately align consumer offset to producer offset
                            AdjustOffset(lastProducerOffset.Value);
goto RESET_TAG;
                        }
                        lastProducerOffset = currentProducerOffset;
                    }
                }

                // Sleep for a short interval before retrying if spin wait times out
                Thread.Sleep(retryIntervalMs);
            }

            // Use SpinWait to avoid busy waiting
            spinWait.SpinOnce();
        }
    }

    public void Commit()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_segment == null)
        {
            throw new InvalidOperationException(
                $"No matched segment found. Ensure {nameof(Consume)} is called before {nameof(Commit)}.");
        }

        _offsetFile.Advance(_payloadSize + Constants.EndMarkerSize);

        // Check if the segment is fully consumed
        if (_offsetFile.Offset > _segment.AllowedLastOffsetToWrite)
        {
            _segment.Dispose();
            _segment = null;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _offsetFile.Dispose();
        _segment?.Dispose();
    }

    private bool TryFindSegmentByOffset([MaybeNullWhen(false)] out MappedFileSegment<T> segment) =>
        MappedFileSegment<T>.TryFind(
            _segmentDirectory,
            _options.SegmentSize,
            _offsetFile.Offset,
            out segment);

    private long? ReadProducerOffset()
    {
        try
        {
            if (!File.Exists(_producerOffsetPath))
            {
                return null;
            }

            using var fileStream = new FileStream(
                _producerOffsetPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite);

            using var mmf = MemoryMappedFile.CreateFromFile(
                fileStream,
                null,
                sizeof(long),
                MemoryMappedFileAccess.Read,
                HandleInheritability.None,
                true);

            using var accessor = mmf.CreateViewAccessor(0, sizeof(long), MemoryMappedFileAccess.Read);
            accessor.Read(0, out long offset);
            return offset;
        }
        catch
        {
            // If we can't read the producer offset, return null
            return null;
        }
    }
}
