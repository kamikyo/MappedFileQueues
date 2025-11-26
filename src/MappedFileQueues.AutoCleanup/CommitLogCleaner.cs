using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

namespace MappedFileQueues.AutoCleanup;

internal sealed class CommitLogCleaner<T> : IDisposable where T : struct
{
    private readonly AutoCleanupOptions _options;
    private readonly string _storePath;
    private readonly long _segmentSize;
    private readonly string _commitLogDirectory;
    private readonly int _payloadSize;
    private readonly long _messageSize;
    private readonly long _adjustedFileSize;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task? _cleanupTask;
    private bool _disposed;

    public CommitLogCleaner(string storePath, long segmentSize, AutoCleanupOptions options)
    {
        _storePath = storePath;
        _segmentSize = segmentSize;
        _options = options;
        _commitLogDirectory = Path.Combine(storePath, "commitlog");
        _payloadSize = Unsafe.SizeOf<T>();
        _messageSize = _payloadSize + 1; // EndMarkerSize = 1

        // Calculate adjusted file size (same logic as MappedFileSegment)
        var maxItems = segmentSize / _messageSize;
        _adjustedFileSize = maxItems * _messageSize;

        _cancellationTokenSource = new CancellationTokenSource();

        if (_options.EnableAutoCleanup)
        {
            StartCleanupTask();
        }
    }

    private void StartCleanupTask()
    {
        _cleanupTask = Task.Factory.StartNew(
            async () => await CleanupLoopAsync(_cancellationTokenSource.Token),
            _cancellationTokenSource.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();
    }

    private async Task CleanupLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.CleanupInterval, cancellationToken);
                PerformCleanup();
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
                break;
            }
        }
    }

    private void PerformCleanup()
    {
        if (!Directory.Exists(_commitLogDirectory))
        {
            return;
        }

        // Read consumer offset
        var consumerOffset = ReadConsumerOffset();
        if (consumerOffset <= 0)
        {
            return; // No consumer offset yet, skip cleanup
        }

        // Get all commit log files
        // File names are 20-digit numbers (fileStartOffset.ToString("D20"))
        var files = Directory.GetFiles(_commitLogDirectory, "*", SearchOption.TopDirectoryOnly)
            .Where(f => Path.GetFileName(f).Length == 20 && Path.GetFileName(f).All(char.IsDigit))
            .ToArray();
        if (files.Length == 0)
        {
            return;
        }

        // Parse file names and calculate which files are expired
        var fileInfos = new List<(string Path, long StartOffset, long EndOffset)>();

        foreach (var filePath in files)
        {
            var fileName = Path.GetFileName(filePath);
            if (fileName.Length != 20 || !long.TryParse(fileName, out var fileStartOffset))
            {
                continue; // Skip invalid file names
            }

            var fileEndOffset = fileStartOffset + _adjustedFileSize - 1;
            fileInfos.Add((filePath, fileStartOffset, fileEndOffset));
        }

        // Sort by start offset to ensure we process files in order
        fileInfos.Sort((a, b) => a.StartOffset.CompareTo(b.StartOffset));

        // Calculate the safe deletion threshold
        // Files can be deleted if: consumerOffset > fileEndOffset
        var deletionThreshold = consumerOffset;

        // Determine which files to delete
        var filesToDelete = new List<string>();
        var filesToKeep = 0;

        // Keep at least MinRetentionSegments files
        for (var i = fileInfos.Count - 1; i >= 0; i--)
        {
            var (filePath, startOffset, endOffset) = fileInfos[i];

            // Always keep the most recent files (up to MinRetentionSegments)
            if (filesToKeep < _options.MinRetentionSegments)
            {
                filesToKeep++;
                continue;
            }

            // Check if file is expired
            if (endOffset < deletionThreshold)
            {
                filesToDelete.Add(filePath);
            }
        }

        // Delete expired files
        foreach (var filePath in filesToDelete)
        {
            File.Delete(filePath);
        }
    }

    private long ReadConsumerOffset()
    {
        var offsetDir = Path.Combine(_storePath, "offset");
        var consumerOffsetPath = Path.Combine(offsetDir, "consumer.offset");

        if (!File.Exists(consumerOffsetPath))
        {
            return 0;
        }

        try
        {
            // Use MemoryMappedFile to read, same as Consumer uses to write
            // This ensures we read the latest value that Consumer has written
            // FileShare.ReadWrite allows concurrent access with Consumer
            using var fileStream = new FileStream(
                consumerOffsetPath,
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
            // If file is being written or locked, return 0 to skip cleanup
            return 0;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _cancellationTokenSource.Cancel();

        _cleanupTask?.Wait(TimeSpan.FromSeconds(5));

        _cancellationTokenSource.Dispose();
    }
}

