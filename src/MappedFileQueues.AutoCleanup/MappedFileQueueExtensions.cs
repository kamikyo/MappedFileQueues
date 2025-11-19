namespace MappedFileQueues.AutoCleanup;

/// <summary>
/// Extension methods for enabling automatic cleanup of expired commit log files.
/// </summary>
public static class MappedFileQueueExtensions
{
    /// <summary>
    /// Enables automatic cleanup of expired commit log files for the specified queue.
    /// </summary>
    /// <typeparam name="T">The type of message stored in the queue.</typeparam>
    /// <param name="queue">The queue to enable cleanup for.</param>
    /// <param name="queueOptions">The queue options containing StorePath and SegmentSize.</param>
    /// <param name="cleanupOptions">The cleanup options. If null, default options will be used.</param>
    /// <returns>A disposable cleanup service that should be disposed when the queue is disposed.</returns>
    public static IDisposable EnableAutoCleanup<T>(
        this MappedFileQueue<T> queue, 
        MappedFileQueueOptions queueOptions,
        AutoCleanupOptions? cleanupOptions = null) where T : struct
    {
        if (queue == null)
        {
            throw new ArgumentNullException(nameof(queue));
        }

        if (queueOptions == null)
        {
            throw new ArgumentNullException(nameof(queueOptions));
        }

        cleanupOptions ??= new AutoCleanupOptions();

        return new CommitLogCleaner<T>(queueOptions.StorePath, queueOptions.SegmentSize, cleanupOptions);
    }
}

