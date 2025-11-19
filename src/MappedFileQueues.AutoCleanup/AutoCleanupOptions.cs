namespace MappedFileQueues.AutoCleanup;

/// <summary>
/// Options for automatic cleanup of expired commit log files.
/// </summary>
public class AutoCleanupOptions
{
    /// <summary>
    /// Whether to enable automatic cleanup of expired commit log files.
    /// </summary>
    public bool EnableAutoCleanup { get; set; } = true;

    /// <summary>
    /// The interval between cleanup operations.
    /// </summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The minimum number of segment files to retain, even if they are expired.
    /// This provides an additional safety buffer to prevent accidental deletion of files that might still be in use.
    /// </summary>
    public int MinRetentionSegments { get; set; } = 2;
}

