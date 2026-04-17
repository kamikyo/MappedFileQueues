namespace MappedFileQueues;

/// <summary>
/// 队列持久化配置，默认值偏向机械硬盘友好：降低刷盘频率，依赖启动恢复兜底。
/// </summary>
public sealed class MappedFilePersistenceOptions
{
    /// <summary>
    /// 生产者游标刷盘配置。
    /// </summary>
    public OffsetFlushOptions ProducerOffset { get; set; } = new()
    {
        Enabled = true,
        FlushEveryMessages = 16_384,
        FlushInterval = TimeSpan.FromSeconds(1),
        FlushOnSegmentSwitch = true,
        FlushOnDispose = true
    };

    /// <summary>
    /// 消费者游标刷盘配置。
    /// </summary>
    public OffsetFlushOptions ConsumerOffset { get; set; } = new()
    {
        Enabled = true,
        FlushEveryMessages = 65_536,
        FlushInterval = TimeSpan.FromSeconds(5),
        FlushOnSegmentSwitch = true,
        FlushOnDispose = true
    };

    /// <summary>
    /// 启动恢复配置。
    /// </summary>
    public MappedFileRecoveryOptions Recovery { get; set; } = new();

    /// <summary>
    /// 创建机械硬盘友好的默认持久化配置。
    /// </summary>
    public static MappedFilePersistenceOptions ForMechanicalDisk() => new();
}
