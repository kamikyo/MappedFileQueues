namespace MappedFileQueues;

/// <summary>
/// 游标文件的周期性刷盘配置。
/// </summary>
public sealed class OffsetFlushOptions
{
    /// <summary>
    /// 是否启用该角色的游标刷盘。
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// 每累计多少条消息尝试刷盘一次；小于等于 0 表示不按条数触发。
    /// </summary>
    public int FlushEveryMessages { get; set; }

    /// <summary>
    /// 距离上次刷盘超过该时间后，在下一次 Produce 或 Commit 时触发刷盘；小于等于 0 表示不按时间触发。
    /// </summary>
    public TimeSpan FlushInterval { get; set; }

    /// <summary>
    /// 切换分段时是否强制刷盘。
    /// </summary>
    public bool FlushOnSegmentSwitch { get; set; } = true;

    /// <summary>
    /// 正常释放队列时是否强制刷盘。
    /// </summary>
    public bool FlushOnDispose { get; set; } = true;
}
