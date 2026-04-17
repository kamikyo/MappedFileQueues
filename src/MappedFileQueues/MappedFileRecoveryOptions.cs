namespace MappedFileQueues;

/// <summary>
/// 队列启动时的恢复配置。
/// </summary>
public sealed class MappedFileRecoveryOptions
{
    /// <summary>
    /// 生产者启动时是否扫描 commitlog 尾部，并按最后一条完整消息修正 producer.offset。
    /// </summary>
    public bool RecoverProducerOffsetFromCommitLogTail { get; set; } = true;

    /// <summary>
    /// 当 producer.offset 与 commitlog 尾部扫描结果冲突时，是否始终以 commitlog 尾部为准。
    /// </summary>
    public bool PreferCommitLogTailOverProducerOffset { get; set; } = true;
}
