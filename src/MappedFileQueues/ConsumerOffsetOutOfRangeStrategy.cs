namespace MappedFileQueues;

/// <summary>
/// 消费者游标落在当前可用数据范围外时的处理策略。
/// </summary>
public enum ConsumerOffsetOutOfRangeStrategy
{
    /// <summary>
    /// 当消费者游标早于当前最老分段时，自动跳转到最老可用分段继续消费。
    /// </summary>
    MoveToEarliestSegment = 0,

    /// <summary>
    /// 当消费者游标早于当前最老分段时，立即抛出异常并停止消费。
    /// </summary>
    FailFast = 1
}
