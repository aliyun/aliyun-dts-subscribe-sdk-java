# DTS 通道内部恢复位点

适用范围：`com.taobao.drc.client.network.dstore.SubscriptionConsumer`。不适用于另一套 `com.aliyun.dts.subscribe` consumer 的位点持久化规则。

## 空拉取

超过 `socketTimeOut` 没有收到数据时，SDK 仍向 CM 检查通道。CM 返回 DTS 时保留当前 TogoConsumer 和读取位置继续 poll；返回 DRC 时保留原有通道切换流程。首次空 poll 不会因计时器为 0 而立即触发检查。

## 明确异常后的恢复

OffsetOutOfRange、集群变化等明确异常仍会重建底层消费者。完整 consumer 重建前的 CM 通道注册和重建后的时间戳定位采用同一规则：

- 多分片模式：优先使用该 shard 的 `CheckpointManager.saveCheckpoint`，即通过 `Record.ackAsConsumed()` 推进的安全时间戳。
- 还没有确认位点：使用该 shard 最初的启动时间戳。不能因为收到较新的心跳就越过未确认记录。
- 单服务模式原本不维护 ack 队列：保守使用起始时间戳，不给不调用 ack 的用户引入无限增长的待确认队列。
- 找不到安全时间戳对应的 offset，或该时间戳早于可用数据范围时，报错退出该次定位，不静默跳到最新位置。

调用方应在记录实际处理完成后 ack，而非只入异步队列后就 ack。事务内记录沿用现有的事务安全时间戳规则，恢复可能重放已处理的部分事务或同一秒的数据，因此调用方仍需支持重复处理。

每次重建会冻结本次恢复时间戳并建立独立的待确认队列。旧批次迟到的 ack 不会推进新批次的 shard 位点。未交付缓存、事务状态按消费批次隔离；未确认的数据从安全位点重新读取。

心跳仍更新原配置 checkpoint 供现有诊断使用，但该字段不再是 DTS 内部恢复的唯一依据。保存的 ack 位点是进程内状态；进程整体重启仍须由调用方提供持久化的安全起点。本次修改不改变 DTS/DRC 跨通道状态迁移逻辑。

## 验证

```sh
mvn -o test
```

回归测试位于 `src/test/java/com/taobao/drc/client/network/dstore` 与 `src/test/java/com/taobao/drc/client/store/impl`，覆盖空拉取、CM 切换探测、异常 reset 的实际 seek 位置、事务安全时间戳、未确认心跳、迟到/乱序/重复 ack、缓存隔离、CM 注册位点以及不可用位点。
