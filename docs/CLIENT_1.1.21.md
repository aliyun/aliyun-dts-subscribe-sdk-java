# Client 1.1.21：ACK、恢复位点与升级说明

适用于 `com.alibaba.dts.client:client:1.1.21` 的 `com.taobao.drc.client.DRCClient` API。
不改变另一套 `com.aliyun.dts.subscribe` 独立客户端的使用契约。本版本为本地候选，尚未发布。

## 固定的完成规则

不新增 `client.ackMode` 或其他模式选择接口。完成规则只由启动方法决定，CM 选择 DRC 或 DTS 不改变规则。

| 启动方式 | DRC store | DTS store | 消费方的责任 |
|---|---|---|---|
| `startService()` | 整个 `notify()` 成功返回后确认；按合格心跳恢复 | 整个 `notify()` 成功返回后确认；按事务安全时间戳恢复 | 返回前完成这一批全部记录 |
| `startMultiService()` | 每条记录 ACK，连续确认前缀中的心跳用于恢复 | 每条记录 ACK，连续确认前缀的事务安全时间戳用于恢复 | 每条记录处理成功后 ACK |

单订阅调用 `record.ackAsConsumed()` 是安全无操作，不提前确认，也不抛未入链 NPE。
即使批内全部调用过 ACK，只要单订阅回调最终抛异常，这一批仍未确认。

散列订阅即使只有一个实际 subTopic，仍要求逐条 ACK。对 SDK **实际交付**的 DML、DDL、心跳、BEGIN、COMMIT、ROLLBACK 均需确认；仅 ACK 批内最后一条不够。可以重复、乱序或跨线程 ACK，但缺口未补齐前不得越过它推进恢复位点。须对原始 Record ACK，序列化副本不能确认原交付。

## 历史兼容范围

旧 DRC 手册的单订阅示例在回调内同步处理、不调用 ACK，仍然可用，不会因漏 ACK 冻结恢复位点。历史散列订阅的逐条 ACK 要求保留。

不保留 1.1.19/1.1.20 过渡实现的单订阅异步 ACK 语义。如果现有 `startService()` 在 `notify()` 中仅入队就返回、实际处理在后台进行，升级前必须调整：等待整批处理成功再返回，或采用 `startMultiService()` 并在异步处理成功后逐条 ACK。SDK 无法从回调返回自动区分“已经完成”和“仅完成入队”。

历史证据与设计取舍见 [设计方案](superpowers/specs/2026-10-30-ack-compatibility-design.md)。不能把散列订阅的 ACK 要求追认为所有历史单订阅的要求。

## 位点怎样推进

SDK 在调用业务前捕获不可变候选，业务修改 DataMessage 列表或 Record 字段不会改变候选。每个 subTopic 独立管理连接代次，旧连接的回调完成、失败通知或迟到 ACK 不得推进新代次的位点。

- DRC：只有已成功交付的单订阅批次，或已进入散列连续确认前缀的心跳，才更新恢复用 heartbeat timestamp。仅收到、仍在缓存中或仍被 ACK 缺口阻挡的心跳不能用于恢复。
- DTS：使用成功批次或连续确认前缀的事务安全时间戳。事务内采用 BEGIN 的安全时间，COMMIT/ROLLBACK 后退出事务状态。CM 注册与 offset 查找使用同一冻结快照。
- 尚无确认边界时使用本分片的初始安全起点。时间戳不是简单取最大值：推进依据是交付顺序，保留源协议允许的时间回退。

发生 notify 或解析异常时，当前代次失败并停止后续交付，随后进入重连。`handleException()` 只作通知，返回正常不表示失败数据已消费；它再次抛出的错误作为 suppressed 保留，不覆盖原异常。

恢复允许重复消费，同秒记录、部分事务及断连前未确认数据都可能重放。业务需具备重复处理能力；内存中的 ACK 状态不代替进程重启所需的持久化起点，也不提供 exactly-once。

## 通道、分片变化和重启

同一通道、同一分片普通重连保留已确认位置。任一散列连接退出时，协调器停止并等待其余连接后再重连。DTS 的集群切换和 OffsetOutOfRange 也走这个路径。

本仓库尚无经过验证的跨 DRC/DTS 或重分片位点映射，因此通道切换、分片新增/删减/替换时，整组回退到初始化时保存的原始时间戳，并输出回退原因。不会复用另一协议的 offset、position 或最新接收心跳。这可能重放从原始时间到当前的全部数据；不能将它误判为 ACK 停滞。原始起点只有物理 position、没有可迁移时间戳，或起点已过保留期时明确报错，不猜测或跳到最新位置。

旧手册的外层重启方式可继续使用：

```java
Thread worker = client.startService(); // 散列订阅使用 startMultiService()
worker.join();
Throwable failure = client.getLastFailure();
// 先处理终止原因；ACK 预算、不可迁移起点、数据过期不应盲目循环重启。
client.resetService();
// 之后可再次 startService()，保留同一会话已确认的恢复状态。
```

运行中或回调内调用 reset 会被拒绝；重复 reset 幂等。保留状态的会话不能从单订阅改为散列订阅或反过来，需创建新客户端。外部 `stopService()` 等待工作线程退出；回调内 stop 只请求停止，避免自己等待自己。业务回调必须能够返回，SDK 不能强行撤销已执行的业务副作用。停止和异常退出后仍可查询位点状态及最后错误。

既有 10 秒重试间隔和重试配置保留；连接成功后重置连续连接失败预算。显式 ACK 预算超限、不可迁移、无可用恢复数据属于终止性错误，不进行内部自动重试。

## 参数和容量

| 参数或容量 | 1.1.21 行为 |
|---|---|
| `client.maxPendingAckRanges` | 非负整数，默认 `0`：不设置隐式区间上限；正数限制每个散列分片的乱序已确认区间数，DRC/DTS 都生效 |
| `client.requireCompleteTxn` 及现有批量参数 | 保持现有缓存开关和阈值规则；大事务可能分段交付，批量条数不是所有情况下的硬上限 |
| `client.maxNumOfRecordsPerMessage` | 是缓存出批阈值；不能当成最大实际批次或 ACK 在途容量 |
| 业务 ringbuffer/队列容量 | 不再与 SDK 固定 10240 pending 阈值耦合；仍须按业务对象占用、所有队列、并行度及事务大小控制内存 |

已删除 ACK 跟踪的 10240 固定条数上限。实际订阅路径使用序号凭证和压缩区间，不由 manager 保留 Record 负载。顺序 ACK 直接推进；未 ACK 的记录不会各占一个已确认区间，但业务持有的记录仍消耗内存。

可选区间预算在合并、连续前缀推进后检查，等于上限允许。超过上限会输出 ERROR，触发该次订阅终止，并向触发 ACK 的线程抛出 `CheckpointTrackingException`；即使没有新消息，后台 ACK 也能通知消费者停止。保留最后确认边界，禁止“冻结位点但继续投递”。应减少 ACK 乱序或调整预算后重启，期间未确认数据会重放。

不新增 ACK 停滞参数。有未确认记录且连续前缀 60 秒未推进时，按 60 秒限频 WARN；该告警不代替业务监控，也不擅自跳过数据或重连。

## 监控

`client.getCheckpointStatus()` 返回按 topic 的不可变快照，多个分片不是全局原子快照：

| 方法 | 含义 |
|---|---|
| `isAckTrackingEnabled()` | 是否要求显式 ACK，仅散列订阅为 true |
| `getChannel()` / `getGeneration()` / `isActive()` | 通道、分片内代次、是否仍可确认 |
| `getReceivedTimestamp()` | 已接收记录时间，包含尚未出缓存的记录，不是恢复依据 |
| `getSafeTimestamp()` | 已确认的事务安全边界 |
| `getConfirmedHeartbeat()` / `getRecoveryTimestamp()` | 合格心跳及当前实际恢复边界，DRC 不以普通 DML 边界代替心跳 |
| `getPendingCount()` / `getPendingAckRangeCount()` | 未确认交付数 / 保留的乱序已确认区间数，两者含义不同 |
| `getLastProgressAgeMillis()` / `getRecoveryProgressAgeMillis()` | 确认前缀 / 实际恢复边界停滞时长，使用单调时钟 |
| `isTrackingOverflowed()` / `getMaxPendingAckRanges()` | 是否已超限及显式预算 |
| `getFailureMessage()` / `getRecoveryReason()` | 分片失败及保守回退原因 |

根级错误（例如 CM 请求失败或不可迁移）通过 `client.getLastFailure()` 查询。跨通道或分片集合变化会新建分片状态，generation 仅在对应 manager 内有意义。

## 验收边界

本地回归覆盖单订阅无 ACK、失败不确认、心跳、散列乱序/重复/并发 ACK、大窗口、异步超限、缓存隔离、注册/seek、停止重启、通道及拓扑回退。构建使用 Java 8。
本地 HTTP/Netty/Kafka 替身不能证明服务端协议行为；发布前还需真实 DRC/DTS 的单订阅/散列四种组合，覆盖双向切换、数据过期、长事务和故障重放。没有实际执行的线上验证不列为已通过。
