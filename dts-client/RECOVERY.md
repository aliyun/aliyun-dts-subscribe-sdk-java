# DTS/DRC 恢复位点（1.1.21）

完整使用约束与迁移说明见 [Client 1.1.21](../docs/CLIENT_1.1.21.md)，设计依据见 [兼容性设计](../docs/superpowers/specs/2026-10-30-ack-compatibility-design.md)。

- `startService()`：DRC 和 DTS 都在整个 notify 成功返回后确认；显式 ACK 是安全无操作。不能仅异步入队就返回。
- `startMultiService()`：DRC 和 DTS 都要求原始 Record 逐条成功后 ACK，包括实际交付的心跳和事务标记；只推进连续前缀。
- DRC 按合格心跳恢复；DTS 按事务安全边界恢复。缓存中未交付的心跳和最新 received 时间不作为已完成证明。
- DTS 空 poll 不重建消费者。明确的集群切换、OffsetOutOfRange 和其他失败交给根协调器，统一隔离分片并重新注册、seek。CM 注册与 seek 复用同一冻结位置。
- 无已确认边界时使用初始起点；数据已过保留期报错。普通同分片重连不重置到最初时间。
- 通道或分片拓扑改变且映射未验证时，显著告警并回退原始可迁移时间戳；缺少时间戳则终止。
- 无固定 10240 ACK 上限。可选区间预算超限会停止订阅、保留错误和最后安全位置，不继续投递。

`getCheckpointStatus()` 与 `getLastFailure()` 在 stop/join 后仍可查询；旧手册的 join/reset/start 循环保留确认状态。进程重启仍需业务持久化起点并处理可能重放的数据。

本地验证命令：`mvn -o test`、`mvn -o package`（Java 8）。真实服务端验收另行执行。
