# ACK 与恢复位点设计（1.1.21）

本次实现采用已经确认的 [兼容性设计](../docs/superpowers/specs/2026-10-30-ack-compatibility-design.md)，基线为 1.1.20（`6e2e555`）。执行记录见 [实施计划](../docs/superpowers/plans/2026-10-30-client-1.1.21.md)，用户说明见 [1.1.21 升级说明](../docs/CLIENT_1.1.21.md)。

最终规则是不增加 ACK 模式参数：单订阅整个回调成功返回即完成，散列订阅逐条 ACK；DRC/DTS 一致。本文之前记录的“DTS 单订阅仍依赖逐条 ACK”“区间超限后冻结但继续投递”不再是本次实现。

| 原反馈 | 处理与兼容考虑 |
|---|---|
| DRC 单订阅 ACK 必然 NPE | 不注册 ACK 凭证，ACK 安全无操作；无 ACK 的旧同步回调继续推进合格心跳 |
| 基类重复 ACK 错链、回退 | 保留旧方法签名，增加所有权、幂等和捕获边界保护；实际两通道采用同一代次凭证与区间算法 |
| DTS 单订阅 10240 pending 后静默冻结 | 单订阅按批次确认，不建显式 ACK pending；取消固定 10240 上限 |
| 散列订阅未 ACK/乱序 | 保留逐条 ACK 契约；不越过缺口，所有交付类型都需确认；固定 60 秒停滞提醒 |
| 可选资源预算超限 | ERROR + 主动停止 + 专用异常；可查询状态，禁止静默继续消费 |
| 异常和重连 | 原异常向上传播；代次隔离、缓存隔离、多分片协调关闭、恢复位置保持 |
| 旧手册外层重试 | 修复并发 start、stop/join、reset；已确认状态和最终错误在停止后保留 |
| 通道/拓扑切换 | 无验证映射时回退不可变原始时间戳，缺少可迁移起点时报错 |

实现入口：`CheckpointManager`、`RecordNotifyHelper/Handler`、`DRCStoreClientImpl`、`DStoreClientImpl`、`BaseDStoreConsumer`、`DRCClientImpl`。Java 8 与既有 API 方法签名保留，Record 的旧序列化 UID 固定，新增确认凭证为 transient。

历史 DRC 同步单订阅及散列订阅优先兼容。1.1.19/1.1.20 的单订阅“仅入队返回、后台 ACK”不作为兼容目标，必须按升级说明迁移。不能承诺所有旧用法零调整、无重放或跨服务端协议已经验收。
