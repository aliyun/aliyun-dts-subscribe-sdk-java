package com.aliyun.dts.subscribe.clients.record;

public enum OperationType {
    INSERT,
    UPDATE,
    DELETE,
    DDL,
    BEGIN,
    COMMIT,
    ROLLBACK,
    ABORT,
    HEARTBEAT,
    CHECKPOINT,
    COMMAND,
    FILL,
    FINISH,
    CONTROL,
    RDB,
    PING,
    NOOP,
    INIT,
    EOF,
    // This type is added for manually generated record to execute for special case when replicate, txn table eg
    MANUAL_GENERATED,
    UNKNOWN,
    SWITCH
}
