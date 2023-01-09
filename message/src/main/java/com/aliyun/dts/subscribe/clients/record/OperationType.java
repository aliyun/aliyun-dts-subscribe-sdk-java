package com.aliyun.dts.subscribe.clients.record;

public enum OperationType {
    INSERT,
    UPDATE,
    DELETE,
    DDL,
    BEGIN,
    COMMIT,
    HEARTBEAT,
    CHECKPOINT,
    UNKNOWN;
}
