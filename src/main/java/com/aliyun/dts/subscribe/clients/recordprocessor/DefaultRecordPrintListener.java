package com.aliyun.dts.subscribe.clients.recordprocessor;

import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.record.RecordSchema;
import com.aliyun.dts.subscribe.clients.record.UserRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRecordPrintListener implements RecordListener {
    private static final Logger log = LoggerFactory.getLogger(DefaultRecordPrintListener.class);

    public DefaultRecordPrintListener(DbType mySQL) {
    }

    @Override
    public void consume(UserRecord record) {

        OperationType operationType = record.getOperationType();

        RecordSchema recordSchema = null;
        if (!operationType.equals(OperationType.HEARTBEAT)) {
            recordSchema = record.getSchema();
        }

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder
                .append("\n")
                // record id can not be used as unique identifier
                .append("RecordID [").append(record.getId()).append("]\n")
                // record generate timestamp in source log
                .append("RecordTimestamp [").append(record.getSourceTimestamp()).append("] \n")
                // source info contains which source this record came from
                .append("Source [").append(recordSchema == null? "null" :recordSchema.getDatabaseInfo()).append("]\n")
                // record type
                .append("RecordType [").append(record.getOperationType()).append("]\n");

        if (operationType.equals(OperationType.INSERT)
                || operationType.equals(OperationType.UPDATE)
                || operationType.equals(OperationType.DELETE)
                || operationType.equals(OperationType.DDL)) {

            stringBuilder
                    .append("Schema info [").append(recordSchema.toString()).append("]\n")
                    //before image
                    .append("Before image {").append(record.getBeforeImage()).append("}\n")
                    //after image
                    .append("After image {").append(record.getAfterImage()).append("}\n");
        }

        log.info(stringBuilder.toString());
    }
}
