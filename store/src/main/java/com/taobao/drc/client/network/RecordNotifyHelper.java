package com.taobao.drc.client.network;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.impl.LocalityFile;
import com.taobao.drc.client.impl.RecordsCache;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.message.DataMessage.Record.Type;
import com.taobao.drc.client.message.drc.BinaryRecord;
import com.taobao.drc.client.message.dts.DStoreAvroRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.taobao.drc.client.utils.Constant.POSITION_INFO;

public class RecordNotifyHelper {
    private static final Logger log = LoggerFactory.getLogger(RecordNotifyHelper.class);

    private Listener messageListener;

    private long recordCheckpointTime;

    private boolean inTransaction=false;

    private UserConfig userConfig;

    private CheckpointManager checkpointManager;

    public RecordNotifyHelper(UserConfig userConfig, CheckpointManager checkpointManager, Listener messageListener) {
        this.userConfig = userConfig;
        this.checkpointManager = checkpointManager;
        this.messageListener = messageListener;
    }

    public void process(Object msg) throws Exception {
        Record record= (Record) msg;
        //update checkpoint&meta cache
        String tm = record.getTimestamp();
        switch (record.getOpt()) {
            case BEGIN:
                userConfig.setTxBeginTimestamp(tm);
                inTransaction=true;
                break;
            case COMMIT:
                inTransaction=false;
                break;
        }
        //set begin ts/heartbeat ts
        switch (record.getDbType()){
            case OCEANBASE1:
                if (record instanceof BinaryRecord) {
                    record.setSafeTimestamp(Long.toString(((BinaryRecord) record).getFileNameOffset()));
                } else {
                    record.setSafeTimestamp(record.getCheckpoint().substring(2));
                }
                break;
            default:
                if(inTransaction) {
                    record.setSafeTimestamp(userConfig.getTxBeginTimestamp());
                }else{
                    record.setSafeTimestamp(record.getTimestamp());
                }
                break;
        }

        //set user config
        record.setCheckpointManager(checkpointManager);

        RecordsCache recordsCache = userConfig.getRecordsCache();
        DataMessage dataMessage = new DataMessage();
        if (recordsCache != null) {
            recordsCache.addRecord(record);
            if (recordsCache.isReady()) {
                dataMessage = recordsCache.getReadyRecords();
            }
        } else {
            dataMessage.addRecord(record);
        }
        if(dataMessage.getRecordCount()>0){
            for(Record r:dataMessage.getRecordList()){
                if(checkpointManager.isMultiMode()) {
                    //add double link list
                    checkpointManager.addRecord(r);
                }
            }
            try {
                messageListener.notify(dataMessage);
            } catch (Exception e) {
                log.error("Caught exception from notify method of [" + userConfig.getSubTopic() + "]", e);
                messageListener.handleException(e);
            }
        }
        //re connect use heartbeat
        Checkpoint checkpoint = userConfig.getCheckpoint();
        if(record.getOpt()== Type.HEARTBEAT) {
            checkpoint.setTimestamp(tm);
            if (record instanceof DStoreAvroRecord) {
                checkpoint.setOffset(((DStoreAvroRecord) record).getOffset());
            }
        }

        //persist not safe,deprecated
        persistCheckpoint(userConfig, checkpoint);
    }

    private void persistCheckpoint(UserConfig userConfig, Checkpoint checkpoint) throws IOException {
        LocalityFile localityFile = userConfig.getLocalityFile();
        if(localityFile==null){
            return;
        }
        long checkpointPeriod = userConfig.getPersistPeriod();
        if (recordCheckpointTime >= checkpointPeriod) {
            final StringBuilder pb = new StringBuilder();
            pb.append(POSITION_INFO);
            pb.append(checkpoint.toString());
            localityFile.writeLine(pb.toString());
            recordCheckpointTime = 0;
        } else {
            recordCheckpointTime++;
        }
    }
}
