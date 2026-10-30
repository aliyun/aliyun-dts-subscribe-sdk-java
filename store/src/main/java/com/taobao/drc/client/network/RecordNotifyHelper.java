package com.taobao.drc.client.network;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.impl.LocalityFile;
import com.taobao.drc.client.impl.RecordsCache;
import com.taobao.drc.client.impl.ListenerContext;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.message.drc.BinaryRecord;
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
        if (!checkpointManager.isActive()) {
            return;
        }
        Record record= (Record) msg;
        checkpointManager.recordReceived(record);
        //update checkpoint&meta cache
        String tm = record.getTimestamp();
        switch (record.getOpt()) {
            case BEGIN:
                userConfig.setTxBeginTimestamp(tm);
                inTransaction=true;
                break;
            case COMMIT:
            case ROLLBACK:
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
            CheckpointManager.Delivery delivery = checkpointManager.beginDelivery(dataMessage.getRecordList());
            if (!checkpointManager.isActive()) {
                return;
            }
            ListenerContext.enter();
            try {
                messageListener.notify(dataMessage);
            } catch (Exception e) {
                checkpointManager.fail(e);
                log.error("Caught exception from notify method of [" + userConfig.getSubTopic() + "]", e);
                try { messageListener.handleException(e); }
                catch (Throwable handlerFailure) { if (handlerFailure != e) { e.addSuppressed(handlerFailure); } }
                throw e;
            } catch (Error error) {
                checkpointManager.fail(error);
                throw error;
            } finally {
                ListenerContext.exit();
            }
            checkpointManager.completeDelivery(delivery);
        }
        if (!checkpointManager.isActive()) {
            return;
        }
        // Only a successfully delivered/confirmed heartbeat is a recovery candidate.
        final Checkpoint checkpoint = userConfig.getCheckpoint();
        checkpointManager.runIfActive(new Runnable() {
            @Override public void run() {
                String heartbeat = checkpointManager.getConfirmedHeartbeat();
                if (heartbeat != null) { checkpoint.setTimestamp(heartbeat); }
            }
        });

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
