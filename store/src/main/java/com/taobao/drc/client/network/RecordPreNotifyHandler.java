package com.taobao.drc.client.network;

import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.network.congestion.CongestionController;
import com.taobao.drc.client.utils.Constant;
import com.taobao.drc.client.utils.NetworkConstant;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by jianjundeng on 10/2/14.
 */
public class RecordPreNotifyHandler extends ChannelInboundHandlerAdapter {
    private static final Long PERIOD_OUTPUT_PENDING_SIZE = 10000L;

    private static final Log log = LogFactory.getLog(RecordPreNotifyHandler.class);

    private boolean first = true;

    private UserConfig userConfig;

    private final CongestionController congestionController;
    private ScheduledFuture statsFuture;
    private volatile String subTopicName;
    private volatile String remoteAddress;
    private volatile long notifiedRecordCount;
    private volatile RecordPreNotifyHelper helper;

    public RecordPreNotifyHandler(CongestionController congestionController) {
        this.congestionController = congestionController;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Record record = (Record) msg;
        record.setChannelFirstRecord(first);
        helper.process(record);
        // make sure there is at least one record to fire after the auto read flag has been set to false,
        // so there is always a chance for the auto read flag to be set to true again in the notify handler.
        if (congestionController.produced() && ctx.channel().config().isAutoRead()) {
            log.info("Reading from [" + subTopicName + "][" + remoteAddress + "] stopped, pending records [" + congestionController.pendingNum() + "]");
            ctx.channel().config().setAutoRead(false);
        }

        //notify
        ctx.fireChannelRead(record);
        notifiedRecordCount++;
        if (first) {
            first = false;
            ctx.channel().attr(NetworkConstant.CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY).get()
                    .onFirstRecord(ctx.channel());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Attribute attr = ctx.channel().attr(Constant.configKey);
        userConfig = (UserConfig) attr.get();
        ctx.fireChannelActive();

        subTopicName = userConfig.getSubTopic();
        remoteAddress = ctx.channel().remoteAddress().toString();
        initOutputStatSchedule(ctx.executor());
        notifiedRecordCount = 0;
        helper = new RecordPreNotifyHelper(userConfig);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelOutputStatSchedule();
        super.channelInactive(ctx);
    }

    private void initOutputStatSchedule(EventExecutor executor) {
        if (statsFuture != null) {
            statsFuture.cancel(false);
        }
        statsFuture = executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (congestionController != null) {
                    log.info("Pending records from [" + subTopicName + "][" + remoteAddress + "]: [" + congestionController.pendingNum() + "], notified [" + notifiedRecordCount + "] records");
                }
            }
        }, PERIOD_OUTPUT_PENDING_SIZE, PERIOD_OUTPUT_PENDING_SIZE, TimeUnit.MILLISECONDS);
    }

    private void cancelOutputStatSchedule() {
        if (statsFuture != null) {
            statsFuture.cancel(false);
        }
        statsFuture = null;
    }
}
