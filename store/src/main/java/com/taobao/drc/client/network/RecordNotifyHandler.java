package com.taobao.drc.client.network;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.RecordsCache;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.network.congestion.CongestionController;
import com.taobao.drc.client.utils.Constant;
import com.taobao.drc.client.utils.NetworkConstant;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.beanutils.BeanUtils;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by jianjundeng on 8/4/16.
 */
public class RecordNotifyHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(RecordNotifyHandler.class);

    private Listener messageListener;
    private UserConfig userConfig;

    private final CongestionController congestionController;
    private volatile RecordNotifyHelper helper;
    private volatile CheckpointManager checkpointManager;
    private CheckpointManager coordinator;
    private io.netty.util.concurrent.ScheduledFuture<?> progressCheck;

    public RecordNotifyHandler(CongestionController congestionController) {
        this.congestionController = congestionController;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Record record = (Record) msg;
        if (congestionController.consumed() && !ctx.channel().config().isAutoRead()) {
            log.info("Reading from [" + userConfig.getSubTopic() + "][" + ctx.channel().remoteAddress() + "] resumed, pending records [" + congestionController.pendingNum() + "]");
            ctx.channel().config().setAutoRead(true);
            ctx.read();
        }
        helper.process(record);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        NetworkEndpoint endpoint = ctx.channel().attr(NetworkConstant.networkKey).get();
        if (endpoint != null && endpoint.isClose()) { ctx.close(); return; }
        log.warn("Channel [" + ctx.channel().remoteAddress() + "] is active in thread [" + Thread.currentThread().getId() + "][" + Thread.currentThread().getName() + "]");
        Attribute<Listener> attribute = ctx.channel().attr(NetworkConstant.listenerKey);
        if (attribute != null) {
            messageListener = attribute.get();
        }

        Attribute attr = ctx.channel().attr(Constant.configKey);
        userConfig = (UserConfig) attr.get();
        CheckpointManager registeredCheckpointManager = ctx.channel().attr(NetworkConstant.checkpointManagerKey).get();
        CheckpointManager owner = null != registeredCheckpointManager
                ? registeredCheckpointManager
                : new CheckpointManager(userConfig.isMultiMode());
        coordinator = owner;
        checkpointManager = owner.beginDrcRecovery(userConfig.getCheckpoint().getTimestamp());
        final CheckpointManager attempt = checkpointManager;
        attempt.markConnected();
        attempt.setFailureListener(new Runnable() {
            @Override public void run() {
                getStateChangeListener(ctx).onException(ctx.channel(), attempt.getFailure());
                ctx.close();
            }
        });
        progressCheck = ctx.executor().scheduleAtFixedRate(new Runnable() {
            @Override public void run() { attempt.checkForStalledProgress(); }
        }, 60, 60, TimeUnit.SECONDS);
        // Parsing/cache state belongs to this connection; a replacement must not inherit it.
        userConfig = (UserConfig) BeanUtils.cloneBean(userConfig);
        userConfig.setTxBeginTimestamp(null);
        if (userConfig.getRecordsCache() != null) {
            userConfig.setRecordsCache(new RecordsCache(userConfig.getRecordsCache()));
        }
        helper = new RecordNotifyHelper(userConfig, checkpointManager, messageListener);
        ctx.fireChannelActive();
        getStateChangeListener(ctx).onChannelActive(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (checkpointManager != null) { checkpointManager.invalidate(); }
        if (progressCheck != null) { progressCheck.cancel(false); }
        Set<Channel> channelSet = ctx.channel().attr(Constant.channelSetKey).get();
        channelSet.remove(ctx.channel());
        ctx.channel().close();

        UserConfig channelConfig = ctx.channel().attr(Constant.configKey).get();
        log.info("channel close,subTopic:" + channelConfig.getSubTopic());

        long backOffMs = getStateChangeListener(ctx).onChannelInactive(ctx.channel());
        if (backOffMs >= 0 && !(checkpointManager != null
                && checkpointManager.getFailure() instanceof com.taobao.drc.client.checkpoint.CheckpointTrackingException)) {
            log.warn("Reconnect [" + userConfig.getSubTopic() + "] after [" + backOffMs + "] milliseconds");

            UserConfig userConfig = ctx.channel().attr(Constant.configKey).get();
            ConnectionStateChangeListener stateChangeListener = ctx.channel().attr(NetworkConstant.CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY).get();
            NetworkEndpoint networkEndpoint = ctx.channel().attr(NetworkConstant.networkKey).get();

            ctx.executor().schedule(new ReconnectTask(ctx.executor(), networkEndpoint, userConfig, stateChangeListener, coordinator), backOffMs, TimeUnit.MILLISECONDS);
        } else {
            log.warn("Do not reconnect [" + channelConfig.getSubTopic() + "]");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        log.error("client error:", cause);
        if (checkpointManager != null) { checkpointManager.fail(cause); }
        getStateChangeListener(ctx).onException(ctx.channel(), cause);
        ctx.channel().close();
    }

    private ConnectionStateChangeListener getStateChangeListener(ChannelHandlerContext ctx) {
        return ctx.channel().attr(NetworkConstant.CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY).get();
    }
}
