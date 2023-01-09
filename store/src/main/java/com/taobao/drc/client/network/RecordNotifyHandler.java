package com.taobao.drc.client.network;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.network.congestion.CongestionController;
import com.taobao.drc.client.utils.Constant;
import com.taobao.drc.client.utils.NetworkConstant;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by jianjundeng on 8/4/16.
 */
public class RecordNotifyHandler extends ChannelInboundHandlerAdapter {

    private static final Log log = LogFactory.getLog(RecordNotifyHandler.class);

    private Listener messageListener;
    private UserConfig userConfig;

    private final CongestionController congestionController;
    private volatile RecordNotifyHelper helper;

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
        log.warn("Channel [" + ctx.channel().remoteAddress() + "] is active in thread [" + Thread.currentThread().getId() + "][" + Thread.currentThread().getName() + "]");
        Attribute<Listener> attribute = ctx.channel().attr(NetworkConstant.listenerKey);
        if (attribute != null) {
            messageListener = attribute.get();
        }

        Attribute attr = ctx.channel().attr(Constant.configKey);
        userConfig = (UserConfig) attr.get();
        helper = new RecordNotifyHelper(userConfig, new CheckpointManager(), messageListener);
        ctx.fireChannelActive();
        getStateChangeListener(ctx).onChannelActive(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Set<Channel> channelSet = ctx.channel().attr(Constant.channelSetKey).get();
        channelSet.remove(ctx.channel());
        ctx.channel().close();

        log.info("channel close,subTopic:" + userConfig.getSubTopic());

        long backOffMs = getStateChangeListener(ctx).onChannelInactive(ctx.channel());
        if (backOffMs >= 0) {
            log.warn("Reconnect [" + userConfig.getSubTopic() + "] after [" + backOffMs + "] milliseconds");

            UserConfig userConfig = ctx.channel().attr(Constant.configKey).get();
            ConnectionStateChangeListener stateChangeListener = ctx.channel().attr(NetworkConstant.CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY).get();
            NetworkEndpoint networkEndpoint = ctx.channel().attr(NetworkConstant.networkKey).get();

            ctx.executor().schedule(new ReconnectTask(ctx.executor(), networkEndpoint, userConfig, stateChangeListener), backOffMs, TimeUnit.MILLISECONDS);
        } else {
            log.warn("Do not reconnect [" + userConfig.getSubTopic() + "]");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        log.error("client error:", cause);
        ctx.channel().close();
        getStateChangeListener(ctx).onException(ctx.channel(), cause);
    }

    private ConnectionStateChangeListener getStateChangeListener(ChannelHandlerContext ctx) {
        return ctx.channel().attr(NetworkConstant.CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY).get();
    }
}
