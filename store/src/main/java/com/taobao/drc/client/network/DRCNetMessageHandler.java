package com.taobao.drc.client.network;

import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.transport.DRCNet.DRCNetCompressByteBufProcessor;
import com.taobao.drc.client.utils.Constant;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jianjundeng on 3/3/15.
 */
public class DRCNetMessageHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(DRCNetMessageHandler.class);

    private DRCNetCompressByteBufProcessor byteBufProcessor;

    private UserConfig userConfig;

    public DRCNetMessageHandler() {
        byteBufProcessor = new DRCNetCompressByteBufProcessor();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = ((ByteBuf) msg);
        byteBufProcessor.process(ctx, byteBuf, userConfig);
        byteBuf.release();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Attribute attr = ctx.channel().attr(Constant.configKey);
        userConfig = (UserConfig) attr.get();
        ctx.fireChannelActive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        ctx.channel().close().awaitUninterruptibly();
    }
}
