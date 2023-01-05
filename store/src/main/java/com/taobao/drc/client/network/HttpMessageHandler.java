package com.taobao.drc.client.network;

import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.protocol.StateMachine;
import com.taobao.drc.client.protocol.binary.BinaryStateMachine;
import com.taobao.drc.client.protocol.text.TextStateMachine;
import com.taobao.drc.client.utils.Constant;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.util.Attribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteOrder;

/**
 * Created by jianjundeng on 10/2/14.
 */
public class HttpMessageHandler extends ChannelInboundHandlerAdapter {

    private static final Log log = LogFactory.getLog(HttpMessageHandler.class);

    private StateMachine stateMachine;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DefaultHttpContent) {
            HttpContent httpContent = (HttpContent) msg;
            ByteBuf byteBuf = httpContent.content();
            stateMachine.parseByteBuf(ctx, byteBuf.order(ByteOrder.LITTLE_ENDIAN), byteBuf.readableBytes());
            httpContent.release();
        }else if(msg.equals(DefaultLastHttpContent.EMPTY_LAST_CONTENT)){
            log.warn("server send last empty response,re connect");
            ctx.close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Attribute attr = ctx.channel().attr(Constant.configKey);
        UserConfig userConfig = (UserConfig) attr.get();
        if (userConfig == null) {
            log.error("user config is null");
        } else {
            switch (userConfig.getMessageType()) {
                case BINARY:
                    stateMachine = new BinaryStateMachine();
                    break;
                case TEXT:
                    stateMachine = new TextStateMachine();
                    break;
            }
        }
        ctx.fireChannelActive();
    }
}
