package com.taobao.drc.client.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This handler is used to encode and write user control messages. Writes before the protocol handshake is over will
 * be reject to prevent messing of the protocol.
 *
 * @author yangyang
 * @since 17/1/20
 */
public class UserCtrlMsgHandler extends ChannelDuplexHandler {
    private static final Log logger = LogFactory.getLog(UserCtrlMsgHandler.class);

    private static final byte USER_CTL_MESSAGE = 6;
    private static final int USER_CTL_MESSAGE_HEADER_LENGTH = 5;

    private boolean firstRecordRead = false;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof UserCtrlMessage) {
            if (!firstRecordRead) {
                promise.setFailure(new IllegalStateException("Channel not ready yet"));
                return;
            }
            writePendingMsg(ctx, (UserCtrlMessage) msg, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!firstRecordRead) {
            firstRecordRead = true;
            logger.info("First record received in connection to [" + ctx.channel().remoteAddress() + "], write enabled");
        }
        ctx.fireChannelRead(msg);
    }

    private void writePendingMsg(ChannelHandlerContext ctx, UserCtrlMessage msg, ChannelPromise promise) {
        ByteBuf byteBuf = ctx.alloc().buffer(USER_CTL_MESSAGE_HEADER_LENGTH + msg.getContent().length);
        byteBuf.writeByte(USER_CTL_MESSAGE);
        byteBuf.writeInt(msg.getContent().length);
        byteBuf.writeBytes(msg.getContent());
        ctx.writeAndFlush(byteBuf, promise);
    }
}
