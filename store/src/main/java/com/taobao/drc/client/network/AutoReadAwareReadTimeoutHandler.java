package com.taobao.drc.client.network;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author yangyang
 * @since 17/5/31
 */
public class AutoReadAwareReadTimeoutHandler extends IdleStateHandler {
    private static final Log logger = LogFactory.getLog(AutoReadAwareReadTimeoutHandler.class);
    public static final int DEFAULT_READER_IDLE_TIME_SECONDS = 3;

    private final long readTimeoutNanos;
    private volatile boolean inRead = false;
    private volatile long lastReadEventIssued = System.nanoTime();

    public AutoReadAwareReadTimeoutHandler(int readTimeoutSeconds) {
        super(DEFAULT_READER_IDLE_TIME_SECONDS, 0, 0);
        logger.info("Read timeout: [" + readTimeoutSeconds + "] seconds");
        this.readTimeoutNanos = TimeUnit.SECONDS.toNanos(readTimeoutSeconds);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        super.read(ctx);
        if (readTimeoutNanos > 0) {
            inRead = true;
            lastReadEventIssued = System.nanoTime();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        if (readTimeoutNanos > 0) {
            lastReadEventIssued = System.nanoTime();
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        if (readTimeoutNanos > 0) {
            inRead = false;
            lastReadEventIssued = System.nanoTime();
        }
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
        long now = System.nanoTime();
        if (inRead && now - lastReadEventIssued > readTimeoutNanos) {
            logger.error("No reads on channel [" + ctx.channel().remoteAddress() + "] for" + " [" + TimeUnit.NANOSECONDS.toMillis(now - lastReadEventIssued) + "] ms, close the channel");
            ctx.channel().close();
        } else {
            logger.warn("No reads on channel [" + ctx.channel().remoteAddress() + "] for" + " [" + TimeUnit.NANOSECONDS.toMillis(now - lastReadEventIssued) + "] ms");
        }
    }
}
