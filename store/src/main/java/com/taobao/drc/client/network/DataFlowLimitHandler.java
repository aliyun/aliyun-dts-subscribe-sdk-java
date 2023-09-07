package com.taobao.drc.client.network;

import com.google.common.util.concurrent.RateLimiter;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.utils.Constant;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;

public class DataFlowLimitHandler extends ChannelInboundHandlerAdapter {
    private RateLimiter limiter = null;

    public DataFlowLimitHandler() {
    }

    public DataFlowLimitHandler(UserConfig userConfig) {
        this();
        if (userConfig.getDataFlowRpsLimit() > 0) {
            this.limiter = RateLimiter.create(userConfig.getDataFlowRpsLimit());
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (null != limiter) {
            limiter.acquire();
        }
        ctx.fireChannelRead(msg);
    }

    public void channelRead() {
        if (null != limiter) {
            limiter.acquire();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Attribute attr = ctx.channel().attr(Constant.configKey);
        UserConfig userConfig = (UserConfig) attr.get();
        if (userConfig.getDataFlowRpsLimit() > 0) {
            this.limiter = RateLimiter.create(userConfig.getDataFlowRpsLimit());
        }
        ctx.fireChannelActive();
    }
}
