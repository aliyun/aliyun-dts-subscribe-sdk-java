package com.taobao.drc.client.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * Created by jianjundeng on 10/12/14.
 */
public interface StateMachine {

    public void parseByteBuf(ChannelHandlerContext ctx, ByteBuf byteBuf, int length) throws Exception;
}
