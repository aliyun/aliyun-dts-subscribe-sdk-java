package com.taobao.drc.client.network;

import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.utils.Constant;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.Attribute;

import java.util.zip.CRC32;

/**
 * Created by jianjundeng on 7/26/16.
 */
public class RecordCRC32Handler extends ChannelInboundHandlerAdapter {

    private CRC32 crc32 = new CRC32();

    private UserConfig userConfig;


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Record record = (Record) msg;
        //crc check
        if (userConfig.isCrc32Check()) {
            long value = record.getCRCValue();
            if (value != 0l) {
                byte[] data=record.getByteArray();
                crc32.update(data, 0, data.length - 4);
                long actual = crc32.getValue();
                crc32.reset();
                if (value != actual) {
                    throw new Exception("crc 32 check failed,expect:" + value + ",actual:" + actual);
                }
            }
        }
        ctx.fireChannelRead(record);
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Attribute attr = ctx.channel().attr(Constant.configKey);
        userConfig = (UserConfig) attr.get();
        ctx.fireChannelActive();
    }
}
