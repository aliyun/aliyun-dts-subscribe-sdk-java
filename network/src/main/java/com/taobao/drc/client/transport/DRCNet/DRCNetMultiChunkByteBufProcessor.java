package com.taobao.drc.client.transport.DRCNet;

import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.utils.MessageV1;
import io.netty.buffer.ByteBuf;

/**
 * Created by jianjundeng on 3/5/15.
 */
public class DRCNetMultiChunkByteBufProcessor extends DRCNetChunkByteBufProcessor {

    private Long id;

    public DRCNetMultiChunkByteBufProcessor(UserConfig userConfig) {
        super(userConfig);
        this.id = userConfig.getId();
    }

    public void process(ByteBuf byteBuf) throws Exception {
        while (byteBuf.isReadable()) {
            byteBuf.readBytes(ctrl);
            int length = MessageV1.getHeaderLen(ctrl);
            boolean isBig = MessageV1.isBigMsg(ctrl[0]);

            if (isBig) {
                byteBuf.readUnsignedInt();
            }
            while (length-- > 0) {
                byte b = byteBuf.readByte();
//                sm.offer_data(b);
            }
        }
    }
}
