package com.taobao.drc.client.protocol.binary;

import com.taobao.drc.client.message.drc.BinaryRecord;
import com.taobao.drc.client.protocol.StateMachine;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Created by jianjundeng on 10/12/14.
 */
public class BinaryStateMachine implements StateMachine {

    private static final int MESSAGE_LENGTH_THRESHOLD = 1024 * 1024 * 128;

    private long messageSize;

    private int offset;

    private int lastBytes;

    private byte[] data;

    private Status status = Status.READ_MESSAGE_HEADER;

    private ByteBuf headByteBuffer = Unpooled.buffer(12);

    public void parseByteBuf(ChannelHandlerContext ctx, ByteBuf byteBuf, int length) throws Exception {
        while (length>0) {
            switch (status) {
                case READ_MESSAGE_HEADER:
                    int readSize = Math.min(headByteBuffer.writableBytes(), byteBuf.readableBytes());
                    byteBuf.readBytes(headByteBuffer, readSize);
                    if (!headByteBuffer.isReadable(12)) {
                        return;
                    }
                    //omit message type and message version
                    ByteBuf handleBuff = headByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    handleBuff.readerIndex(4);
                    messageSize = handleBuff.readUnsignedInt();
                    if (messageSize <= 4 || messageSize > MESSAGE_LENGTH_THRESHOLD)
                        throw new IOException("Header shows message size " + messageSize + " less than 4 or bigger then 100m");
                    messageSize -= 4;
                    data = new byte[(int) (messageSize + 12)];
                    offset = 12;
                    lastBytes = (int) messageSize;
                    status = Status.READ_MESSAGE_DATA;
                    length = length - readSize;
                    headByteBuffer.clear();
                    break;
                case READ_MESSAGE_DATA:
                    if (length >= lastBytes) {
                        length = length - lastBytes;
                        byteBuf.readBytes(data, offset, lastBytes);
                        status = Status.READ_MESSAGE_HEADER;
                        BinaryRecord binaryRecord = new BinaryRecord();
                        ByteBuf inner = Unpooled.wrappedBuffer(data, 0, data.length).order(ByteOrder.LITTLE_ENDIAN);
                        binaryRecord.setByteBuf(inner);
                        binaryRecord.setRecordLength(data.length);
                        ctx.fireChannelRead(binaryRecord);
                    } else {
                        byteBuf.readBytes(data, offset, length);
                        offset += length;
                        lastBytes -= length;
                        length=0;
                    }
                    break;
            }
        }
    }

    public enum Status {

        READ_MESSAGE_HEADER,

        READ_MESSAGE_DATA,
    }
}
