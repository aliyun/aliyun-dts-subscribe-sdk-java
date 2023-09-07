package com.taobao.drc.client.utils;

import com.taobao.drc.client.message.ByteString;
import com.taobao.drc.client.protocol.binary.DataType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static com.taobao.drc.client.protocol.binary.DataType.*;

/**
 * Created by jianjundeng on 4/19/15.
 */
public class BinaryMessageUtils {

    private static final int PREFIX_LENGTH=12;

    private static final int[] elementArray = {0, 1, 1, 2, 2, 4, 4, 8, 8};

    /**
     * 从offset取string
     *
     * @param offset
     * @param charset
     * @return
     */
    public static String getString(byte[] data, int offset, Charset charset) {
        ByteBuf wrapByteBuf = Unpooled.wrappedBuffer(data).order(ByteOrder.LITTLE_ENDIAN);
        wrapByteBuf.readerIndex(PREFIX_LENGTH + offset);
        byte t = wrapByteBuf.readByte();
        if ((t & DC_ARRAY) != 0 || (t & DC_NULL) != 0) {
            return null;
        }
        int length = (int) wrapByteBuf.readUnsignedInt();
        return new String(wrapByteBuf.array(), PREFIX_LENGTH + 5 + offset, length - 1, charset);
    }

    /**
     * 获得offset list
     *
     * @param offset
     * @return
     * @throws IOException
     */
    public static List getArray(byte[] data,int offset) throws IOException {
        ByteBuf wrapByteBuf = Unpooled.wrappedBuffer(data).order(ByteOrder.LITTLE_ENDIAN);
        wrapByteBuf.readerIndex(PREFIX_LENGTH + offset);
        byte t = wrapByteBuf.readByte();
        if ((t & DataType.DC_ARRAY) == 0) {
            return null;
        }
        int count = (int) wrapByteBuf.readUnsignedInt();
        if (count == 0) {
            return null;
        }
        List lists = new ArrayList(count);
        int elementSize = elementArray[t & DataType.DT_MASK];
        for (int i = 0; i < count; i++) {
            switch (elementSize) {
                case 1:
                    lists.add((int) wrapByteBuf.readUnsignedByte());
                    break;
                case 2:
                    lists.add(wrapByteBuf.readUnsignedShort());
                    break;
                case 4:
                    lists.add((int) wrapByteBuf.readUnsignedInt());
                    break;
                case 8:
                    lists.add((int) wrapByteBuf.readLong());
                    break;
            }
        }
        return lists;
    }

    /**
     * 从offset处获得string list
     *
     * @param offset
     * @return
     * @throws Exception
     */
    public static List<ByteString> getByteStringList(byte[] data,long offset) throws Exception {
        if(offset == -1) {
            return null;
        }
        ByteBuf wrapByteBuf = Unpooled.wrappedBuffer(data).order(ByteOrder.LITTLE_ENDIAN);
        wrapByteBuf.readerIndex((int) (PREFIX_LENGTH + offset));
        byte t = wrapByteBuf.readByte();
        if ((t & DC_ARRAY) == 0 || (t & DT_MASK) != DT_STRING) {
            throw new Exception("Data type not array or not string");
        }
        int count = wrapByteBuf.readInt();
        if (count == 0) {
            return null;
        }
        int readBytes = 5;
        readBytes += (count + 1) * 4;
        List<ByteString> lists = new ArrayList<ByteString>(count);
        int currentOffset = (int) wrapByteBuf.readUnsignedInt();
        int nextOffset;
        for (int i = 0; i < count; i++) {
            nextOffset = (int) wrapByteBuf.readUnsignedInt();
            if (nextOffset == currentOffset) {
                lists.add(null);
            } else {
                lists.add(new ByteString(wrapByteBuf.array(), PREFIX_LENGTH +
                        currentOffset + readBytes + (int) offset, nextOffset - currentOffset - 1));
            }
            currentOffset = nextOffset;
        }
        return lists;
    }
}
