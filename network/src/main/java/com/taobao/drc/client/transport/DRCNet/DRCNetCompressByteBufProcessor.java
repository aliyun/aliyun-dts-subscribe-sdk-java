package com.taobao.drc.client.transport.DRCNet;

import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.transport.enums.DRCNetCompressState;
import com.taobao.drc.client.utils.MessageV1;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.nio.charset.Charset;

import static com.taobao.drc.client.transport.enums.DRCNetCompressState.*;

/**
 * Created by jianjundeng on 3/3/15.
 */
public class DRCNetCompressByteBufProcessor {

    private static final Logger log = LoggerFactory.getLogger(DRCNetCompressByteBufProcessor.class);

    private static final int MAGIC_NUMBER_SIZE = 25;

    private static final String MAGIC_NUMBER_STR = "kjnkjabhbanc283ubcsbhdc72";

    private static final byte ASK_ID = 1;

    private static final byte START_CONNECTION = 2;

    private static final byte SINGLE_CONNECTION = 4;

    private static final byte MULTI_CONNECTION = 5;

    private static final byte ACCEPT_CONNECTION = 'a';

    private DRCNetCompressState state = MAGIC_NUMBER;

    private byte[] raw_bb;

    private int raw_index;

    private byte[] compress_bb;

    private int compress_index;

    private boolean compress;

    private long rawLeft;

    private long srcLength;

    private LZ4FastDecompressor deCompressor = LZ4Factory.fastestInstance().fastDecompressor();

    private LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

    private long id;

    private byte[] controlArray=new byte[128];

    private int controlIndex=0;

    private int serverVersion;

    private DRCNetChunkByteBufProcessor drcNetChunkByteBufProcessor;


    public void process(ChannelHandlerContext ctx, ByteBuf byteBuf, UserConfig userConfig) throws Exception {
        while (byteBuf.isReadable()) {
            switch (state) {
                case MAGIC_NUMBER:
                    if(controlIndex<MAGIC_NUMBER_SIZE) {
                        controlArray[controlIndex++] = byteBuf.readByte();
                        if (controlIndex == MAGIC_NUMBER_SIZE) {
                            String magicNumber = new String(controlArray, 0, controlIndex, Charset.defaultCharset());
                            if (!magicNumber.equals(MAGIC_NUMBER_STR)) {
                                throw new Exception("magic number not match");
                            }
                            controlIndex = 0;
                            state = SERVER_VERSION;
                        }
                    }
                    break;
                case SERVER_VERSION:
                    if(controlIndex<4) {
                        controlArray[controlIndex++] = byteBuf.readByte();
                        if(controlIndex==4) {
                            serverVersion = MessageV1.getHeaderLen(controlArray);
                            controlIndex = 0;
                            ByteBuf temp = Unpooled.buffer();
                            temp.writeByte(ASK_ID);
                            ctx.channel().writeAndFlush(temp);
                            state = GET_IDENTIFY;
                        }
                    }
                    break;
                case GET_IDENTIFY:
                    if ((serverVersion==1&&controlIndex < 4)||(serverVersion==2&&controlIndex<8)) {
                        controlArray[controlIndex++] = byteBuf.readByte();
                        if((serverVersion==1&&controlIndex==4)||(serverVersion==2&&controlIndex==8)) {
                            long size=0;
                            if(serverVersion==1){
                                size=1024*1024*32;
                            }else if(serverVersion==2){
                                size= MessageV1.getInt32(controlArray, 4);
                                log.info("drc net assign raw buffer size:"+size);
                            }
                            raw_bb=new byte[(int) size];
                            size=((compressor.maxCompressedLength((int) size) + 1023 + 9) / 1024) * 1024;
                            compress_bb=new byte[(int) size];
                            id = MessageV1.getHeaderLen(controlArray);
                            controlIndex = 0;
                            userConfig.setId(id);
                            ByteBuf temp = Unpooled.buffer();
                            sendUserParam(temp, ctx, userConfig);
                            state = GET_HANDSHAKE;
                        }
                    }
                    break;
                case GET_HANDSHAKE:
                    if (byteBuf.readByte() == ACCEPT_CONNECTION) {
                        state = READ_COMPRESS_FLAG;
                    } else {
                        throw new Exception();
                    }
                    break;
                case READ_COMPRESS_FLAG:
                    short flag = byteBuf.readUnsignedByte();
                    if (flag == 129) {
                        compress = true;
                    } else {
                        compress = false;
                    }
                    state = READ_RAW_DATA_LENGTH;
                    break;
                case READ_RAW_DATA_LENGTH:
                    if (controlIndex < 4) {
                        controlArray[controlIndex++] = byteBuf.readByte();
                        if(controlIndex==4) {
                            controlIndex = 0;
                            rawLeft = MessageV1.getInt32(controlArray, 0);
                            if (compress) {
                                state = READ_COMPRESS_RAW_DATA_LENGTH;
                            } else {
                                state = READ_RAW_DATA;
                            }
                        }
                    }
                    break;
                case READ_COMPRESS_RAW_DATA_LENGTH:
                    if (controlIndex < 4) {
                        controlArray[controlIndex++] = byteBuf.readByte();
                        if(controlIndex==4) {
                            controlIndex = 0;
                            srcLength = MessageV1.getInt32(controlArray, 0);
                            rawLeft -= 4;
                            state = READ_COMPRESS_DATA;
                        }
                    }
                    break;
                case READ_COMPRESS_DATA:
                    int available = byteBuf.readableBytes();
                    if (rawLeft < available) {
                        byteBuf.readBytes(compress_bb, compress_index, (int) rawLeft);
                        compress_index += rawLeft;
                        rawLeft = 0;
                    } else {
                        byteBuf.readBytes(compress_bb, compress_index, available);
                        compress_index += available;
                        rawLeft -= available;
                    }
                    if (rawLeft == 0) {
                        deCompressor.decompress(compress_bb, 0, raw_bb, 0, (int) srcLength);
                        drcNetChunkByteBufProcessor.process(ctx, Unpooled.wrappedBuffer(raw_bb, 0, (int) srcLength).order(ByteOrder.LITTLE_ENDIAN));
                        state = READ_COMPRESS_FLAG;
                        compress_index = 0;
                        raw_index = 0;
                    }
                    break;
                case READ_RAW_DATA:
                    available = byteBuf.readableBytes();
                    if (rawLeft < available) {
                        byteBuf.readBytes(raw_bb, raw_index, (int) rawLeft);
                        raw_index += rawLeft;
                        rawLeft = 0;
                    } else {
                        byteBuf.readBytes(raw_bb, raw_index, available);
                        raw_index += available;
                        rawLeft -= available;
                    }
                    if (rawLeft == 0) {
                        drcNetChunkByteBufProcessor.process(ctx, Unpooled.wrappedBuffer(raw_bb, 0, raw_index).order(ByteOrder.LITTLE_ENDIAN));
                        state = READ_COMPRESS_FLAG;
                        compress_index = 0;
                        raw_index = 0;
                    }
                    break;
            }
        }
    }

    private void sendUserParam(ByteBuf temp, ChannelHandlerContext ctx, UserConfig userConfig) throws Exception {
        temp.writeByte(START_CONNECTION);
//        if (userConfig.isMultiConn()) {
//            temp.writeByte(MULTI_CONNECTION);
//            drcNetChunkByteBufProcessor = new DRCNetMultiChunkByteBufProcessor(userConfig);
//        } else {
            temp.writeByte(SINGLE_CONNECTION);
            drcNetChunkByteBufProcessor = new DRCNetChunkByteBufProcessor(userConfig);
//        }
        temp.writeInt((int) id);
        String urlString = userConfig.toUrlString();
        byte[] data = urlString.getBytes();
        temp.writeInt(data.length);
        temp.writeBytes(data);
        ctx.channel().writeAndFlush(temp);
    }
}
