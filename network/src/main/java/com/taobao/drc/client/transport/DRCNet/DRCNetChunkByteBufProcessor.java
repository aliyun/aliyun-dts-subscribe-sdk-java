package com.taobao.drc.client.transport.DRCNet;

import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.protocol.StateMachine;
import com.taobao.drc.client.protocol.binary.BinaryStateMachine;
import com.taobao.drc.client.protocol.text.TextStateMachine;
import com.taobao.drc.client.utils.MessageV1;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by jianjundeng on 3/3/15.
 */
public class DRCNetChunkByteBufProcessor {

    protected static final Log log = LogFactory.getLog(DRCNetChunkByteBufProcessor.class);
    private static final int ERROR_HEADER_LENGTH = 8;

    protected byte[] ctrl = new byte[4];

    protected byte[] bigCtrl=new byte[4];

    protected StateMachine sm;

    public DRCNetChunkByteBufProcessor(UserConfig userConfig) {
        switch (userConfig.getMessageType()) {
            case TEXT:
                sm = new TextStateMachine();
                break;
            case BINARY:
                sm = new BinaryStateMachine();
                break;
        }
    }

    private String retrieveErrMsgFromByteBuffer(ByteBuf byteBuffer) {
        byteBuffer.readerIndex(0);
        int byteBufferLength = byteBuffer.readableBytes();
        if (byteBufferLength <= 16) {
            return "Connect drc store failed with unknown error";
        }
        byteBuffer.readerIndex(12);
        byte[] errorMessageLengthBytes = new byte[4];
        byteBuffer.readBytes(errorMessageLengthBytes);
        int errorMessageLength  = MessageV1.getHeaderLen(errorMessageLengthBytes);
        int canReadBytes = byteBufferLength - 16 >= errorMessageLength ? errorMessageLength : byteBufferLength - 16;
        byte[] errMessageBytes = new byte[canReadBytes];
        byteBuffer.readBytes(errMessageBytes);
        return new String(errMessageBytes);
    }

    public void process(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
        while (byteBuf.isReadable()) {
            byteBuf.readBytes(ctrl);
            int length= MessageV1.getHeaderLen(ctrl);
            boolean isBig = MessageV1.isBigMsg(ctrl[0]);
            if (ERROR_HEADER_LENGTH == length && !isBig) {
                // the message length is accurate 8 bytes long
                throw new RuntimeException(retrieveErrMsgFromByteBuffer(byteBuf.duplicate()));
            }
            if (isBig) {
                byteBuf.readBytes(bigCtrl);
            }
            sm.parseByteBuf(ctx, byteBuf,length);
        }
    }
}
