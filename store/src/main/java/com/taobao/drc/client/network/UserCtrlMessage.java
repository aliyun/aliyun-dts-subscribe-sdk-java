package com.taobao.drc.client.network;

/**
 * @author yangyang
 * @since 17/1/20
 */
public class UserCtrlMessage {
    private final byte[] content;

    public UserCtrlMessage(byte[] content) {
        if (content == null) {
            throw new NullPointerException("Message cannot be null");
        }
        this.content = content;
    }

    public byte[] getContent() {
        return content;
    }
}
