package com.taobao.drc.client.message.dts;

import com.taobao.drc.client.message.ByteString;

public class DStoreRawValue {
    private ByteString data;
    private String encoding;
    private int type;
    public DStoreRawValue(ByteString data, String encoding, int type) {
        this.data = data;
        this.encoding = encoding;
        this.type = type;
    }

    public ByteString getData() {
        return data;
    }

    public String getEncoding() {
        return encoding;
    }

    public int getType() {
        return type;
    }

    public int size() {
        return data.getLen();
    }

    @Override
    public String toString() {
        try {
            return new String(data.getBytes(), encoding);
        } catch (Throwable e) {
            return new String(data.getBytes());
        }
    }
}
