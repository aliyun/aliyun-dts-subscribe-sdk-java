package com.taobao.drc.client.message;

import com.taobao.drc.client.message.DataMessage.Record.Field;

/**
 * Created by jianjundeng on 5/25/16.
 */
public interface FieldParseListener {

    public void parseNotify(Field prev, Field next)throws Exception;
}
