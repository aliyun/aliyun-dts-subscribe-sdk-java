package com.aliyun.dts.subscribe.clients.common;

import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;

public  interface RecordListener {

    public void consume(DefaultUserRecord record);

}
