package com.aliyun.dts.subscribe.clients.common;

import com.aliyun.dts.subscribe.clients.record.UserRecord;

public  interface RecordListener {

    public void consume(UserRecord record);

}
