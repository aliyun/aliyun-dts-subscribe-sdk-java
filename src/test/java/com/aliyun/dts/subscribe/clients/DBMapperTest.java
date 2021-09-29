package com.aliyun.dts.subscribe.clients;

import com.aliyun.dms.subscribe.clients.DBMapper;
import com.aliyun.dts.subscribe.clients.formats.avro.Operation;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import org.junit.Assert;
import org.junit.Test;


public class DBMapperTest {

    @Test
    public void dbMapperTest() {
        DBMapper.init("{\"dts_h02\":{\"all\":false,\"name\":\"dts_h\",\"Table\":{\"dtsh27_02\":{\"all\":true,\"name\":\"dtsh\"},\"dts28_01\":{\"all\":true,\"name\":\"dts\"},\"dts28_02\":{\"all\":true,\"name\":\"dts\"}}},\"dts_h01\":{\"all\":false,\"name\":\"dts_h\",\"Table\":{\"dtsh27_01\":{\"all\":true,\"name\":\"dtsh\"},\"dts29_02\":{\"all\":true,\"name\":\"dts\"},\"dts29_01\":{\"all\":true,\"name\":\"dts\"}}}}");
        Record record = new Record();
        record.setOperation(Operation.UPDATE);
        String physicTable = "dts_h02.dtsh27_02";
        String logicTable = "dts_h.dtsh";

        record.setObjectName(physicTable);
        record = DBMapper.transform(record);
        Assert.assertEquals(record.getObjectName(), logicTable);

        String physicDb = "dts_h01";
        String logicDb = "dts_h";
        record.setOperation(Operation.DDL);
        record.setObjectName(physicDb);
        record =  DBMapper.transform(record);
        Assert.assertEquals(record.getObjectName(), logicDb);

    }
}
