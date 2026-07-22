package com.aliyun.dts.subscribe.clients;

import com.aliyun.dms.subscribe.clients.DBMapper;
import com.aliyun.dts.subscribe.clients.formats.avro.Operation;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import org.junit.Assert;
import org.junit.Test;

public class Fastjson2CompatibilityTest {

    @Test
    public void mapsPhysicalTableNameFromDtsMetadata() {
        DBMapper dbMapper = new DBMapper();
        dbMapper.init("{\"physical_db\":{\"name\":\"logical_db\",\"Table\":{\"physical_table\":{\"name\":\"logical_table\"}}}}");

        Record record = new Record();
        record.setOperation(Operation.UPDATE);
        record.setObjectName("physical_db.physical_table");

        Assert.assertEquals("logical_db.logical_table", dbMapper.transform(record).getObjectName());
    }
}
