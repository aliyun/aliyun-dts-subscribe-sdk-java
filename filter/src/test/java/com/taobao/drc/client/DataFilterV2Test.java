package com.taobao.drc.client;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DataFilterV2Test {
    @Test
    public void testParseFilterInfoList() {
        DataFilterV2 dataFilterV2 = DataFilterV2.create();
        dataFilterV2.addFilterTuple("*", "buyers_0004", "tc_biz_order_[0-9]*", "*");

        Map<String, Map<String, List<String>>> dstoreRequiredMap = dataFilterV2.dstoreRequiredMap();

        Assert.assertTrue(dstoreRequiredMap.get("buyers_0004") != null);
    }
}
