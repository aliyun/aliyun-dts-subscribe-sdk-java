package com.aliyun.dts.subscribe.clients.record.fast;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LazyRecordDeserializerJsonTest {

    @Test
    public void deserializesPrimaryAndUniqueKeyMetadata() throws Exception {
        Pair<Map<String, Boolean>, Map<String, Pair<Boolean, List<String>>>> result =
                LazyRecordDeserializer.deserializePkUkInfo(
                        "{\"PRIMARY\":[\"id\"],\"uk_tenant_name\":[\"tenant_id\",\"name\"]}");

        Assert.assertTrue(result.getLeft().get("id"));
        Assert.assertFalse(result.getLeft().get("tenant_id"));
        Assert.assertFalse(result.getLeft().get("name"));
        Assert.assertTrue(result.getRight().get("PRIMARY").getLeft());
        Assert.assertEquals(Arrays.asList("id"), result.getRight().get("PRIMARY").getRight());
        Assert.assertFalse(result.getRight().get("uk_tenant_name").getLeft());
        Assert.assertEquals(Arrays.asList("tenant_id", "name"),
                result.getRight().get("uk_tenant_name").getRight());
    }
}
