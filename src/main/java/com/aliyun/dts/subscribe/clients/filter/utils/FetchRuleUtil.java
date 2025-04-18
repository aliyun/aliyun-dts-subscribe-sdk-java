package com.aliyun.dts.subscribe.clients.filter.utils;

import com.aliyun.dts.subscribe.clients.filter.DataFilter;
import com.taobao.drc.togo.client.consumer.FetchRules;
import com.taobao.drc.togo.client.consumer.FetchRule;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchRuleUtil {

    public static FetchRule generateFetchRule(DataFilter base) {
        FetchRules.FilterRuleBuilder filter = FetchRules.filter();
        Map<String, Map<String, List<String>>> rules = base.dstoreRequiredMap();
        //include
        if (null != rules && !rules.isEmpty()) {
            for (Map.Entry<String, Map<String, List<String>>> entry : rules.entrySet()) {
                String dbName = entry.getKey();
                for (String tb : entry.getValue().keySet()) {
                    String tableName = tb;
                    filter.include().add("default", "dbName", dbName).add("default", "tbName", tableName).done();
                }
            }
        }
        //exclude
        if (StringUtils.isNotEmpty(base.getBlackList())) {
            String[] segment = base.getBlackList().split("\\|");
            Map<String, List<String>> excludes = new HashMap<String, List<String>>();
            for (String dt : segment) {
                String[] dtPair = dt.split("[;,\\.]");
                String dbName = dtPair[0];
                String table = dtPair[1];
                List<String> tables = excludes.get(dbName);
                if (null == tables) {
                    tables = new ArrayList<String>();
                }
                tables.add(table);
                excludes.put(dbName, tables);
            }
            for (Map.Entry<String, List<String>> tb : excludes.entrySet()) {
                for (String tbName : tb.getValue()) {
                    filter.exclude().add("default", "dbName", tb.getKey()).add("default", "tbName", tbName).done();
                }
            }
        }

        FetchRule filterRule =  filter.build();

        return filterRule;
    }
}
