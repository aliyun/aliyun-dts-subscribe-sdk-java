package com.aliyun.dms.subscribe.clients;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.dts.subscribe.clients.formats.avro.Operation;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;

import java.util.HashMap;
import java.util.Map;

public class DBMapper {


    // Object = Map<PhysicDB，Map<LogicTable, List<PhysicTable>>>
    // Map<LogicDB, List<Object>>
   // Map<PhicDB, LogicDB>


    // Map<LogicDB, Map<PhysicDB, Map<LogicTable, PhysicTable>>>
    private static Map<String, Map<String, Map<String, String>>> logic2PhysicDBMapper;
    private static Map<String, String> physic2logicDBMapper = new HashMap<>();
    private static Map<String, String> physic2logicTableMapper = new HashMap<>();
    private static boolean mapping = true;
    // map logic dbname to a map stored table name mapping
  //  private static Map<String, Map<String, String>> db2tbMapper;


    public static void init(String dbListString) {
        JSONObject dbList = JSONObject.parseObject(dbListString);
        for (Map.Entry<String, Object> entry: dbList.entrySet()) {
            String physicDb = entry.getKey();
            String logicDb = (String)((JSONObject)entry.getValue()).get("name");
            JSONObject tables = (JSONObject)((JSONObject)entry.getValue()).get("Table");

            physic2logicDBMapper.put(physicDb, logicDb);
            for (Map.Entry<String, Object> table: tables.entrySet()) {
                String physicTable = table.getKey();
                String logicTable = (String)((JSONObject)table.getValue()).get("name");
                physic2logicTableMapper.put(physicDb + "." + physicTable, logicDb + "." + logicTable);
            }
        }

    }
    public static void init(Map<String, Map<String, Map<String, String>>> dbmapper, boolean map) {
        logic2PhysicDBMapper = dbmapper;
        mapping = map;

        for (Map.Entry<String, Map<String, Map<String, String>>> entry: logic2PhysicDBMapper.entrySet()) {
            String logicDB = entry.getKey();

            for (Map.Entry<String, Map<String, String>> db2Tb: entry.getValue().entrySet()) {
                String physicDB = db2Tb.getKey();
                physic2logicDBMapper.put(physicDB, logicDB);

                Map<String, Map<String, String>> logic2tbmapper = new HashMap<>();
                for (Map.Entry<String, String> tbEntry: db2Tb.getValue().entrySet()) {
                    String logicTb = tbEntry.getKey();
                    String physicTb = tbEntry.getValue();

                    physic2logicTableMapper.put(physicDB + "." + physicTb, logicDB + "." + logicTb);
                }
            }
        }
    }

    public static Record transform(Record record) {

            if (record.getOperation().equals(Operation.DDL)) {
                record.setObjectName(physic2logicDBMapper.get(record.getObjectName()));
            } else {
                record.setObjectName(physic2logicTableMapper.get(record.getObjectName()));
            }
            return record;

    }

    public static boolean isMapping() {
        return mapping;
    }

    public static void setMapping(boolean mapping) {
        DBMapper.mapping = mapping;
    }

//    public static void main(String[] args) {
//        init("{\"dts_h02\":{\"all\":false,\"name\":\"dts_h\",\"Table\":{\"dtsh27_02\":{\"all\":true,\"name\":\"dtsh\"},\"dts28_01\":{\"all\":true,\"name\":\"dts\"},\"dts28_02\":{\"all\":true,\"name\":\"dts\"}}},\"dts_h01\":{\"all\":false,\"name\":\"dts_h\",\"Table\":{\"dtsh27_01\":{\"all\":true,\"name\":\"dtsh\"},\"dts29_02\":{\"all\":true,\"name\":\"dts\"},\"dts29_01\":{\"all\":true,\"name\":\"dts\"}}}}");
//    }
}
