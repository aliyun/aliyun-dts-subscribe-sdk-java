package com.taobao.drc.togo.util;


import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by longxuan on 18/1/8.
 *
 */
public class MemoryStaticsStore {
    public interface StoreHelper {
        public void serializeTo(MemoryStaticsStore info);
        public MemoryStaticsStore deserializeFrom();
    }
    public static long DEFAULT_EXPIRE_TIME = 1000 * 60;
    public class OffsetMetaDataStatic extends StaticsObject<OffsetAndMetadata> {
        private long offset;
        private String message;
        public OffsetMetaDataStatic(long expireTimeMs) {
            super(expireTimeMs);
            offset = -1;
            message = "";
        }
        @Override
        public OffsetAndMetadata value() {
            return new OffsetAndMetadata(offset, message);
        }

        @Override
        public String serialize(OffsetAndMetadata value) {
  //          System.out.println("current （offset：metadata):(" + value.offset() + ", " + value.metadata() + ")");
            return value.offset() + ":" + value.metadata();
        }

        @Override
        public OffsetAndMetadata copyElement() {
            return new OffsetAndMetadata(offset, message);
        }

        @Override
        public void processValue(OffsetAndMetadata value) {
            offset = value.offset();
            message = value.metadata();
        }
    }

    private Map<String, OffsetMetaDataStatic> metaDataStaticMap;
    private long expireTimeMs;

    public MemoryStaticsStore(long expireTimeMs) {
        metaDataStaticMap = new ConcurrentHashMap<>();
        this.expireTimeMs = expireTimeMs;
    }



    public void putStatic(String id, OffsetAndMetadata metadata) {
        OffsetMetaDataStatic storeElement = metaDataStaticMap.get(id);
        if (null == storeElement) {
            OffsetMetaDataStatic value = new OffsetMetaDataStatic(expireTimeMs);
            value.processValue(metadata);
            metaDataStaticMap.put(id, value);
        } else {
            storeElement.processValue(metadata);
        }
    }

    public String buildStoreMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<Map.Entry<String, OffsetMetaDataStatic>> iterator = metaDataStaticMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, OffsetMetaDataStatic> entry = iterator.next();
            OffsetMetaDataStatic offsetMetaDataStatic = entry.getValue();
            if (offsetMetaDataStatic.hasExpired()) {
                iterator.remove();
                continue;
            }
            stringBuilder.append("|").append(entry.getKey()).append(":").append(offsetMetaDataStatic.getSerializedString());
        }
        return stringBuilder.toString();
    }

    public boolean isEmpty() {
        return metaDataStaticMap.isEmpty();
    }

}
