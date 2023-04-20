package com.taobao.drc.togo.common.log;

import com.taobao.drc.togo.util.BitUtil;

/**
 * Created by longxuan on 18/1/17.
 */
public class TagAppendEntry implements Comparable<TagAppendEntry>{
    public int tableID;
    public long threadID;
    public long hashAndRegionIDAndRecordType;
    public TagAppendEntry(int tableID, long threadID, long hashAndRegionIDAndRecordType) {
        this.tableID = tableID;
        this.threadID = threadID;
        this.hashAndRegionIDAndRecordType = hashAndRegionIDAndRecordType;
    }

    public void assign(int tableID, long threadID, long hashAndRegionIDAndRecordType) {
        this.tableID = tableID;
        this.threadID = threadID;
        this.hashAndRegionIDAndRecordType = hashAndRegionIDAndRecordType;
    }

    public int getHashValue() {
        return BitUtil.readHashID(hashAndRegionIDAndRecordType);
    }

    public int getRegionID() {
        return BitUtil.readRegionID(hashAndRegionIDAndRecordType);
    }

    public int getRecordType() {
        return BitUtil.readRecordType(hashAndRegionIDAndRecordType);
    }


    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("TableID:").append(tableID).append("\t")
                .append("ThreadID:").append(threadID).append("\t")
                .append("hashValue:").append(getHashValue()).append("\t")
                .append("regionID:").append(getRegionID()).append("\n");
        return stringBuilder.toString();
    }

    @Override
    public int compareTo(TagAppendEntry o) {
        return tableID - o.tableID;
    }
}
