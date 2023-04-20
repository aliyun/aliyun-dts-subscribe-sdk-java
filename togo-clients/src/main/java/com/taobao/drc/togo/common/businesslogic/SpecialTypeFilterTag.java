package com.taobao.drc.togo.common.businesslogic;

/**
 * Created by longxuan on 18/1/23.
 * Use bit to indicate if the given special type is required
 * bit 0: being and commit
 * bit 1: ddl
 * bit 2: heartbeat
 * bit 3: null kv type
 * bit 4 - 63: reserved
 */
public class SpecialTypeFilterTag {
    // require all the types
    public static final long DEFAULT_FILTER_TAG = 0x0FFFFFFFFFFFFFFFL;
    public static final SpecialTypeFilterTag DEFAULT_SPECIAL_TYPE_TAG = new SpecialTypeFilterTag(DEFAULT_FILTER_TAG);
    private long filterTag;
    public SpecialTypeFilterTag() {
        filterTag = DEFAULT_FILTER_TAG;
    }
    public SpecialTypeFilterTag(long filterTag) {
        this.filterTag = filterTag;
    }
    public SpecialTypeFilterTag requireBeginAndCommit() {
        filterTag = filterTag | 0x0000000000000001L;
        return this;
    }
    public SpecialTypeFilterTag filterBeginAndCommit() {
        filterTag = filterTag & 0x0FFFFFFFFFFFFFFEL;
        return this;
    }

    public SpecialTypeFilterTag requireDDL() {
        filterTag = filterTag | 0x0000000000000002L;
        return this;
    }

    public SpecialTypeFilterTag filterDDL() {
        filterTag = filterTag & 0x0FFFFFFFFFFFFFFDL;
        return this;
    }

    public SpecialTypeFilterTag requireHeartbeat() {
        filterTag = filterTag | 0x0000000000000004L;
        return this;
    }

    public SpecialTypeFilterTag filterHeartbeat() {
        filterTag = filterTag & 0x0FFFFFFFFFFFFFFBL;
        return this;
    }

    private SpecialTypeFilterTag requiredNullKVType() {
        filterTag = filterTag | 0x0000000000000008L;
        return this;
    }

    public SpecialTypeFilterTag filterNullKVType() {
        filterTag = filterTag & 0x0FFFFFFFFFFFFFF7L;
        return this;
    }

    public long getFilterTag() {
        return filterTag;
    }

    public boolean isBeginCommitRequired() {
        return (filterTag & 0x0000000000000001L) != 0L;
    }

    public boolean isDDLRequired() {
        return (filterTag & 0x0000000000000002L) != 0L;
    }

    public boolean isHeartbeatRequired() {
        return (filterTag & 0x0000000000000004L) != 0L;
    }

    public boolean isNullKVRequired() {
        return (filterTag & 0x0000000000000008L) != 0L;
    }

    public void clear() {
        filterTag = 0x0000000000000000L;
    }

    public void reset() {
        filterTag = DEFAULT_FILTER_TAG;
    }

    public boolean isNotDefaultSpecialFilterTag() {
        return filterTag != DEFAULT_FILTER_TAG;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("require begin&commit:").append(isBeginCommitRequired()).append("\t").
                append("require heartbeat:").append(isHeartbeatRequired()).append("\t").
                append("require ddl:").append(isDDLRequired()).append("\t").
                append("require null kv:").append(isNullKVRequired()).append("\n");
        return stringBuilder.toString();
    }
}
