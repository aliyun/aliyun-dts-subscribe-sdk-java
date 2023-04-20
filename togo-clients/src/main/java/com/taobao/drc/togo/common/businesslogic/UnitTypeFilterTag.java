package com.taobao.drc.togo.common.businesslogic;


import org.apache.kafka.common.requests.SetFetchTagsRequest;

import java.util.*;

/**
 * Created by longxuan on 18/1/23.
 */
public class UnitTypeFilterTag {
    private static final String DEFAULT_NAME = "default";
    private static final String DB_NAME = "dbName";
    private static final String TABLE_NAME = "tbName";
    public  enum UnitType {
        ASK_ALL_UNIT(0, "AskAllUnit"),
        ASK_SELF_UNIT(1, "AskSelfUnit"),
        ASK_OTHER_UNIT(2, "AskOtherUnit"),
        ASK_GIVEN_UNIT(3, "AskGivenUnit"),
        ASK_EXCLUDE_GIVEN_UNIT(4, "AskExcludeGivenUnit"),
        UNKNOWN(1024, "UnKnown");
        private int unitType;
        private String describe;
        UnitType(int unitType, String describe) {
            this.unitType = unitType;
            this.describe = describe;
        }
        public int getUnitType() {
            return unitType;
        }
        public String getDescribe() {
            return describe;
        }
    }
    static Map<Integer, UnitType> unitTypes;
    static {
        unitTypes = new HashMap<>();
        for (UnitType unitType : UnitType.values()) {
            unitTypes.put(unitType.unitType, unitType);
        }
    }

    public static SetFetchTagsRequest.TagEntry buildTagEntryFromDbNameAndTableName(String dbName, String tableName) {
        if (null == dbName || null == tableName) {
            throw new RuntimeException("bad filter on build tag entry, dbname:" + dbName + ", tableName:" + tableName);
        }
        SetFetchTagsRequest.FieldKeyValueEntry dbKV = new SetFetchTagsRequest.FieldKeyValueEntry(DEFAULT_NAME, DB_NAME, dbName);
        SetFetchTagsRequest.FieldKeyValueEntry tbKV = new SetFetchTagsRequest.FieldKeyValueEntry(DEFAULT_NAME, TABLE_NAME, tableName);
        return new SetFetchTagsRequest.TagEntry(true, Arrays.asList(dbKV, tbKV));
    }


    public static UnitType idToUnitType(int id) {
        return unitTypes.getOrDefault(id, UnitType.UNKNOWN);
    }
    public static final UnitTypeFilterTag DEFAULT_UNIT_FILTER_TAG = UnitTypeFilterTagBuilder.create().askAllUnit().build();
    private UnitType currentUnitType;
    private List<Integer> requiredRegionIDs;
    private List<SetFetchTagsRequest.TagEntry> txnPattern;
    private boolean useTxn;
    private boolean useThreadID;
    private boolean useRegionID;
    private UnitTypeFilterTag() {
        currentUnitType = UnitType.UNKNOWN;
        requiredRegionIDs = new ArrayList<>();
        txnPattern = new LinkedList<>();
        useTxn = false;
        useThreadID = false;
        useRegionID = false;
    }

    public boolean isUseTxn() {
        return useTxn;
    }

    public boolean isUseThreadID() {
        return useThreadID;
    }

    public boolean isUseRegionID() {
        return useRegionID;
    }

    public UnitType getCurrentUnitType() {
        return currentUnitType;
    }

    public boolean isAskAllUnit() {
        return currentUnitType == UnitType.ASK_ALL_UNIT;
    }

    public List<SetFetchTagsRequest.TagEntry> getTxnPattern() {
        return txnPattern;
    }

    public List<Integer> getRequiredRegionIDs() {
        return requiredRegionIDs;
    }

    public void checkAndSetPreviousType(UnitType toSet) {
        if (currentUnitType == UnitType.UNKNOWN) {
            currentUnitType = toSet;
        } else {
            if (currentUnitType != toSet) {
                throw new RuntimeException("Previous unit type: " + currentUnitType.describe +
                        " not compatible with given type: " + toSet.describe);
            }
        }
    }

    public void validateTagTypes() {
        switch (currentUnitType) {
            // ask all unit indicate unit filter tag will not work
            case ASK_ALL_UNIT: {
                return;
            }
            case ASK_SELF_UNIT:
            case ASK_OTHER_UNIT:{
                if (useTxn) {
                    if (null == txnPattern || txnPattern.isEmpty()) {
                        throw new RuntimeException("Unit type tag txn flag set but is null");
                    }
                }
                return;
            }
            case ASK_EXCLUDE_GIVEN_UNIT:
            case ASK_GIVEN_UNIT: {
                if (requiredRegionIDs == null || requiredRegionIDs.isEmpty()) {
                    throw new RuntimeException("Unit type ask [given/exclude] flag set but required id is empty");
                }
                if (false == useRegionID) {
                    throw new RuntimeException("Unit type ask [given/exclude]  flag set use region id flag is false");
                }
                return;
            }
            case UNKNOWN: {
                throw new RuntimeException("Unit type unknown flag ");
            }

        }
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        switch (currentUnitType) {
            case ASK_ALL_UNIT: {
                stringBuilder.append("all data required\n");
                break;
            }
            case ASK_SELF_UNIT:
            case ASK_OTHER_UNIT: {
                stringBuilder.append(currentUnitType.describe).append(", use thread:").append(useThreadID)
                        .append(", use txn:").append(useTxn).append(",txn pattern:").append(txnPattern).append("\n");
                break;
            }
            case ASK_EXCLUDE_GIVEN_UNIT:
            case ASK_GIVEN_UNIT: {
                if (currentUnitType == UnitType.ASK_GIVEN_UNIT) {
                    stringBuilder.append("ask given unit, sub region id:[");
                } else {
                    stringBuilder.append("ask excluded unit, exclude region id:[");
                }
                if (requiredRegionIDs != null) {
                    for (Integer regionID : requiredRegionIDs) {
                        stringBuilder.append(regionID).append(",");
                    }
                }
                stringBuilder.append("]").append("\n");
                break;
            }
            case UNKNOWN: {
                stringBuilder.append("Unknown state");
                break;
            }
        }
        return stringBuilder.toString();
    }

    public static UnitTypeFilterTag fromParam(UnitType unitType, boolean useTxn,
                                              List<SetFetchTagsRequest.TagEntry> txnPattern,
                                              boolean useThreadID,
                                              boolean useRegionID,
                                              List<Integer> requiredRegionIDs) {
        UnitTypeFilterTag unitTypeFilterTag = new UnitTypeFilterTag();
        unitTypeFilterTag.currentUnitType = unitType;
        unitTypeFilterTag.useTxn = useTxn;
        unitTypeFilterTag.txnPattern =txnPattern;
        unitTypeFilterTag.useThreadID = useThreadID;
        unitTypeFilterTag.useRegionID = useRegionID;
        unitTypeFilterTag.requiredRegionIDs = requiredRegionIDs;
        unitTypeFilterTag.validateTagTypes();
        return unitTypeFilterTag;
    }

    public static class UnitTypeFilterTagBuilder {
        private UnitTypeFilterTag unitTypeFilterTag;
        public static UnitTypeFilterTagBuilder create() {
            UnitTypeFilterTagBuilder ret = new UnitTypeFilterTagBuilder();
            ret.unitTypeFilterTag = new UnitTypeFilterTag();
            return ret;
        }


        public UnitTypeFilterTagTxnTablePatternBuilder askSelfUnitUseTxn() {
            unitTypeFilterTag.checkAndSetPreviousType(UnitType.ASK_SELF_UNIT);
            unitTypeFilterTag.useTxn = true;
            return new UnitTypeFilterTagTxnTablePatternBuilder();
        }

        public class UnitTypeFilterTagTxnTablePatternBuilder {
            public UnitTypeFilterTagTxnTablePatternBuilder addTxnPattern(String dbNamePattern, String tableNamePattern) {
                SetFetchTagsRequest.TagEntry tagEntry = buildTagEntryFromDbNameAndTableName(dbNamePattern, tableNamePattern);
                unitTypeFilterTag.txnPattern.add(tagEntry);
                return this;
            }
            public UnitTypeFilterTagBuilder done() {
                return UnitTypeFilterTagBuilder.this;
            }
        }

        public UnitTypeFilterTagTxnTablePatternBuilder askOtherUnitUseTxn() {
            unitTypeFilterTag.checkAndSetPreviousType(UnitType.ASK_OTHER_UNIT);
            unitTypeFilterTag.useTxn = true;
            return new UnitTypeFilterTagTxnTablePatternBuilder();
        }

        public UnitTypeFilterTagBuilder askSelfUnitUseThreadID() {
            unitTypeFilterTag.checkAndSetPreviousType(UnitType.ASK_SELF_UNIT);
            unitTypeFilterTag.useThreadID = true;
            return this;
        }

        public UnitTypeFilterTagBuilder askOtherUnitUseThreadID() {
            unitTypeFilterTag.checkAndSetPreviousType(UnitType.ASK_OTHER_UNIT);
            unitTypeFilterTag.useThreadID = true;
            return this;
        }

        public UnitTypeFilterTagBuilder askGivenUnit(Collection<Integer> regionIDs) {
            unitTypeFilterTag.checkAndSetPreviousType(UnitType.ASK_GIVEN_UNIT);
            unitTypeFilterTag.useRegionID = true;
            if (null != regionIDs) {
                unitTypeFilterTag.requiredRegionIDs.addAll(regionIDs);
            }
            return this;
        }

        public UnitTypeFilterTagBuilder askAllUnit() {
            unitTypeFilterTag.checkAndSetPreviousType(UnitType.ASK_ALL_UNIT);
            return this;
        }

        public UnitTypeFilterTag build() {
            unitTypeFilterTag.validateTagTypes();
            return unitTypeFilterTag;
        }
    }
}
