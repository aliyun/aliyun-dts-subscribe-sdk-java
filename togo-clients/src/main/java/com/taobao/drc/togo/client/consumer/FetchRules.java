package com.taobao.drc.togo.client.consumer;


import com.taobao.drc.togo.common.businesslogic.HashTypeFilterTag;
import com.taobao.drc.togo.common.businesslogic.SpecialTypeFilterTag;
import com.taobao.drc.togo.common.businesslogic.UnitTypeFilterTag;
import org.apache.kafka.common.requests.SetFetchTagsRequest;

import java.util.*;
import java.util.stream.Collectors;

import static com.taobao.drc.togo.common.businesslogic.UnitTypeFilterTag.buildTagEntryFromDbNameAndTableName;

/**
 * @author yangyang
 * @since 17/3/9
 */
public class FetchRules {
    public static FilterRuleBuilder filter() {
        return new FilterRuleBuilder();
    }

    interface FetchRuleBuilder {
        FetchRule build();
    }

    public static class FilterRuleBuilder implements FetchRuleBuilder {
        private final List<List<FilterEntry>> includeList = new ArrayList<>();
        private final List<List<FilterEntry>> excludeList = new ArrayList<>();
        private final SpecialTypeFilterTag specialTypeFilterTag = new SpecialTypeFilterTag();
        private final HashTypeFilterTag hashTypeFilterTag = new HashTypeFilterTag(0);
        private UnitTypeFilterTag unitTypeFilterTag = UnitTypeFilterTag.DEFAULT_UNIT_FILTER_TAG;
        public ConjunctionBuilder include() {
            return new ConjunctionBuilder(includeList);
        }

        public ConjunctionBuilder exclude() {
            return new ConjunctionBuilder(excludeList);
        }

        public SpecialRecordTypeBuilder specialRecordTypeBuilder() {
            return new SpecialRecordTypeBuilder();
        }

        public HashTypeFilerBuilder hashTypeFilterBuilder() {
            return new HashTypeFilerBuilder();
        }

        public UnitFilterTypeBuilder unitFilterTypeBuilder() {
            return new UnitFilterTypeBuilder();
        }

        @Override
        public FetchRule build() {
            return new FilterRule(Collections.unmodifiableList(includeList), Collections.unmodifiableList(excludeList), specialTypeFilterTag, hashTypeFilterTag, unitTypeFilterTag);
        }

        static class FilterRule implements FetchRule {
            private final List<List<FilterEntry>> includeList;
            private final List<List<FilterEntry>> excludeList;
            private final SpecialTypeFilterTag specialTypeFilterTag;
            private final HashTypeFilterTag hashTypeFilterTag;
            private final UnitTypeFilterTag unitTypeFilterTag;
            FilterRule(List<List<FilterEntry>> includeList, List<List<FilterEntry>> excludeList, SpecialTypeFilterTag specialTypeFilterTag, HashTypeFilterTag hashTypeFilterTag, UnitTypeFilterTag unitTypeFilterTag) {
                this.includeList = includeList;
                this.excludeList = excludeList;
                this.specialTypeFilterTag = specialTypeFilterTag;
                this.hashTypeFilterTag = hashTypeFilterTag;
                this.unitTypeFilterTag = unitTypeFilterTag;
            }

            private List<SetFetchTagsRequest.FieldKeyValueEntry> buildKVEntries(Collection<FilterEntry> entries) {
                return entries.stream()
                        .map(entry -> new SetFetchTagsRequest.FieldKeyValueEntry(entry.getSchemaName(), entry.getFieldName(), entry.getFieldValue()))
                        .collect(Collectors.toList());
            }

            private List<SetFetchTagsRequest.TagEntry> buildTagEntries(boolean include, List<List<FilterEntry>> entries) {
                return entries.stream()
                        .map(entry -> new SetFetchTagsRequest.TagEntry(include, buildKVEntries(entry)))
                        .collect(Collectors.toList());
            }

            @Override
            public List<SetFetchTagsRequest.TagEntry> createFetchEntry() {
                List<SetFetchTagsRequest.TagEntry> ret = new ArrayList<>();
                ret.addAll(buildTagEntries(true, includeList));
                ret.addAll(buildTagEntries(false, excludeList));
                return ret;
            }

            @Override
            public SpecialTypeFilterTag specialTypeFilterTag() {
                return specialTypeFilterTag;
            }

            @Override
            public HashTypeFilterTag hashTypeFilterTag() {
                return hashTypeFilterTag;
            }

            @Override
            public UnitTypeFilterTag unitTypeFilterTag() {
                return unitTypeFilterTag;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                FilterRule that = (FilterRule) o;
                return Objects.equals(includeList, that.includeList) &&
                        Objects.equals(excludeList, that.excludeList);
            }

            @Override
            public int hashCode() {
                return Objects.hash(includeList, excludeList);
            }
        }

        static class FilterEntry {
            private final String schemaName;
            private final String fieldName;
            private final String fieldValue;

            public FilterEntry(String schemaName, String fieldName, String fieldValue) {
                this.schemaName = schemaName;
                this.fieldName = fieldName;
                this.fieldValue = fieldValue;
            }

            public String getSchemaName() {
                return schemaName;
            }

            public String getFieldName() {
                return fieldName;
            }

            public String getFieldValue() {
                return fieldValue;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                FilterEntry that = (FilterEntry) o;
                return Objects.equals(schemaName, that.schemaName) &&
                        Objects.equals(fieldName, that.fieldName) &&
                        Objects.equals(fieldValue, that.fieldValue);
            }

            @Override
            public int hashCode() {
                return Objects.hash(schemaName, fieldName, fieldValue);
            }
        }

        public class ConjunctionBuilder {
            private final List<List<FilterEntry>> target;
            private final List<FilterEntry> entries = new ArrayList<>();

            ConjunctionBuilder(List<List<FilterEntry>> target) {
                this.target = target;
            }

            public ConjunctionBuilder add(String schemaName, String fieldName, String fieldValue) {
                entries.add(new FilterEntry(schemaName, fieldName, fieldValue));
                return this;
            }

            public FilterRuleBuilder done() {
                target.add(entries);
                return FilterRuleBuilder.this;
            }
        }

        public class SpecialRecordTypeBuilder {
            public SpecialRecordTypeBuilder filterBeginCommit() {
                specialTypeFilterTag.filterBeginAndCommit();
                return this;
            }
            public SpecialRecordTypeBuilder filterHeartBeat() {
                specialTypeFilterTag.filterHeartbeat();
                return this;
            }
            public SpecialRecordTypeBuilder filterDDL() {
                specialTypeFilterTag.filterDDL();
                return this;
            }
            public SpecialRecordTypeBuilder filterNullKV() {
                specialTypeFilterTag.filterNullKVType();
                return this;
            }
            public FilterRuleBuilder done() {
                return FilterRuleBuilder.this;
            }
        }

        public class HashTypeFilerBuilder {
            public HashTypeFilerBuilder maxHashValue(int maxHashValue) {
                hashTypeFilterTag.setMaxHashValue(maxHashValue);
                return this;
            }
            public HashTypeFilerBuilder addSubHashElement(int hashElement) {
                hashTypeFilterTag.addHashElement(hashElement);
                return this;
            }
            public HashTypeFilerBuilder addSubHashElements(Collection<Integer> hashElements) {
                hashTypeFilterTag.addHashElements(hashElements);
                return this;
            }
            public FilterRuleBuilder done() {
                return FilterRuleBuilder.this;
            }
        }
        public class UnitFilterTypeBuilder {
            private UnitTypeFilterTag.UnitType unitType = UnitTypeFilterTag.UnitType.UNKNOWN;
            private boolean useTxn = false;
            private boolean useThreadID = false;
            private boolean useGivenID = false;
            private List<SetFetchTagsRequest.TagEntry> txnEntries = null;
            private List<Integer> regionIDList = null;
            public class TxnEntryBuilder {
                public TxnEntryBuilder addTxnPattern(String dbPattern, String tablePattern) {
                    SetFetchTagsRequest.TagEntry tagEntry = buildTagEntryFromDbNameAndTableName(dbPattern, tablePattern);
                    if (null == txnEntries) {
                        txnEntries = new LinkedList<>();
                    }
                    txnEntries.add(tagEntry);
                    return this;
                }

                public UnitFilterTypeBuilder done() {
                    return UnitFilterTypeBuilder.this;
                }
            }
            public UnitFilterTypeBuilder askAllUnit() {
                unitType = UnitTypeFilterTag.UnitType.ASK_ALL_UNIT;
                return this;
            }
            private void checkState(UnitTypeFilterTag.UnitType toType) {
                if (unitType != toType && unitType != UnitTypeFilterTag.UnitType.UNKNOWN) {
                    throw new RuntimeException("state change failed, can't convert from " + unitType.getDescribe() + " to " + toType.getDescribe());
                }
            }
            public TxnEntryBuilder askSelfUnitUseTxn() {
                checkState(UnitTypeFilterTag.UnitType.ASK_SELF_UNIT);
                unitType = UnitTypeFilterTag.UnitType.ASK_SELF_UNIT;
                useTxn = true;
                return new TxnEntryBuilder();
            }
            public UnitFilterTypeBuilder askSelfUnitUseThreadID() {
                checkState(UnitTypeFilterTag.UnitType.ASK_SELF_UNIT);
                unitType = UnitTypeFilterTag.UnitType.ASK_SELF_UNIT;
                useThreadID = true;
                return this;
            }
            public TxnEntryBuilder askOtherUnitUseTxn() {
                checkState(UnitTypeFilterTag.UnitType.ASK_OTHER_UNIT);
                unitType = UnitTypeFilterTag.UnitType.ASK_OTHER_UNIT;
                useTxn = true;
                return new TxnEntryBuilder();
            }
            public UnitFilterTypeBuilder askOtherUnitUseThreadID() {
                checkState(UnitTypeFilterTag.UnitType.ASK_OTHER_UNIT);
                unitType = UnitTypeFilterTag.UnitType.ASK_OTHER_UNIT;
                useThreadID = true;
                return this;
            }
            public UnitFilterTypeBuilder askGivenUnit(Collection<Integer> givenUnitID, boolean excluded) {
                UnitTypeFilterTag.UnitType previousState = excluded ? UnitTypeFilterTag.UnitType.ASK_EXCLUDE_GIVEN_UNIT : UnitTypeFilterTag.UnitType.ASK_GIVEN_UNIT;
                checkState(previousState);
                unitType = previousState;
                useGivenID = true;
                if (null == regionIDList) {
                    regionIDList = new LinkedList<>();
                    regionIDList.addAll(givenUnitID);
                }
                return this;
            }
            public FilterRuleBuilder done() {
                unitTypeFilterTag = UnitTypeFilterTag.fromParam(unitType, useTxn, txnEntries, useThreadID, useGivenID, regionIDList);
                return FilterRuleBuilder.this;
            }
        }
    }
}