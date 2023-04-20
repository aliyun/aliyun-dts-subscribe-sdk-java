package com.taobao.drc.togo.client.consumer;

import org.apache.kafka.common.requests.FlexListOffsetRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author yangyang
 * @since 17/4/1
 */
public class QueryConditions {
    interface Builder {
        QueryCondition build();
    }

    public static SimpleEqualityQueryBuilder simpleEquality() {
        return new SimpleEqualityQueryBuilder();
    }

    public static class SimpleEqualityQueryBuilder implements Builder, QueryCondition {
        private final List<EqualityEntry> entries = new ArrayList<>();
        private AtomicBoolean built = new AtomicBoolean(false);

        public SimpleEqualityQueryBuilder add(String fieldName, String fieldValue) {
            entries.add(new EqualityEntry(fieldName, fieldValue));
            return this;
        }

        @Override
        public FlexListOffsetRequest.IndexEntry createRequestEntry() {
            if (!built.get()) {
                throw new IllegalStateException("The condition [" + this + "] must be built first");
            }
            return new FlexListOffsetRequest.IndexEntry(0, entries.stream()
                    .map(eq -> new FlexListOffsetRequest.FieldEntry(eq.getFieldName(), eq.getFieldValue()))
                    .collect(Collectors.toList()));
        }

        @Override
        public QueryCondition build() {
            built.compareAndSet(false, true);
            return this;
        }

        @Override
        public String toString() {
            if (built.get()) {
                return entries.stream()
                        .map(entry -> entry.getFieldName() + "=" + entry.getFieldValue())
                        .collect(Collectors.joining(", ", "SimpleEqualityQuery{", "}"));
            } else {
                return "SimpleEqualityQueryBuilder{" +
                        "entries=" + entries +
                        '}';
            }
        }

        private static class EqualityEntry {
            private final String fieldName;
            private final String fieldValue;

            private EqualityEntry(String fieldName, String fieldValue) {
                this.fieldName = fieldName;
                this.fieldValue = fieldValue;
            }

            String getFieldName() {
                return fieldName;
            }

            String getFieldValue() {
                return fieldValue;
            }
        }
    }
}
