package com.taobao.drc.togo.client.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AbstractIterator;

import java.util.*;

/**
 * @author yangyang
 * @since 17/3/28
 */
public class SchemafulConsumerRecords implements Iterable<SchemafulConsumerRecord> {
    private static final SchemafulConsumerRecords EMPTY = new SchemafulConsumerRecords(Collections.emptyMap());

    private final Map<TopicPartition, List<SchemafulConsumerRecord>> records;

    public SchemafulConsumerRecords(Map<TopicPartition, List<SchemafulConsumerRecord>> records) {
        this.records = records;
    }

    /**
     * Get just the records for the given partition
     *
     * @param partition The partition to get records for
     */
    public List<SchemafulConsumerRecord> records(TopicPartition partition) {
        List<SchemafulConsumerRecord> recs = this.records.get(partition);
        if (recs == null)
            return Collections.emptyList();
        else
            return Collections.unmodifiableList(recs);
    }

    /**
     * Get just the records for the given topic
     */
    public Iterable<SchemafulConsumerRecord> records(String topic) {
        if (topic == null)
            throw new IllegalArgumentException("Topic must be non-null.");
        List<List<SchemafulConsumerRecord>> recs = new ArrayList<>();
        for (Map.Entry<TopicPartition, List<SchemafulConsumerRecord>> entry : records.entrySet()) {
            if (entry.getKey().topic().equals(topic))
                recs.add(entry.getValue());
        }
        return new ConcatenatedIterable(recs);
    }

    /**
     * Get the partitions which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (may be empty if no data was returned)
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(records.keySet());
    }

    @Override
    public Iterator<SchemafulConsumerRecord> iterator() {
        return new ConcatenatedIterable(records.values()).iterator();
    }

    /**
     * The number of records for all topics
     */
    public int count() {
        int count = 0;
        for (List<SchemafulConsumerRecord> recs : this.records.values())
            count += recs.size();
        return count;
    }

    private static class ConcatenatedIterable implements Iterable<SchemafulConsumerRecord> {

        private final Iterable<? extends Iterable<SchemafulConsumerRecord>> iterables;

        public ConcatenatedIterable(Iterable<? extends Iterable<SchemafulConsumerRecord>> iterables) {
            this.iterables = iterables;
        }

        @Override
        public Iterator<SchemafulConsumerRecord> iterator() {
            return new AbstractIterator<SchemafulConsumerRecord>() {
                Iterator<? extends Iterable<SchemafulConsumerRecord>> iters = iterables.iterator();
                Iterator<SchemafulConsumerRecord> current;

                public SchemafulConsumerRecord makeNext() {
                    while (current == null || !current.hasNext()) {
                        if (iters.hasNext())
                            current = iters.next().iterator();
                        else
                            return allDone();
                    }
                    return current.next();
                }
            };
        }
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static SchemafulConsumerRecords empty() {
        return EMPTY;
    }
}
