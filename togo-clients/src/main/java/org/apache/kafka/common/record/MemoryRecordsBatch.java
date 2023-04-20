package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Created by longxuan on 2018/6/29.
 */
public class MemoryRecordsBatch  implements RecordBatch{
    final List<Record> records;
    final long baseOffset;
    final CompressionType compressionType;
    final TimestampType timestampType;
    final long maxTimeStamp;
    final long endOffset;
    public MemoryRecordsBatch(CompressionType compressionType, List<Record> records, long baseOffset, TimestampType timestampType, long maxTimeStamp, long endOffset) {
        this.records = records;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.timestampType = timestampType;
        this.maxTimeStamp = maxTimeStamp;
        this.endOffset = endOffset;
    }
    @Override
    public boolean isValid() {
        throw new RuntimeException("should not used");
    }

    @Override
    public void ensureValid() {
        throw new RuntimeException("should not used");
    }

    @Override
    public long checksum() {
        throw new RuntimeException("should not used");
    }

    @Override
    public long maxTimestamp() {
        return maxTimeStamp;
    }

    @Override
    public TimestampType timestampType() {
        return timestampType;
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public long lastOffset() {
        return endOffset;
    }

    @Override
    public long nextOffset() {
        throw new RuntimeException("should not used");
    }

    @Override
    public byte magic() {
        return RecordBatch.CURRENT_MAGIC_VALUE;
    }

    @Override
    public long producerId() {
        throw new RuntimeException("should not used");
    }

    @Override
    public short producerEpoch() {
        throw new RuntimeException("should not used");
    }

    @Override
    public boolean hasProducerId() {
        throw new RuntimeException("should not used");
    }

    @Override
    public int baseSequence() {
        throw new RuntimeException("should not used");
    }

    @Override
    public int lastSequence() {
        throw new RuntimeException("should not used");
    }

    @Override
    public CompressionType compressionType() {
        return compressionType;
    }

    @Override
    public int sizeInBytes() {
        throw new RuntimeException("should not used");
    }

    @Override
    public Integer countOrNull() {
        throw new RuntimeException("should not used");
    }

    @Override
    public boolean isCompressed() {
        throw new RuntimeException("should not used");
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        throw new RuntimeException("should not used");
    }

    @Override
    public boolean isTransactional() {
        throw new RuntimeException("should not used");
    }

    @Override
    public int partitionLeaderEpoch() {
        throw new RuntimeException("should not used");
    }

    @Override
    public CloseableIterator<Record> streamingIterator(BufferSupplier decompressionBufferSupplier) {
        throw new RuntimeException("should not used");
    }

    @Override
    public boolean isControlBatch() {
        throw new RuntimeException("should not used");
    }

    @Override
    public Iterator<Record> iterator() {
        return records.iterator();
    }
}
