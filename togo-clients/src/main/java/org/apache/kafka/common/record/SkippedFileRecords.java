package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by longxuan on 17/12/6.
 */
public class SkippedFileRecords extends AbstractRecords implements Cloneable{
    private class OffsetPair {
        public final int beginPos;
        public final int endPos;
        private OffsetPair(int beginPos, int endPos) {
            this.beginPos = beginPos;
            this.endPos = endPos;
        }
    }

    private static final int INVALID_POSITION = -1;
    private  final boolean isSlice;
    private  final int start;
    private  final int end;
    private  final List<Long> skippedOffset;
    private  AtomicInteger size;
    private  final FileChannel channel;
    private  volatile File file;
    private  final Iterable<FileLogInputStream.FileChannelRecordBatch> batches;
    // offset list pair for file record to send
    private List<OffsetPair> sendRecordPositionList;
    private int writeStart;
    private int writeEnd;
    private int hasWriteCount;

    public SkippedFileRecords(FileRecords fileRecords, List<Long> skippedOffset) {
        this.isSlice = fileRecords.isSlice();
        this.start = fileRecords.getStart();
        this.end = fileRecords.getEnd();
        this.skippedOffset = skippedOffset;
        this.channel = fileRecords.channel();
        this.file = fileRecords.file();
        this.batches = fileRecords.batches();
        size = new AtomicInteger(0);
        if (isSlice == false) {
            throw new KafkaException("read file record should not meet false slice situation");
        }
        buildSendRecordPositionList(fileRecords);
        hasWriteCount = 0;
    }

    private void buildSendRecordPositionList(FileRecords fileRecords) {
        sendRecordPositionList = new LinkedList<>();
        int startPos = start;
        int endPos;
        // in records iterator, data in disk is copy to memory. see readFullIndex
        Iterable<Record> records = fileRecords.records();
        //TODO: merge adjacent offset position
        for (Record r : records) {
            int recordSize = r.sizeInBytes();
            if (!skippedOffset.contains(r.offset())) {
                endPos = startPos + recordSize;
                sendRecordPositionList.add(new OffsetPair(startPos, endPos));
                size.addAndGet(recordSize);
            }
            startPos += recordSize;
        }
        if (startPos != end) {
            throw new KafkaException("record length check failed actual:" + startPos + ", excepted:" + end);
        }
        if (sendRecordPositionList.isEmpty()) {
            throw new KafkaException("empty record list not legal");
        }
        OffsetPair firstSend  = sendRecordPositionList.remove(0);
        writeStart = firstSend.beginPos;
        writeEnd = firstSend.endPos;
    }



    @Override
    public int sizeInBytes() {
        return size.get();
    }

    @Override
    public long writeTo(GatheringByteChannel destChannel, long offset, int length) throws IOException {
        long newSize = Math.min(channel.size(), end) - writeStart;
        int oldSize = end - writeStart;
        if (newSize < oldSize)
            throw new KafkaException(String.format(
                    "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), oldSize, newSize));

        long position = writeStart;
        int count = Math.min(length, writeEnd - writeStart);
        if (count == 0) {
            throw new KafkaException("loop detected");
        }
        final long bytesTransferred;
        if (destChannel instanceof TransportLayer) {
            TransportLayer tl = (TransportLayer) destChannel;
            bytesTransferred = tl.transferFrom(channel, position, count);
        } else {
            bytesTransferred = channel.transferTo(position, count, destChannel);
        }
        if (bytesTransferred >= 0) {
            writeStart += bytesTransferred;
            hasWriteCount += bytesTransferred;
            if (writeStart == writeEnd) {
                // move to next write position
                if (sendRecordPositionList.isEmpty()) {
                    if (hasWriteCount != sizeInBytes()) {
                        throw new KafkaException("bad check write count, except:" + sizeInBytes() + ",actual:" + hasWriteCount);
                    }
                } else {
                    OffsetPair nextOffsetPair = sendRecordPositionList.remove(0);
                    writeStart = nextOffsetPair.beginPos;
                    writeEnd = nextOffsetPair.endPos;
                }
            }
        }
        return bytesTransferred;
    }

    @Override
    public Iterable<? extends RecordBatch> batches() {
        return batches;
    }

    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        List<? extends RecordBatch> batches = Utils.toList(batches().iterator());
        if (batches.isEmpty()) {
            // This indicates that the message is too large, which means that the buffer is not large
            // enough to hold a full record batch. We just return all the bytes in this instance.
            // Even though the record batch does not have the right format version, we expect old clients
            // to raise an error to the user after reading the record batch size and seeing that there
            // are not enough available bytes in the response to read it fully. Note that this is
            // only possible prior to KIP-74, after which the broker was changed to always return at least
            // one full record batch, even if it requires exceeding the max fetch size requested by the client.
            return new ConvertedRecords<>(this, RecordsProcessingStats.EMPTY);
        } else {
            return downConvert(batches, toMagic, firstOffset, time);
        }
    }

    @Override
    protected Iterator<Record> recordsIterator() {
        return new AbstractIterator<Record>() {
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            private Iterator<Record> records;

            @Override
            protected Record makeNext() {
                while (records != null && records.hasNext()) {
                    Record record =  records.next();
                    if (!skippedOffset.contains(record.offset())) {
                        return record;
                    } else {
                        continue;
                    }
                }

                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }

                return allDone();
            }
        };
    }
}
