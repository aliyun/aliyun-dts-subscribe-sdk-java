package com.aliyun.dts.subscribe.clients.recordgenerator;

import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroDeserializer {
    private static final Logger log = LoggerFactory.getLogger(AvroDeserializer.class);

    private final SpecificDatumReader<Record> reader = new SpecificDatumReader<Record>(Record.class);

    public AvroDeserializer() {
    }

    public Record deserialize(byte[] data) {

        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        Record payload = null;
        try {
            payload = reader.read(null, decoder);
            return payload;
        }catch (Throwable ex) {
            log.error("AvroDeserializer: deserialize record failed cause " + ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }
}
