package com.aliyun.dts.subscribe.clients.formats.avro;

import com.aliyun.dts.subscribe.clients.record.value.*;

public class DefaultValueDeserializer {

    public static Value deserialize(Object data) {

        if (null == data) {
            return null;
        }

        if (data instanceof String) {
            return new StringValue((String) data);
        }

        if (data instanceof Integer) {
            Integer integer = (Integer) data;
            return new IntegerNumeric(integer.getValue());
        }

        if (data instanceof Character) {
            Character character = (Character) data;
            return new StringValue(character.getValue(), character.getCharset());
        }

        if (data instanceof TextObject) {
            TextObject textObject = (TextObject) data;
            return new TextEncodingObject(ObjectType.valueOf(textObject.getType().toUpperCase()), textObject.getValue());
        }

        if (data instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) data;

            UnixTimestamp unixTimestamp = new UnixTimestamp();
            unixTimestamp.setTimestampSec(timestamp.getTimestamp());
            if (null != timestamp.getMillis()) {
                unixTimestamp.setMicro(timestamp.getMillis());
            }
            return unixTimestamp;
        }

        if (data instanceof DateTime) {
            DateTime aDt = (DateTime) data;
            return deserialize(aDt);
        }

        if (data instanceof TimestampWithTimeZone) {

            TimestampWithTimeZone timestampWithTimeZone = (TimestampWithTimeZone) data;
            com.aliyun.dts.subscribe.clients.record.value.DateTime dt = deserialize(timestampWithTimeZone.getValue());
            dt.setTimeZone(timestampWithTimeZone.getTimezone());
            return dt;
        }

        if (data instanceof BinaryObject) {
            BinaryObject binaryObject = (BinaryObject) data;
            return new BinaryEncodingObject(ObjectType.valueOf(binaryObject.getType().toUpperCase()), binaryObject.getValue());
        }

        if (data instanceof Float) {
            Float aFloat = (Float) data;
            return new FloatNumeric(aFloat.getValue());
        }

        if (data instanceof Decimal) {
            Decimal decimal = (Decimal) data;
            return new DecimalNumeric(decimal.getValue());
        }

        if (data instanceof BinaryGeometry) {
            BinaryGeometry geometry = (BinaryGeometry) data;
            return new WKBGeometry(geometry.getValue());
        }

        if (data instanceof TextGeometry) {
            TextGeometry geometry = (TextGeometry) data;
            return new WKTGeometry(geometry.getValue());
        }

        if (data == EmptyObject.NONE) {
            return new NoneValue();
        }

        throw new RuntimeException("Not support avro class type:" + data.getClass().getName());
    }

    static com.aliyun.dts.subscribe.clients.record.value.DateTime deserialize(DateTime inDt)  {
        com.aliyun.dts.subscribe.clients.record.value.DateTime dt = new com.aliyun.dts.subscribe.clients.record.value.DateTime();
        if (null != inDt.getYear()) {
            dt.setSegments(com.aliyun.dts.subscribe.clients.record.value.DateTime.SEG_YEAR);
            dt.setYear(inDt.getYear());
        }

        if (null != inDt.getMonth()) {
            dt.setSegments(com.aliyun.dts.subscribe.clients.record.value.DateTime.SEG_MONTH);
            dt.setMonth(inDt.getMonth());
        }

        if (null != inDt.getDay()) {
            dt.setSegments(com.aliyun.dts.subscribe.clients.record.value.DateTime.SEG_DAY);
            dt.setDay(inDt.getDay());
        }

        if (null != inDt.getHour()) {
            dt.setSegments(com.aliyun.dts.subscribe.clients.record.value.DateTime.SEG_TIME);
            dt.setHour(inDt.getHour());
            dt.setMinute(inDt.getMinute());
            dt.setSecond(inDt.getSecond());
        }
        if (null != inDt.getMillis()) {
            dt.setSegments(com.aliyun.dts.subscribe.clients.record.value.DateTime.SEG_NAONS);
            dt.setNaons(inDt.getMillis() * 1000);
        }
        return dt;
    }
}
