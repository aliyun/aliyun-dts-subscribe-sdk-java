package com.aliyun.dts.subscribe.clients.record.value;


public class SpecialNumeric implements Value<SpecialNumeric.SpecialNumericType> {

    private static final String NAN = "NaN";
    private static final String INFINITY = "Infinity";
    private static final String NEGATIVE_INFINITY = "-Infinity";
    private static final String NEAR = "~";

    private SpecialNumericType value;

    public SpecialNumeric(SpecialNumericType value) {
        this.value = value;
    }

    public SpecialNumeric(String text) {
        this(SpecialNumericType.parseFrom(text));
    }

    @Override
    public ValueType getType() {
        return ValueType.SPECIAL_NUMERIC;
    }

    @Override
    public SpecialNumericType getData() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public long size() {
        return Integer.BYTES;
    }

    public enum SpecialNumericType {
        NOT_ASSIGNED,
        INFINITY,
        NEGATIVE_INFINITY,
        NOT_A_NUMBER,
        NAN,
        NEAR;

        public static SpecialNumericType parseFrom(String value) {
            if (SpecialNumeric.NAN.equals(value)) {
                return NAN;
            }
            if (SpecialNumeric.NEAR.equals(value)) {
                return NEAR;
            }
            if (SpecialNumeric.INFINITY.equals(value)) {
                return INFINITY;
            }
            if (SpecialNumeric.NEGATIVE_INFINITY.equals(value)) {
                return NEGATIVE_INFINITY;
            }
            return SpecialNumericType.valueOf(value);
        }

        @Override
        public String toString() {
            if (this.equals(NAN)) {
                return SpecialNumeric.NAN;
            }
            if (this.equals(NEAR)) {
                return SpecialNumeric.NEAR;
            }
            if (this.equals(INFINITY)) {
                return SpecialNumeric.INFINITY;
            }
            if (this.equals(NEGATIVE_INFINITY)) {
                return SpecialNumeric.NEGATIVE_INFINITY;
            }
            return this.name();
        }
    }
}
