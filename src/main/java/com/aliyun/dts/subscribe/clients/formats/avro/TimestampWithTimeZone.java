/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.aliyun.dts.subscribe.clients.formats.avro;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class TimestampWithTimeZone extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -5089347690050137038L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TimestampWithTimeZone\",\"namespace\":\"com.alibaba.dts.formats.avro\",\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"record\",\"name\":\"DateTime\",\"fields\":[{\"name\":\"year\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"month\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"day\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"hour\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"minute\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"second\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"millis\",\"type\":[\"null\",\"int\"],\"default\":null}]}},{\"name\":\"timezone\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<TimestampWithTimeZone> ENCODER =
      new BinaryMessageEncoder<TimestampWithTimeZone>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<TimestampWithTimeZone> DECODER =
      new BinaryMessageDecoder<TimestampWithTimeZone>(MODEL$, SCHEMA$);

  public static BinaryMessageDecoder<TimestampWithTimeZone> getDecoder() {
    return DECODER;
  }

  public static BinaryMessageDecoder<TimestampWithTimeZone> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<TimestampWithTimeZone>(MODEL$, SCHEMA$, resolver);
  }

  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  public static TimestampWithTimeZone fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public DateTime value;
  @Deprecated public String timezone;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public TimestampWithTimeZone() {}

  /**
   * All-args constructor.
   * @param value The new value for value
   * @param timezone The new value for timezone
   */
  public TimestampWithTimeZone(DateTime value, String timezone) {
    this.value = value;
    this.timezone = timezone;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return value;
    case 1: return timezone;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: value = (DateTime)value$; break;
    case 1: timezone = (String)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'value' field.
   * @return The value of the 'value' field.
   */
  public DateTime getValue() {
    return value;
  }

  /**
   * Sets the value of the 'value' field.
   * @param value the value to set.
   */
  public void setValue(DateTime value) {
    this.value = value;
  }

  /**
   * Gets the value of the 'timezone' field.
   * @return The value of the 'timezone' field.
   */
  public String getTimezone() {
    return timezone;
  }

  /**
   * Sets the value of the 'timezone' field.
   * @param value the value to set.
   */
  public void setTimezone(String value) {
    this.timezone = value;
  }

  /**
   * Creates a new TimestampWithTimeZone RecordBuilder.
   * @return A new TimestampWithTimeZone RecordBuilder
   */
  public static TimestampWithTimeZone.Builder newBuilder() {
    return new TimestampWithTimeZone.Builder();
  }

  /**
   * Creates a new TimestampWithTimeZone RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new TimestampWithTimeZone RecordBuilder
   */
  public static TimestampWithTimeZone.Builder newBuilder(TimestampWithTimeZone.Builder other) {
    return new TimestampWithTimeZone.Builder(other);
  }

  /**
   * Creates a new TimestampWithTimeZone RecordBuilder by copying an existing TimestampWithTimeZone instance.
   * @param other The existing instance to copy.
   * @return A new TimestampWithTimeZone RecordBuilder
   */
  public static TimestampWithTimeZone.Builder newBuilder(TimestampWithTimeZone other) {
    return new TimestampWithTimeZone.Builder(other);
  }

  /**
   * RecordBuilder for TimestampWithTimeZone instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TimestampWithTimeZone>
    implements org.apache.avro.data.RecordBuilder<TimestampWithTimeZone> {

    private DateTime value;
    private DateTime.Builder valueBuilder;
    private String timezone;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(TimestampWithTimeZone.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.value)) {
        this.value = data().deepCopy(fields()[0].schema(), other.value);
        fieldSetFlags()[0] = true;
      }
      if (other.hasValueBuilder()) {
        this.valueBuilder = DateTime.newBuilder(other.getValueBuilder());
      }
      if (isValidValue(fields()[1], other.timezone)) {
        this.timezone = data().deepCopy(fields()[1].schema(), other.timezone);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing TimestampWithTimeZone instance
     * @param other The existing instance to copy.
     */
    private Builder(TimestampWithTimeZone other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.value)) {
        this.value = data().deepCopy(fields()[0].schema(), other.value);
        fieldSetFlags()[0] = true;
      }
      this.valueBuilder = null;
      if (isValidValue(fields()[1], other.timezone)) {
        this.timezone = data().deepCopy(fields()[1].schema(), other.timezone);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'value' field.
      * @return The value.
      */
    public DateTime getValue() {
      return value;
    }

    /**
      * Sets the value of the 'value' field.
      * @param value The value of 'value'.
      * @return This builder.
      */
    public TimestampWithTimeZone.Builder setValue(DateTime value) {
      validate(fields()[0], value);
      this.valueBuilder = null;
      this.value = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'value' field has been set.
      * @return True if the 'value' field has been set, false otherwise.
      */
    public boolean hasValue() {
      return fieldSetFlags()[0];
    }

    /**
     * Gets the Builder instance for the 'value' field and creates one if it doesn't exist yet.
     * @return This builder.
     */
    public DateTime.Builder getValueBuilder() {
      if (valueBuilder == null) {
        if (hasValue()) {
          setValueBuilder(DateTime.newBuilder(value));
        } else {
          setValueBuilder(DateTime.newBuilder());
        }
      }
      return valueBuilder;
    }

    /**
     * Sets the Builder instance for the 'value' field
     * @param value The builder instance that must be set.
     * @return This builder.
     */
    public TimestampWithTimeZone.Builder setValueBuilder(DateTime.Builder value) {
      clearValue();
      valueBuilder = value;
      return this;
    }

    /**
     * Checks whether the 'value' field has an active Builder instance
     * @return True if the 'value' field has an active Builder instance
     */
    public boolean hasValueBuilder() {
      return valueBuilder != null;
    }

    /**
      * Clears the value of the 'value' field.
      * @return This builder.
      */
    public TimestampWithTimeZone.Builder clearValue() {
      value = null;
      valueBuilder = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'timezone' field.
      * @return The value.
      */
    public String getTimezone() {
      return timezone;
    }

    /**
      * Sets the value of the 'timezone' field.
      * @param value The value of 'timezone'.
      * @return This builder.
      */
    public TimestampWithTimeZone.Builder setTimezone(String value) {
      validate(fields()[1], value);
      this.timezone = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'timezone' field has been set.
      * @return True if the 'timezone' field has been set, false otherwise.
      */
    public boolean hasTimezone() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'timezone' field.
      * @return This builder.
      */
    public TimestampWithTimeZone.Builder clearTimezone() {
      timezone = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TimestampWithTimeZone build() {
      try {
        TimestampWithTimeZone record = new TimestampWithTimeZone();
        if (valueBuilder != null) {
          record.value = this.valueBuilder.build();
        } else {
          record.value = fieldSetFlags()[0] ? this.value : (DateTime) defaultValue(fields()[0]);
        }
        record.timezone = fieldSetFlags()[1] ? this.timezone : (String) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<TimestampWithTimeZone>
    WRITER$ = (org.apache.avro.io.DatumWriter<TimestampWithTimeZone>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<TimestampWithTimeZone>
    READER$ = (org.apache.avro.io.DatumReader<TimestampWithTimeZone>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
