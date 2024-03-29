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
public class BinaryGeometry extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -9183362578343147886L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"BinaryGeometry\",\"namespace\":\"com.alibaba.dts.formats.avro\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"value\",\"type\":\"bytes\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<BinaryGeometry> ENCODER =
      new BinaryMessageEncoder<BinaryGeometry>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<BinaryGeometry> DECODER =
      new BinaryMessageDecoder<BinaryGeometry>(MODEL$, SCHEMA$);

  /**
   * @return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<BinaryGeometry> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return BinaryMessageDecoder
   */
  public static BinaryMessageDecoder<BinaryGeometry> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<BinaryGeometry>(MODEL$, SCHEMA$, resolver);
  }

  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  public static BinaryGeometry fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public String type;
  @Deprecated public java.nio.ByteBuffer value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public BinaryGeometry() {}

  /**
   * All-args constructor.
   * @param type The new value for type
   * @param value The new value for value
   */
  public BinaryGeometry(String type, java.nio.ByteBuffer value) {
    this.type = type;
    this.value = value;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return type;
    case 1: return value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: type = (String)value$; break;
    case 1: value = (java.nio.ByteBuffer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'type' field.
   * @return The value of the 'type' field.
   */
  public String getType() {
    return type;
  }

  /**
   * Sets the value of the 'type' field.
   * @param value the value to set.
   */
  public void setType(String value) {
    this.type = value;
  }

  /**
   * Gets the value of the 'value' field.
   * @return The value of the 'value' field.
   */
  public java.nio.ByteBuffer getValue() {
    return value;
  }

  /**
   * Sets the value of the 'value' field.
   * @param value the value to set.
   */
  public void setValue(java.nio.ByteBuffer value) {
    this.value = value;
  }

  /**
   * Creates a new BinaryGeometry RecordBuilder.
   * @return A new BinaryGeometry RecordBuilder
   */
  public static BinaryGeometry.Builder newBuilder() {
    return new BinaryGeometry.Builder();
  }

  /**
   * Creates a new BinaryGeometry RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new BinaryGeometry RecordBuilder
   */
  public static BinaryGeometry.Builder newBuilder(BinaryGeometry.Builder other) {
    return new BinaryGeometry.Builder(other);
  }

  /**
   * Creates a new BinaryGeometry RecordBuilder by copying an existing BinaryGeometry instance.
   * @param other The existing instance to copy.
   * @return A new BinaryGeometry RecordBuilder
   */
  public static BinaryGeometry.Builder newBuilder(BinaryGeometry other) {
    return new BinaryGeometry.Builder(other);
  }

  /**
   * RecordBuilder for BinaryGeometry instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<BinaryGeometry>
    implements org.apache.avro.data.RecordBuilder<BinaryGeometry> {

    private String type;
    private java.nio.ByteBuffer value;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(BinaryGeometry.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.type)) {
        this.type = data().deepCopy(fields()[0].schema(), other.type);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.value)) {
        this.value = data().deepCopy(fields()[1].schema(), other.value);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing BinaryGeometry instance
     * @param other The existing instance to copy.
     */
    private Builder(BinaryGeometry other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.type)) {
        this.type = data().deepCopy(fields()[0].schema(), other.type);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.value)) {
        this.value = data().deepCopy(fields()[1].schema(), other.value);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'type' field.
      * @return The value.
      */
    public String getType() {
      return type;
    }

    /**
      * Sets the value of the 'type' field.
      * @param value The value of 'type'.
      * @return This builder.
      */
    public BinaryGeometry.Builder setType(String value) {
      validate(fields()[0], value);
      this.type = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'type' field has been set.
      * @return True if the 'type' field has been set, false otherwise.
      */
    public boolean hasType() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'type' field.
      * @return This builder.
      */
    public BinaryGeometry.Builder clearType() {
      type = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'value' field.
      * @return The value.
      */
    public java.nio.ByteBuffer getValue() {
      return value;
    }

    /**
      * Sets the value of the 'value' field.
      * @param value The value of 'value'.
      * @return This builder.
      */
    public BinaryGeometry.Builder setValue(java.nio.ByteBuffer value) {
      validate(fields()[1], value);
      this.value = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'value' field has been set.
      * @return True if the 'value' field has been set, false otherwise.
      */
    public boolean hasValue() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'value' field.
      * @return This builder.
      */
    public BinaryGeometry.Builder clearValue() {
      value = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BinaryGeometry build() {
      try {
        BinaryGeometry record = new BinaryGeometry();
        record.type = fieldSetFlags()[0] ? this.type : (String) defaultValue(fields()[0]);
        record.value = fieldSetFlags()[1] ? this.value : (java.nio.ByteBuffer) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<BinaryGeometry>
    WRITER$ = (org.apache.avro.io.DatumWriter<BinaryGeometry>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<BinaryGeometry>
    READER$ = (org.apache.avro.io.DatumReader<BinaryGeometry>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
