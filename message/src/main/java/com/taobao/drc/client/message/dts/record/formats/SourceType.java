
package com.taobao.drc.client.message.dts.record.formats;
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public enum SourceType {
  MySQL, Oracle, SQLServer, PostgreSQL, MongoDB, Redis, DB2, PPAS, DRDS, HBASE, HDFS, FILE, TIDB, OTHER  ;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"SourceType\",\"namespace\":\"com.alibaba.amp.any.common.record.formats.avro\",\"symbols\":[\"MySQL\",\"Oracle\",\"SQLServer\",\"PostgreSQL\",\"MongoDB\",\"Redis\",\"DB2\",\"PPAS\",\"DRDS\",\"HBASE\",\"HDFS\",\"FILE\",\"TIDB\",\"OTHER\"]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
}
