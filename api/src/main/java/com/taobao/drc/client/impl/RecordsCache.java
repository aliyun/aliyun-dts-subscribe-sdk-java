package com.taobao.drc.client.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.DataMessage.Record;

/*
* TBD: Add the strategy to package one huge transaction and
* allow the parameters for records and timeout per batch.
*/
public class RecordsCache {
  private int maxRecordsCached = 10240;

  private int maxTxnsBatched = 10240;

  private int maxRecordsBatched = 1024;

  private long maxTimeoutBatched = 500;

  private long maxBytesCached = Long.MAX_VALUE;

  private boolean inTransaction=false;

  private List<Record> readyRecords=new ArrayList<Record>();

  private long numOfTxnsInReadyRecords;

  private DataMessage cachedMessage;

  private List<Record> records=new ArrayList<Record>();

  private long lastFetchedTime = System.currentTimeMillis();
  private long bytesOfRecordsCached = 0;
  public RecordsCache() {
  }

  /**
  * The copy constructor to copy parameters from another instance.
  */
  public RecordsCache(RecordsCache that) {
      this.maxRecordsCached = that.maxRecordsCached;
      this.maxTxnsBatched = that.maxTxnsBatched;
      this.maxRecordsBatched = that.maxRecordsBatched;
      this.maxTimeoutBatched = that.maxTimeoutBatched;
      this.maxBytesCached = that.maxBytesCached;
  }

  public void setMaxRecordsCached(int count) {
      maxRecordsCached = count;
  }

  public void setmaxRecordsBatched(int count) {
      maxRecordsBatched = count;
  }

  public void setMaxTxnsBatched(int count) {
      maxTxnsBatched = count;
  }

  public void setmaxTimeoutBatched(long timeout) {
      maxTimeoutBatched = timeout;
  }

  public void setMaxBytesCached(long count) {
      maxBytesCached = count;
  }

  public boolean isReady() {
      if (readyRecords.size() >= maxRecordsBatched ||
              numOfTxnsInReadyRecords >= maxTxnsBatched ||
              bytesOfRecordsCached >= maxBytesCached)
          return true;
      long now = System.currentTimeMillis();
      if (!readyRecords.isEmpty() && now - lastFetchedTime >= maxTimeoutBatched)
          return true;
      return false;
  }

  public DataMessage getReadyRecords() {
      cachedMessage = new DataMessage();
      for (Record r : readyRecords) {
          cachedMessage.addRecord(r);
      }
      readyRecords.clear();
      numOfTxnsInReadyRecords = 0;
      bytesOfRecordsCached = 0;
      lastFetchedTime = System.currentTimeMillis();
      return cachedMessage;
  }

  public void addRecord(Record r) throws IOException {
      switch (r.getOpt()) {
      case HEARTBEAT:
          if (inTransaction == false)
              readyRecords.add(r);
          else
              records.add(r);
          break;
      case BEGIN:
          if (inTransaction == true)
              records.clear();
          records.add(r);
          inTransaction = true;
          break;
      case COMMIT:
      case ROLLBACK:
          if (inTransaction == false)
              records.clear();
          else {
              inTransaction = false;
              records.add(r);
              readyRecords.addAll(records);
              numOfTxnsInReadyRecords ++;
              bytesOfRecordsCached += r.getRecordLength();
              records.clear();
          }
          break;
      case INSERT:
      case DELETE:
      case UPDATE:
      case REPLACE:
          if (inTransaction == false) {
              records.clear();
          } else {
              records.add(r);
              bytesOfRecordsCached += r.getRecordLength();
              if (records.size() >= maxRecordsCached || bytesOfRecordsCached >= maxBytesCached) {
                  readyRecords.addAll(records);
                  records.clear();
              }
          }
          break;
      case DDL:
      default:
          if (inTransaction == true)
              records.add(r);
          else
              readyRecords.add(r);
          break;
      }
  }
}
