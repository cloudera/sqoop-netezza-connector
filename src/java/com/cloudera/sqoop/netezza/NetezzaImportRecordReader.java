// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * InputFormat to facilitate direct-mode import from Netezza.
 * The input is a single integer, so we deliver that integer and then stop.
 */
public class NetezzaImportRecordReader
    extends RecordReader<Integer, NullWritable> {

  private InputSplit split;
  private boolean used;

  public NetezzaImportRecordReader(InputSplit input, TaskAttemptContext ctxt) {
    initialize(input, ctxt);
  }

  public void initialize(InputSplit input, TaskAttemptContext ctxt) {
    this.split = input;
    this.used = false;
  }

  @Override
  public boolean nextKeyValue() {
    if (used) {
      return false;
    } else {
      used = true;
      return true;
    }
  }

  @Override
  public Integer getCurrentKey() {
    return ((NetezzaImportInputFormat.IntSplit) this.split).getValue();
  }

  @Override
  public NullWritable getCurrentValue() {
    return NullWritable.get();
  }

  @Override
  public float getProgress() {
    return used ? 1.0f : 0.0f;
  }

  @Override
  public void close() {
  }
}
