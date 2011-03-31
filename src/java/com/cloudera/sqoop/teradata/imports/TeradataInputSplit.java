// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.imports;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TeradataInputSplit extends
    com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat.DBInputSplit
    implements Writable {
  private long partition = 0;

  /**
   * Default constructor
   */
  public TeradataInputSplit() {
  }

  /**
   * @param startPartition
   * @param endPartition
   * @param logNode
   */
  public TeradataInputSplit(long partition) {
    this.partition = partition;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
   */
  @Override
  public long getLength() throws IOException {
    return 1;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
   */
  @Override
  public String[] getLocations() throws IOException {
    // TODO For optimization
    return new String[] {};
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    this.partition = input.readLong();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(partition);
  }

}
