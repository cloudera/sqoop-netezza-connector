// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sqoop.shims.HadoopShim;

/**
 * InputFormat to facilitate direct-mode import from Netezza.
 */
public class NetezzaImportInputFormat
    extends InputFormat<Integer, NullWritable> {

  /**
   * A split that just holds an integer, representing a partition
   * of the input.
   */
  public static class IntSplit extends InputSplit implements Writable {
    private int val; // The integer value.

    public IntSplit() {
      this.val = 0;
    }

    public IntSplit(int i) {
      this.val = i;
    }

    public int getValue() {
      return val;
    }

    @Override
    public long getLength() {
      return 0L;
    }

    @Override
    public String [] getLocations() {
      // MapReduce locality is unimportant.
      return new String[0];
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.val = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(this.val);
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException {
    // Input data in Netezza is sharded across a bunch of hash buckets.
    // Each map task is to read 1/n'th of the data. So we assign all
    // the rows with hash % num_mappers = k to the k'th mapper. Enumerate
    // a list of values 0..k-1.

    int numMappers = HadoopShim.get().getJobNumMaps(context);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < numMappers; i++) {
      splits.add(new IntSplit(i));
    }

    return splits;
  }

  @Override
  public RecordReader<Integer, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new NetezzaImportRecordReader(split, context);
  }
}
