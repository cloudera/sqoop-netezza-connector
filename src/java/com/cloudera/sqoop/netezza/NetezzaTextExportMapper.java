// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Netezza exporter which accepts SqoopRecords (e.g., from
 * SequenceFiles) to emit to the database.
 */
public class NetezzaTextExportMapper
    extends NetezzaExportMapper<LongWritable, Text> {

  /**
   * Export the table to netezza.
   *
   * Expects one line of text (one record) as the value. Ignores the key.
   */
  @Override
  public void map(LongWritable key, Text val, Context context)
      throws IOException, InterruptedException {

    writeRecord(val);

    // We don't emit anything to the OutputCollector because we wrote
    // straight to the fifo. Send a progress indicator to prevent a timeout.
    context.progress();
  }
}
