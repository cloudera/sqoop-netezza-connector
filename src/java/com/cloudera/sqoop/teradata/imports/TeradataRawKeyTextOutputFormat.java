// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.imports;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.sqoop.mapreduce.RawKeyTextOutputFormat;

/**
 * A Teradata RawKeyTextOutputFormat that provides the OutputCommitter
 * responsible for job cleanup.
 */
public class TeradataRawKeyTextOutputFormat<K, V> extends
    RawKeyTextOutputFormat<K, V> {

  @Override
  /** {@inheritDoc} */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    return new TeradataImportOutputCommitter(FileOutputFormat
        .getOutputPath(context), context);
  }

}
