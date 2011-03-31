// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.exports;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.mapreduce.JdbcExportJob;

/**
 * An optimized export job that writes the exported data in parallel to
 * temporary tables (one table per task), and when the whole job succeeds, it
 * merges these temporary tables to form the final exported table. 
 */
public class TeradataExportJob extends JdbcExportJob {

  public static final Log LOG = LogFactory.getLog(TeradataExportJob.class
      .getName());

  /**
   * @param context
   */
  public TeradataExportJob(final ExportJobContext context) {
    super(context, null, null, NullOutputFormat.class);
  }

  /*
   * (non-Javadoc)
   *
   * @see com.cloudera.sqoop.mapreduce.ExportJobBase#getOutputFormatClass()
   */
  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass()
      throws ClassNotFoundException {
    return TeradataOutputFormat.class;
  }

}
