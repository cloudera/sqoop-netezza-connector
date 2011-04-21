// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.imports;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.teradata.util.TDBQueryExecutor;
import com.cloudera.sqoop.teradata.util.TeradataConstants;

/**
 * An OutputCommitter that takes care of import job cleanup.
 */
public class TeradataImportOutputCommitter extends FileOutputCommitter {

  public TeradataImportOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }

  public static final Log LOG = LogFactory
  .getLog(TeradataImportOutputCommitter.class.getName());

  @Override
  public void cleanupJob(JobContext context)
      throws IOException {
    super.cleanupJob(context);
    Configuration conf = context.getConfiguration();
    DBConfiguration dbConf = new DBConfiguration(conf);
    String tableName = dbConf.getInputTableName();
    TDBQueryExecutor queryExecutor = new TDBQueryExecutor(dbConf);
    if (conf.getBoolean(TeradataConstants.IMPORT_DELETE_TEMP_TABLE, true)) {
      String dropQuery = "DROP TABLE " + tableName
          + conf.get(TeradataConstants.IMPORT_TEMP_TABLE_SUFFIX, "_temp");
      LOG.debug("Try executing drop query: " + dropQuery);
      int r2 = 0;
      try {
        r2 = queryExecutor.executeUpdate(dropQuery);
      } catch (Exception e) {
        throw new IOException(e);
      }
      LOG.debug("Query executed, return=" + r2);
    }
  }

}
