// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.exports;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.teradata.util.TDBQueryExecutor;
import com.cloudera.sqoop.teradata.util.TeradataConstants;

/**
 * An OutputCommitter that takes care of export job cleanup. It merges the
 * individual temporary tables into the final exported table, and finally
 * delete these temporary tables.
 */
public class TeradataExportOutputCommitter extends OutputCommitter {

  public static final Log LOG = LogFactory
      .getLog(TeradataExportOutputCommitter.class.getName());
  private String tableName;
  private int numMappers;
  private Configuration conf;


  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.OutputCommitter#cleanupJob(org.apache.hadoop
   * .mapred.JobContext)
   */
  @Override
  public void cleanupJob(JobContext context)
      throws IOException {
    LOG.debug("starting cleanupJob...");
    conf = context.getConfiguration();
    DBConfiguration dbConf = new DBConfiguration(conf);
    tableName = dbConf.getOutputTableName();
    numMappers = Integer.valueOf(conf.get("mapred.map.tasks"));
    TDBQueryExecutor queryExecutor = new TDBQueryExecutor(dbConf);
    String insertQuery = getInsertQuery();
    LOG.debug("Try executing insert query: " + insertQuery);
    int r = 0;
    try {
      r = queryExecutor.executeUpdate(insertQuery);
    } catch (Exception e) {
      throw new IOException(e);
    }
    LOG.debug("Query executed, return = " + r);

    if (conf.getBoolean(TeradataConstants.EXPORT_DELETE_TEMP_TABLES, true)) {
      for (int i = 0; i < numMappers; i++) {
        String dropQuery = "DROP TABLE " + tableName
            + conf.get(TeradataConstants.EXPORT_TEMP_TABLES_SUFFIX, "_temp_")
            + i + ";";
        int r2 = 0;
        try {
          r2 = queryExecutor.executeUpdate(dropQuery);
        } catch (Exception e) {
          throw new IOException(e);
        }
        LOG.debug("Query executed, return = " + r2);
      }
    }
  }

  /**
   * @return the SQL insert query to merge data from temporary tables to the 
   * final table
   */
  private String getInsertQuery() {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO " + tableName);
    String delim = "";
    for (int i = 0; i < numMappers; i++) {
      sb.append(delim);
      sb.append(" SELECT * FROM " + tableName
          + conf.get(TeradataConstants.EXPORT_TEMP_TABLES_SUFFIX, "_temp_")
          + i);
      delim = " UNION ALL";
    }
    sb.append(";");
    return sb.toString();
  }

  @Override
  public void abortTask(TaskAttemptContext arg0) throws IOException {
  }

  @Override
  public void commitTask(TaskAttemptContext arg0) throws IOException {
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
    return false;
  }

  @Override
  public void setupJob(JobContext arg0) throws IOException {
  }

  @Override
  public void setupTask(TaskAttemptContext arg0) throws IOException {
  }

}
