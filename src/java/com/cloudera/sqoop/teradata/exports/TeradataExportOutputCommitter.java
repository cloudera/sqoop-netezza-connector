// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.exports;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.teradata.util.TDBQueryExecutor;

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
  private JobConf conf;

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.OutputCommitter#abortTask(org.apache.hadoop.
   * mapred .TaskAttemptContext)
   */
  @Override
  public void abortTask(org.apache.hadoop.mapred.TaskAttemptContext arg0)
      throws IOException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.OutputCommitter#cleanupJob(org.apache.hadoop
   * .mapred.JobContext)
   */
  @Override
  public void cleanupJob(org.apache.hadoop.mapred.JobContext context)
      throws IOException {
    LOG.debug("starting cleanupJob...");
    conf = context.getJobConf();
    DBConfiguration dbConf = new DBConfiguration(conf);
    tableName = dbConf.getOutputTableName();
    numMappers = Integer.valueOf(conf.get("mapred.map.tasks"));
    TDBQueryExecutor queryExecutor = null;
    try {
      queryExecutor = new TDBQueryExecutor(dbConf.getConnection());
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (SQLException se) {
      throw new IOException(se);
    }
    String insertQuery = getInsertQuery();
    LOG.debug("Try executing insert query: " + insertQuery);
    int r = 0;
    try {
      r = queryExecutor.executeUpdate(insertQuery);
    } catch (SQLException se) {
      throw new IOException(se);
    }
    LOG.debug("Query executed, return=" + r);
    try {
      queryExecutor.close();
    } catch (SQLException se) {
      throw new IOException(se);
    }

    if (conf.getBoolean("teradata.export.delete_temporary_tables", true)) {
      for (int i = 0; i < numMappers; i++) {
        String dropQuery = "DROP TABLE " + tableName + conf.get("teradata." +
                          "export.tables_suffix", "_temp_") + i + ";";
        int r2 = 0;
        try {
          r2 = queryExecutor.executeUpdate(dropQuery);
        } catch (SQLException se) {
          throw new IOException(se);
        }
        LOG.debug("Query executed, return=" + r2);
      }
    }
    try {
      queryExecutor.close();
    } catch (SQLException se) {
      throw new IOException(se);
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
      sb.append(" SELECT * FROM " + tableName + conf.get("teradata.export." +
                "tables_suffix", "_temp_") + i);
      delim = " UNION ALL";
    }
    sb.append(";");
    return sb.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.OutputCommitter#commitTask(org.apache.hadoop
   * .mapred .TaskAttemptContext)
   */
  @Override
  public void commitTask(org.apache.hadoop.mapred.TaskAttemptContext arg0)
      throws IOException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.OutputCommitter#needsTaskCommit(org.apache.hadoop
   * .mapred.TaskAttemptContext)
   */
  @Override
  public boolean needsTaskCommit(
      org.apache.hadoop.mapred.TaskAttemptContext arg0) throws IOException {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.OutputCommitter#setupJob(org.apache.hadoop.mapred
   * .JobContext)
   */
  @Override
  public void setupJob(org.apache.hadoop.mapred.JobContext arg0)
      throws IOException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.mapred.OutputCommitter#setupTask(org.apache.hadoop.
   * mapred .TaskAttemptContext)
   */
  @Override
  public void setupTask(org.apache.hadoop.mapred.TaskAttemptContext arg0)
      throws IOException {
  }
}
