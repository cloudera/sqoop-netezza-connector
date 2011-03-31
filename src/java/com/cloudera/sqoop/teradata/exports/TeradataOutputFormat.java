// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.exports;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.ExportOutputFormat;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.teradata.util.TemporaryTableGenerator;

/**
 * An ExportOutputFormat that provides a Teradata-specific RecordWriter and
 * OutputCommitter.
 */
public class TeradataOutputFormat<K extends SqoopRecord, V> extends
    ExportOutputFormat<K, V> {

  private static final Log LOG = LogFactory
      .getLog(TeradataOutputFormat.class);

  @Override
  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    try {
      return new TeradataRecordWriter(context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  /** {@inheritDoc} */
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new TeradataExportOutputCommitter();
  }

  /**
   * Writes the records to the task-specific corresponding temporary table in
   * the EDW.
   */
  public class TeradataRecordWriter extends ExportRecordWriter {

    private DBConfiguration dbConf;

    public TeradataRecordWriter(TaskAttemptContext context)
        throws ClassNotFoundException, SQLException, IOException {
      super(context);
      Configuration conf = getConf();

      dbConf = new DBConfiguration(conf);

      // create the temporary table
      TemporaryTableGenerator temporaryTableGenerator =
        new TemporaryTableGenerator(
          dbConf.getOutputTableName(), dbConf.getOutputTableName()
              + conf.get("teradata.export.tables_suffix", "_temp_")
              + conf.getInt("mapred.task.partition", 0), dbConf
              .getConnection(), null, 0);
      temporaryTableGenerator.createExportTempTable();
    }

    @Override
    protected String getInsertStatement(int numRows) {

      if (this.getConf().getBoolean("multi.insert.statements", false) == true) {
        return getInsertMultiStatements(numRows);
      } else {
        return super.getInsertStatement(numRows);
      }

    }

    protected String getInsertMultiStatements(int numRows) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numRows; i++) {
        sb.append("INSERT into " + dbConf.getOutputTableName()
            + getConf().get("teradata.export.tables_suffix", "_temp_")
            + getConf().getInt("mapred.task.partition", 0) + " (");
        String delim = "";
        for (String columnName : getColumnNames()) {
          sb.append(delim);
          sb.append(columnName);
          delim = ", ";
        }
        delim = "";
        sb.append(") values(");
        for (int j = 0; j < getColumnNames().length; j++) {
          sb.append(delim);
          sb.append("?");
          delim = ", ";
        }
        sb.append(")");
        sb.append("; ");
      }
      return sb.toString();
    }

    // /* (non-Javadoc)
    // * @see com.cloudera.sqoop.mapreduce.AsyncSqlRecordWriter#isBatchExec()
    // */
    // @Override
    // protected boolean isBatchExec() {
    // return true;
    // }
  }

}
