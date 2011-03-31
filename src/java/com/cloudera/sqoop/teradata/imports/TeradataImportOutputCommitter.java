// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.imports;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileOutputCommitter;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.teradata.util.TDBQueryExecutor;

public class TeradataImportOutputCommitter extends FileOutputCommitter {

  public static final Log LOG = LogFactory
      .getLog(TeradataImportOutputCommitter.class.getName());

  @Override
  public void cleanupJob(org.apache.hadoop.mapred.JobContext context)
      throws IOException {
    super.cleanupJob(context);
    Configuration conf = context.getConfiguration();
    DBConfiguration dbConf = new DBConfiguration(conf);
    String tableName = dbConf.getInputTableName();
    TDBQueryExecutor queryExecutor = null;
    try {
      queryExecutor = new TDBQueryExecutor(dbConf.getConnection());
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (SQLException se) {
      throw new IOException(se);
    }

    if (conf.getBoolean("teradata.import.delete_temporary_table", true)) {
      String dropQuery = "DROP TABLE " + tableName + conf.get("teradata." +
                "import.table_suffix","_temp");
      LOG.debug("Try executing drop query: " + dropQuery);
      int r2 = 0;
      try {
        r2 = queryExecutor.executeUpdate(dropQuery);
      } catch (SQLException se) {
        throw new IOException(se);
      }
      LOG.debug("Query executed, return=" + r2);
    }
    try {
      queryExecutor.close();
    } catch (SQLException se) {
      throw new IOException(se);
    }
  }

}
