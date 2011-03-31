// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.imports;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.DataDrivenImportJob;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;

/**
 * Class that runs an import job
 */
public class TeradataImportJob extends DataDrivenImportJob {

  public TeradataImportJob(SqoopOptions opts) {
    super(opts);
  }

  public TeradataImportJob(SqoopOptions options,
      Class<? extends InputFormat> inputFormat, ImportJobContext context) {
    super(options, inputFormat, context);
  }

  public static final Log LOG = LogFactory.getLog(TeradataImportJob.class
      .getName());

  @Override
  protected void configureInputFormat(Job job, String tableName,
      String tableClassName, String splitByCol) throws IOException {
    ConnManager mgr = getContext().getConnManager();
    try {
      String username = options.getUsername();
      if (null == username || username.length() == 0) {
        DBConfiguration.configureDB(job.getConfiguration(), mgr
            .getDriverClass(), options.getConnectString());
      } else {
        DBConfiguration.configureDB(job.getConfiguration(), mgr
            .getDriverClass(), options.getConnectString(), username, options
            .getPassword());
      }

      if (null != tableName) {
        // Import a table.
        String[] colNames = options.getColumns();
        if (null == colNames) {
          colNames = mgr.getColumnNames(tableName);
        }

        String[] sqlColNames = null;
        if (null != colNames) {
          sqlColNames = new String[colNames.length];
          for (int i = 0; i < colNames.length; i++) {
            sqlColNames[i] = mgr.escapeColName(colNames[i]);
          }
        }
        String whereClause = options.getWhereClause();
        DataDrivenDBInputFormat.setInput(job, DBWritable.class, mgr
            .escapeTableName(tableName), whereClause, mgr
            .escapeColName(splitByCol), sqlColNames);

        TeradataInputFormat.setInput(job, mgr.escapeTableName(tableName),
            options.getNumMappers(), mgr.getColumnNames(tableName));
        LOG.debug("Using InputFormat: " + inputFormatClass);
        job.setInputFormatClass(getInputFormatClass());
      } else {
        // TODO add support for free-form query
        LOG.error("No table name configured. "
            + "Teradata direct mode requires a table-based import.");
        throw new IOException(
            "Cannot import a free-form query with --direct.");
      }

      LOG.debug("Using table class: " + tableClassName);
      job.getConfiguration().set(
          ConfigurationHelper.getDbInputClassProperty(), tableClassName);

      job.getConfiguration().setLong(
          LargeObjectLoader.MAX_INLINE_LOB_LEN_KEY,
          options.getInlineLobLimit());

      LOG.debug("Using InputFormat: " + inputFormatClass);
      job.setInputFormatClass(inputFormatClass);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (SQLException se) {
      throw new IOException(se);
    } finally {
      try {
        mgr.close();
      } catch (SQLException se) {
        LOG.warn("Error closing connection: " + se);
        throw new IOException(se);
      }
    }
  }

}
