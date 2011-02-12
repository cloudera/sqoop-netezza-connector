// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.cloudera.sqoop.mapreduce.ImportJobBase;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.MySQLUtils;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.shims.ShimLoader;

/**
 * Class that runs an import job using netezza remote external tables.
 */
public class NetezzaImportJob extends ImportJobBase {

  public static final Log LOG =
      LogFactory.getLog(NetezzaImportJob.class.getName());

  public NetezzaImportJob(ImportJobContext context)
      throws ClassNotFoundException {
    super(context.getOptions(), NetezzaImportMapper.class,
        NetezzaImportInputFormat.class,
        (Class<? extends OutputFormat>) ShimLoader.getShimClass(
            "com.cloudera.sqoop.mapreduce.RawKeyTextOutputFormat"), context);
  }

  /**
   * Configure the inputformat to use for the job.
   */
  protected void configureInputFormat(Job job, String tableName,
      String tableClassName, String splitByCol)
      throws ClassNotFoundException, IOException {

    if (null == tableName) {
      LOG.error("No table name configured. "
          + "Netezza direct mode requires a table-based import.");
      throw new IOException("Cannot import a free-form query with --direct.");
    }

    ConnManager mgr = getContext().getConnManager();
    String username = options.getUsername();
    if (null == username || username.length() == 0) {
      DBConfiguration.configureDB(job.getConfiguration(),
          mgr.getDriverClass(), options.getConnectString());
    } else {
      DBConfiguration.configureDB(job.getConfiguration(),
          mgr.getDriverClass(), options.getConnectString(), username,
          options.getPassword());
    }

    String [] colNames = options.getColumns();
    if (null == colNames) {
      colNames = mgr.getColumnNames(tableName);
    }

    String [] sqlColNames = null;
    if (null != colNames) {
      sqlColNames = new String[colNames.length];
      for (int i = 0; i < colNames.length; i++) {
        sqlColNames[i] = mgr.escapeColName(colNames[i]);
      }
    }

    // It's ok if the where clause is null in DBInputFormat.setInput.
    String whereClause = options.getWhereClause();

    DataDrivenDBInputFormat.setInput(job, DBWritable.class,
        tableName, whereClause,
        mgr.escapeColName(splitByCol), sqlColNames);

    // Check the user's delimiters and warn if they're not the same
    // as Netezza's. Save the field delim and escape characters for
    // use in the mapper.
    char field = options.getOutputFieldDelim();
    char record = options.getOutputRecordDelim();
    char escape = options.getOutputEscapedBy();
    char enclose = options.getOutputEnclosedBy();

    if (enclose != '\000') {
      LOG.warn("Netezza does not support --enclosed-by. Ignoring.");
    }

    // Netezza requires that the escape character be set to '\\'. If this is
    // not the case, then the JDBC connection will hang if it tries to import
    // an un-escaped value. So if it's not set, or incorrectly set, we set it
    // here. Unfortunately, this will not change the generated code for the
    // user, so their parse method will be incorrect (this is too late in the
    // flow).  TODO: We should really add some sort of
    // ConnManager.validate(JobData data) method that gets called in
    // SqoopTool.init(); after the SqoopTool.validateOptions() method, but
    // before the rest of SqoopTool.run() takes off, so that the ConnManager
    // has a chance to adjust the configuration at the start of the job.
    if (escape == '\000') {
      LOG.warn("Netezza requires the '\\' escape character. Enabling "
          + "escaped-by. Note that the generated parse() method will not "
          + "be able to detect this condition. You should regenerate any "
          + "code you plan to use with sqoop codegen --escaped-by '\\' ...");
      options.setEscapedBy('\\');
      escape = '\\';
    } else if (escape != '\\') {
      LOG.warn("Netezza requires the '\\' escape character. Forcing "
          + "escaped-by to this setting for the import. Note that the "
          + "generated parse() method will not be able to detect this "
          + "condition. You should regenerate any code you plan to use "
          + "with sqoop codegen --escaped-by '\\' ...");
      options.setEscapedBy('\\');
      escape = '\\';
    }

    if (record != '\000') {
      LOG.warn("Netezza does not support --lines-terminated-by. Ignoring.");
    }

    // Reuse keys from MySQL.
    Configuration conf = job.getConfiguration();
    conf.setInt(MySQLUtils.OUTPUT_FIELD_DELIM_KEY, field);
    conf.setInt(MySQLUtils.OUTPUT_ESCAPED_BY_KEY, escape);

    LOG.debug("Using InputFormat: " + inputFormatClass);
    job.setInputFormatClass(getInputFormatClass());
  }

  /**
   * Set the mapper class implementation to use in the job,
   * as well as any related configuration (e.g., map output types).
   */
  protected void configureMapper(Job job, String tableName,
      String tableClassName) throws ClassNotFoundException, IOException {
    job.setMapperClass(getMapperClass());
    job.setOutputKeyClass(String.class);
    job.setOutputValueClass(NullWritable.class);
  }
}
