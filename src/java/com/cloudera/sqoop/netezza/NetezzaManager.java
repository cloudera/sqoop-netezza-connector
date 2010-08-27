// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.GenericJdbcManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * Manages connections to Netezza EDW.
 */
public class NetezzaManager extends GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(
      NetezzaManager.class.getName());

  // driver class to ensure is loaded when making db connection.
  protected static final String DRIVER_CLASS = "org.netezza.Driver";

  public NetezzaManager(final SqoopOptions opts) {
    super(DRIVER_CLASS, opts);
  }

  @Override
  protected int getMetadataIsolationLevel() {
    // Netezza doesn't support READ_UNCOMMITTED.
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public void exportTable(ExportJobContext context)
      throws IOException, ExportException {
    // Netezza does not support multi-row INSERT statements.
    context.getOptions().getConf().setInt("sqoop.export.records.per.statement",
        1);
    super.exportTable(context);
  }

  @Override
  protected void checkTableImportOptions(ImportJobContext context)
      throws IOException, ImportException {
    // SqlManager implementation validates that there is a split column
    // specified.  Netezza does not require this since we're using DATASLICEID
    // based splits.
  }


  @Override
  public void importTable(ImportJobContext context)
      throws IOException, ImportException {
    context.setConnManager(this);
    // Specify the Netezza-specific DBInputFormat for import.
    // The RR here will use DATASLICEID to partition the workload.
    context.setInputFormat(NetezzaJdbcInputFormat.class);
    super.importTable(context);
  }
}

