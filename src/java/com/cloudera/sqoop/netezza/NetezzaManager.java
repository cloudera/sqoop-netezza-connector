// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;

import java.sql.Connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.GenericJdbcManager;

import com.cloudera.sqoop.util.ExportException;

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
}

