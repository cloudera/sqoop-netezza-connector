// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.GenericJdbcManager;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.teradata.util.TeradataConstants;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * Manages connections to Teradata EDW.
 */
public class TeradataManager extends GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(TeradataManager.class
      .getName());

  // driver class to ensure is loaded when making db connection.
  protected static final String DRIVER_CLASS = "com.teradata.jdbc.TeraDriver";

  /**
   * @param opts the sqoop options
   */
  public TeradataManager(final SqoopOptions opts) {
    super(DRIVER_CLASS, opts);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.cloudera.sqoop.manager.SqlManager#exportTable(com.cloudera.sqoop.
   * manager .ExportJobContext)
   */
  @Override
  public void exportTable(ExportJobContext context) throws IOException,
      ExportException {
    context.setConnManager(this);
    context.getOptions().getConf()
        .setBoolean(TeradataConstants.MULTI_INSERT_STATEMENTS, true);
    super.exportTable(context);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.cloudera.sqoop.manager.SqlManager#importTable(com.cloudera.sqoop.
   * manager .ImportJobContext)
   */
  @Override
  public void importTable(ImportJobContext context) throws IOException,
      ImportException {
    context.setConnManager(this);
    super.importTable(context);
  }
}
