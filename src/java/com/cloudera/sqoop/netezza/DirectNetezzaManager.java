// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.util.ExportException;

/**
 * Uses remote external tables to import/export bulk data to/from Netezza.
 */
public class DirectNetezzaManager extends NetezzaManager {

  public static final Log LOG = LogFactory.getLog(
      DirectNetezzaManager.class.getName());

  public DirectNetezzaManager(final SqoopOptions opts) {
    super(opts);
  }

  @Override
  /**
   *  Export the table from HDFS to NZ by using remote external
   *  tables to insert the data back into the database.
   */
  public void exportTable(ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);
    NetezzaExportJob exportJob = new NetezzaExportJob(context);
    exportJob.runExport();
  }
}

