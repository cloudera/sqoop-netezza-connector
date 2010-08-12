// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

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

  @Override
  /**
   * Import the table from NZ to HDFS by reading from remote
   * external tables.
   * {@inheritDoc}
   */
  public void importTable(ImportJobContext context)
      throws IOException, ImportException {
    context.setConnManager(this);

    NetezzaImportJob importer = null;
    try {
      importer = new NetezzaImportJob(context);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Coudl not load required class", cnfe);
    }

    LOG.info("Beginning Netezza fast path import");

    if (options.getFileLayout() != SqoopOptions.FileLayout.TextFile) {
      // TODO(aaron): Support SequenceFile-based load-in.
      LOG.warn("File import layout " + options.getFileLayout()
          + " is not supported by Netezza direct import. Import will proceed "
          + "as text files.");
    }

    importer.runImport(options.getTableName(), context.getJarFile(), null,
        options.getConf());
  }
}

