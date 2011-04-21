// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.teradata.exports.TeradataExportJob;
import com.cloudera.sqoop.teradata.imports.TeradataImportJob;
import com.cloudera.sqoop.teradata.imports.TeradataInputFormat;
import com.cloudera.sqoop.teradata.util.TeradataConstants;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * An optimized direct manager for Teradata EDW connections. This manager uses
 * partitioned temp tables for importing and exporting the data, so it offers
 * better performance and isolation features.
 */
public class DirectTeradataManager extends TeradataManager {

  public static final Log LOG = LogFactory.getLog(DirectTeradataManager.class
      .getName());

  /**
   * @param opts
   */
  public DirectTeradataManager(final SqoopOptions opts) {
    super(opts);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.cloudera.sqoop.teradata.TeradataManager#exportTable(com.cloudera.
   * sqoop .manager.ExportJobContext)
   */
  @Override
  public void exportTable(ExportJobContext context) throws IOException,
      ExportException {
    context.setConnManager(this);
//    context.getOptions().getConf().set("mapred.output.committer.class",
//        "com.cloudera.sqoop.teradata.exports.TeradataExportOutputCommitter");
    context.getOptions().getConf()
        .setBoolean(TeradataConstants.MULTI_INSERT_STATEMENTS, true);
    TeradataExportJob exportJob = new TeradataExportJob(context);
    exportJob.runExport();
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.cloudera.sqoop.teradata.TeradataManager#importTable(com.cloudera.
   * sqoop .manager.ImportJobContext)
   */
  @Override
  public void importTable(ImportJobContext context) throws IOException,
      ImportException {
    context.setConnManager(this);
//    context.getOptions().getConf().set("mapred.output.committer.class",
//        "com.cloudera.sqoop.teradata.imports.TeradataImportOutputCommitter");
    TeradataImportJob importer = new TeradataImportJob(context.getOptions(),
        TeradataInputFormat.class, context);
    LOG.info("Beginning Teradata import");

    if (options.getFileLayout() != SqoopOptions.FileLayout.TextFile) {
      // TODO: add support for sequence files.
      LOG.warn("File import layout "
          + options.getFileLayout()
          + " is not supported by Teradata direct import, import will proceed "
          + "as text files.");
    }

    String tableName = context.getTableName();

    importer.runImport(tableName, context.getJarFile(),
        null, context.getOptions().getConf());
  }
}
