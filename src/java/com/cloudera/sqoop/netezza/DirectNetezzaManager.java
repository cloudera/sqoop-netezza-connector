// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.RelatedOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;

/**
 * Uses remote external tables to import/export bulk data to/from Netezza.
 */
public class DirectNetezzaManager extends NetezzaManager {
  public static final Log LOG = LogFactory.getLog(DirectNetezzaManager.class
      .getName());

  // Hadoop Configuration key
  public static final String NZ_MAXERRORS_CONF = "nz.export.maxerrors";
  // cmd line args
  public static final String NZ_MAXERRORS_ARG = "nz-maxerrors";

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

    // set netezza specific settings in context's sqoop options.
    String[] extras = context.getOptions().getExtraArgs();
    try {
      parseExtraArgs(extras, context.getOptions().getConf());
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          "Bad arguments with netezza specific options", e);
    } catch (InvalidOptionsException e) {
      throw new IllegalArgumentException(
          "Bad arguments with netezza specific options", e);
    }

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

  /**
   * Parse extra args, set approriate values in conf so they can be read by job.
   *
   * @param args
   * @param conf
   * @throws ParseException
   * @throws InvalidOptionsException
   */
  void parseExtraArgs(String[] args, Configuration conf) throws ParseException,
      InvalidOptionsException {
    if (args == null || args.length == 0) {
      // no extras? then ignore.
      return;
    }
    String[] toolArgs = args; // args after generic parser is done.
    CommandLineParser parser = new GnuParser(); // new SqoopParser();
    CommandLine cmdLine = parser.parse(getNZOptions(), toolArgs, true);
    applyCliOptions(cmdLine, conf);
  }

  /**
   * @return NZ related command line options used by cli parser
   */
  @SuppressWarnings("static-access")
  RelatedOptions getNZOptions() {
    // Connection args (common)
    RelatedOptions nzOpts = new RelatedOptions(
        "Netezza Direct specific arguments");
    nzOpts.addOption(OptionBuilder.withArgName(NZ_MAXERRORS_CONF).hasArg()
        .withDescription("Specify the maximum Netezza Connector "
            +	"specific errors")
        .withLongOpt(NZ_MAXERRORS_ARG).create());
    return nzOpts;
  }

  /**
   * Take a commons-cli command line and augment the Hadoop configuration.
   *
   * @param in
   * @param conf
   * @throws InvalidOptionsException
   */
  protected void applyCliOptions(CommandLine in, Configuration conf)
      throws InvalidOptionsException {

    // common options.
    if (in.hasOption(NZ_MAXERRORS_ARG)) {
      // Immediately switch into DEBUG logging.
      int maxerrs = Integer.parseInt(in.getOptionValue(NZ_MAXERRORS_ARG));
      conf.setInt(NZ_MAXERRORS_CONF, maxerrs);
    }
  }
}
