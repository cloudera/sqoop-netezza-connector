// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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

import com.cloudera.sqoop.netezza.util.NetezzaUtil;
import static com.cloudera.sqoop.netezza.util.NetezzaConstants.*;

/**
 * Uses remote external tables to import/export bulk data to/from Netezza.
 */
public class DirectNetezzaManager extends NetezzaManager {
  public static final Log LOG = LogFactory.getLog(DirectNetezzaManager.class
      .getName());

  // Hadoop Configuration key
  public static final String NZ_MAXERRORS_CONF = "nz.export.maxerrors";
  public static final String NZ_LOGDIR_CONF = "nz.export.logdir";

  // cmd line args
  public static final String NZ_MAXERRORS_ARG = "nz-maxerrors";
  public static final String NZ_LOGDIR_ARG = "nz-logdir";

  // Catalog query for looking up object types
  private static final String QUERY_OBJECTY_TYPE = "SELECT OBJTYPE FROM "
      + "_V_OBJECTS WHERE OBJNAME = ? AND OWNER = ?";

  // Error message used for indicating that only Table based import export
  // is allowed. This is needed as a constant to ensure correct working of
  // tests that assert this functionality.
  public static final String ERROR_MESSAGE_TABLE_SUPPORT_ONLY =
      "The direct mode of operation can only work with "
          + "real tables. Using it against view or other types is not "
          + "supported.";


  public DirectNetezzaManager(final SqoopOptions opts) {
    super(opts);
  }

  /**
   *  Export the table from HDFS to NZ by using remote external
   *  tables to insert the data back into the database.
   */
  @Override
  public void exportTable(ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);

    SqoopOptions options = context.getOptions();

    // set netezza specific settings in context's sqoop options.
    String[] extras = options.getExtraArgs();
    try {
      parseExtraArgs(extras, options.getConf());
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          "Bad arguments with netezza specific options", e);
    } catch (InvalidOptionsException e) {
      throw new IllegalArgumentException(
          "Bad arguments with netezza specific options", e);
    }

    // Netezza specific validations
    validateTargetObjectType();

    // Handle NULL string
    propagateNullSubstituteValues(options.getInNullStringValue(),
                                  options.getInNullNonStringValue(),
                                  options.getConf());
    // Run the export itself
    NetezzaExportJob exportJob = new NetezzaExportJob(context);
    exportJob.runExport();
  }

  @Override
  public void updateTable(ExportJobContext context)
      throws IOException, ExportException {
    throw new ExportException("The direct mode of Cloudera Connector for "
        + "Netezza does not support exports in update mode.");
  }

  /**
   * Propagate configuration of NULL substituion string to configuration object
   * so that it can be retrieved from job. Prior to doing so this method will
   * validate the configuration for acceptable values.
   *
   * @param string Substitution for string columns
   * @param nonString Substitution for non string columns
   * @param configuration Job's configuration object
   */
  public void propagateNullSubstituteValues(String string, String nonString,
                                             Configuration configuration) {
    // Validate supported configuration (this should be in some generic handler,
    // however sqoop does not have something like that at the moment).

    // We do not supports custom NULL substitution characters for non string
    if (nonString != null) {
      throw new RuntimeException("Detected incompatible NULL substitution"
        + " strings. Netezza direct connector do not supports custom NULL"
        + " escape character for non string values. Please remove"
        + " --input-null-non-string in case of export job or --null-non-string"
        + " in case of import job.");
    }

    // We do not need to continue in case that user is using defaults
    if (string == null) { return; }

    // Our de-escaping is not suporting octal escape sequences
    if (string.matches("\\\\[0-9]+")) {
      throw new RuntimeException("It seems that you've specified octal based"
        + " escape sequences in NULL substitution string. Netezza direct"
        + " connector does not support them at the moment. Please use non"
        + " direct mode by omitting --direct parameter.");
    }

    // De-escape all escape sequences
    String deEscaped = NetezzaUtil.removeEscapeCharacters(string);
    LOG.debug("Using de-escaped NULL substitution string: " + deEscaped);

    // Save it to job
    configuration.set(PROPERTY_NULL_STRING, deEscaped);
  }

  /**
   * Validates that the operation is being performed against a TABLE and not
   * any other object type such as VIEW or MATERIALIZED VIEW. This is because
   * the underlying external table based load/export is only supported for
   * TABLE type objects and not for other types. This method raises an
   * IOException if the given table is not a TABLE type.
   *
   * @throws IOException if the target object is not a TABLE type.
   */
  private void validateTargetObjectType() throws IOException {
    String givenTableName = options.getTableName();

    String owner = options.getUsername();
    String shortTableName = givenTableName;

    int dotIndex = givenTableName.indexOf('.');
    if (dotIndex != -1) {
      owner = givenTableName.substring(0, dotIndex);
      shortTableName = givenTableName.substring(dotIndex + 1);
    }

    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      conn = getConnection();

      pstmt = conn.prepareStatement(QUERY_OBJECTY_TYPE,
          ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      pstmt.setString(1, shortTableName);
      pstmt.setString(2, owner);

      ResultSet rset = pstmt.executeQuery();

      if (!rset.next()) {
        throw new IOException("Unable to validate object type for given table. "
            + "Please ensure that the given user name and table name is in the "
            + "the correct case. If you are not sure, please use upper case to "
            + "specify both these values.");
      }

      String objType = rset.getString(1);
      LOG.debug("Object type found to be: " + objType);
      if (!objType.equalsIgnoreCase("TABLE")) {
        throw new IOException(ERROR_MESSAGE_TABLE_SUPPORT_ONLY);
      }
    } catch (SQLException ex) {
      LOG.error("Unable to verify object type", ex);
      throw new IOException(ex);
    } finally {
      if (pstmt != null) {
        try {
          pstmt.close();
        } catch (SQLException ex) {
          LOG.error("Unable to close prepared statement for object type check",
              ex);
        }
      }
    }
  }

  /**
   * Import the table from NZ to HDFS by reading from remote
   * external tables.
   * {@inheritDoc}
   */
  @Override
  public void importTable(ImportJobContext context)
      throws IOException, ImportException {
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

    // Netezza specific validations
    validateTargetObjectType();

    // Handle NULL string
    propagateNullSubstituteValues(options.getNullStringValue(),
                                  options.getNullNonStringValue(),
                                  options.getConf());

    // Run import job
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
        "Netezza Connector specific arguments");
    nzOpts.addOption(OptionBuilder.withArgName(NZ_MAXERRORS_CONF).hasArg()
        .withDescription("Specify the maximum Netezza Connector "
            + "specific errors")
        .withLongOpt(NZ_MAXERRORS_ARG).create());

    nzOpts.addOption(OptionBuilder.withArgName(NZ_LOGDIR_CONF).hasArg()
        .withDescription("Specify the log directory where Netezza Connector "
            + "will place the nzlog and nzbad files")
            .withLongOpt(NZ_LOGDIR_ARG).create());

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

    // MAXERRORS option
    if (in.hasOption(NZ_MAXERRORS_ARG)) {
      int maxerrs = Integer.parseInt(in.getOptionValue(NZ_MAXERRORS_ARG));
      conf.setInt(NZ_MAXERRORS_CONF, maxerrs);
    }

    // LOGDIR option
    if (in.hasOption(NZ_LOGDIR_ARG)) {
      String logDir = in.getOptionValue(NZ_LOGDIR_ARG);
      conf.set(NZ_LOGDIR_CONF, logDir);
    }
  }
}
