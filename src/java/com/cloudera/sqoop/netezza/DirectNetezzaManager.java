// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.SqoopOptions.InvalidOptionsException;
import org.apache.sqoop.cli.RelatedOptions;
import org.apache.sqoop.manager.ExportJobContext;
import org.apache.sqoop.manager.ImportJobContext;
import org.apache.sqoop.util.ExportException;
import org.apache.sqoop.util.ImportException;

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
  public static final String NZ_UPLOADDIR_CONF = "nz.export.uploaddir";
  public static final String NZ_CTRLCHARS_CONF = "nz.export.ctrlchars";

  // cmd line args
  public static final String NZ_MAXERRORS_ARG = "nz-maxerrors";
  public static final String NZ_LOGDIR_ARG = "nz-logdir";
  public static final String NZ_UPLOADDIR_ARG = "nz-uploaddir";
  public static final String NZ_CTRLCHARS_ARG = "nz-ctrlchars";

  public static final String NETEZZA_SCHEMA_OPT = "netezza.schema";
  public static final String NETEZZA_TABLE_SCHEMA_LONG_ARG = "schema";

  // Catalog query for looking up object types
  private static final String QUERY_OBJECTY_TYPE = "SELECT OBJTYPE FROM "
      + "_V_OBJECTS WHERE OBJNAME = ? AND SCHEMA = CURRENT_SCHEMA";

  private static final String QUERY_OBJECTY_TYPE_WITH_SCHEMA = "SELECT OBJTYPE FROM "
      + "_V_OBJECTS WHERE OBJNAME = ? AND SCHEMA = ?";

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
    LOG.debug("extraArgs " + Arrays.toString(extras));

    try {
      CommandLine parser = getParser(extras);
      if (parser != null) {
        applyCliOptions(parser, options.getConf());
      }
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          "Bad arguments with netezza specific options", e);
    } catch (InvalidOptionsException e) {
      throw new IllegalArgumentException(
          "Bad arguments with netezza specific options", e);
    }

    // Validate parameter compatiblitiy
    validateParameterCompatibility(options);

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

      if (getSchema() != null) {
        pstmt = conn.prepareStatement(QUERY_OBJECTY_TYPE_WITH_SCHEMA,
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        pstmt.setString(2, getSchema());
      } else {
        pstmt = conn.prepareStatement(QUERY_OBJECTY_TYPE,
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      }

      pstmt.setString(1, shortTableName);

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
    LOG.debug("extraArgs " + Arrays.toString(extras));

    try {
      CommandLine parser = getParser(extras);
      if (parser != null) {
        applyCliOptions(parser, context.getOptions().getConf());
      }
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          "Bad arguments with netezza specific options", e);
    } catch (InvalidOptionsException e) {
      throw new IllegalArgumentException(
          "Bad arguments with netezza specific options", e);
    }

    // Validate parameter compatiblitiy
    validateParameterCompatibility(options);

    NetezzaImportJob importer = null;
    try {
      importer = new NetezzaImportJob(context);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not load required class", cnfe);
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
   * @return NZ related command line options used by cli parser
   */
  @SuppressWarnings("static-access")
  protected RelatedOptions getExtraOptions() {
    RelatedOptions nzOpts = super.getExtraOptions();

    nzOpts.addOption(OptionBuilder.withArgName(NZ_MAXERRORS_CONF).hasArg()
        .withDescription("Specify the maximum Netezza Connector "
            + "specific errors")
        .withLongOpt(NZ_MAXERRORS_ARG).create());

    nzOpts.addOption(OptionBuilder.withArgName(NZ_LOGDIR_CONF).hasArg()
        .withDescription("Specify the log directory where Netezza Connector "
            + "will place the nzlog and nzbad files")
        .withLongOpt(NZ_LOGDIR_ARG).create());

    nzOpts.addOption(OptionBuilder
        .withArgName(NZ_UPLOADDIR_CONF).hasArg()
        .withDescription("HDFS directory where Netezza Connector should upload Netezza logs")
        .withLongOpt(NZ_UPLOADDIR_ARG).create());

    nzOpts.addOption(OptionBuilder
        .withArgName(NZ_CTRLCHARS_CONF)
        .withDescription("Pass CTRLCHARS option to nzLoad")
        .withLongOpt(NZ_CTRLCHARS_ARG).create());

    nzOpts.addOption(OptionBuilder.withArgName(NETEZZA_SCHEMA_OPT)
        .hasArg().withDescription("Allow Schema")
        .withLongOpt(NETEZZA_TABLE_SCHEMA_LONG_ARG).create());

    return nzOpts;
  }

  /**
   * Take a commons-cli command line and augment the Hadoop configuration.
   *
   * @param in
   * @param conf
   * @throws InvalidOptionsException
   */
  void applyCliOptions(CommandLine in, Configuration conf)
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

    // UPLOADDIR option
    if (in.hasOption(NZ_UPLOADDIR_ARG)) {
      String uploadDir = in.getOptionValue(NZ_UPLOADDIR_ARG);
      conf.set(NZ_UPLOADDIR_CONF, uploadDir);
    }

    // CTRLCHARS
    if (in.hasOption(NZ_CTRLCHARS_ARG)) {
      conf.setBoolean(NZ_CTRLCHARS_CONF, true);
    }

    // SCHEMA option
    if (in.hasOption(NETEZZA_TABLE_SCHEMA_LONG_ARG)) {
      String schemaName = in.getOptionValue(NETEZZA_TABLE_SCHEMA_LONG_ARG);
      LOG.info("We will use schema " + schemaName);
      conf.set(NETEZZA_SCHEMA_OPT, schemaName);
    }
  }

  /**
   * Verify compatibility of this connector with user entered parameters.
   *
   * @param options User entered parsed command line arguments
   */
  public void validateParameterCompatibility(SqoopOptions options) {
    if(options.getHBaseTable() != null) {
      throwIllegalArgumentException("--hbase-table");
    }

    if(options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
      throwIllegalArgumentException("--as-avrodatafile");
    }

    if(options.getFileLayout() == SqoopOptions.FileLayout.SequenceFile) {
      throwIllegalArgumentException("--as-sequencefile");
    }

    if(options.getFileLayout() == SqoopOptions.FileLayout.ParquetFile) {
      throwIllegalArgumentException("--as-parquetfile");
    }
  }

  private void throwIllegalArgumentException(String option) {
    throw new IllegalArgumentException("Unsupported argument with Netezza Connector: " + option);
  }

  @Override
  public String escapeTableName(String tableName) {
    Configuration conf = options.getConf();

    String schema = conf.get(NETEZZA_SCHEMA_OPT);
    // Return table name including schema if requested
    if (schema != null && !schema.isEmpty()) {
      return escapeIdentifier(schema) + "." + escapeIdentifier(tableName);
    }

    return escapeIdentifier(tableName);
  }
}
