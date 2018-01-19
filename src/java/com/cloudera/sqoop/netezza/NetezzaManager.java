// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ExportJobContext;
import org.apache.sqoop.manager.GenericJdbcManager;
import org.apache.sqoop.manager.ImportJobContext;
import org.apache.sqoop.util.ExportException;
import org.apache.sqoop.util.ImportException;
import org.apache.sqoop.cli.RelatedOptions;

/**
 * Manages connections to Netezza EDW.
 */
public class NetezzaManager extends GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(
      NetezzaManager.class.getName());

  // Pass schema to Netezza
  public static final String SCHEMA_ARG = "schema";

  // driver class to ensure is loaded when making db connection.
  protected static final String DRIVER_CLASS = "org.netezza.Driver";

  /**
   * Command line parser for extra args.
   */
  private CommandLine parser;

  /*
   * Netezza schema that we should use.
   */
  private String schema;

  public NetezzaManager(final SqoopOptions opts) {
    super(DRIVER_CLASS, opts);

    // Try to parse extra arguments
    try {
      parseExtraArgs(opts.getExtraArgs());
    } catch (ParseException e) {
      throw new RuntimeException("Can't parse extra arguments", e);
    }

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
  public void updateTable(ExportJobContext context)
      throws IOException, ExportException {
    if (context.getOptions().getNumMappers() != 1) {
      throw new ExportException("The Cloudera Connector for Netezza does not "
          + "support multi-mapper exports in update mode. Please try again "
          + "with number of mappers explicitly set to 1.");
    }

    super.updateTable(context);
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

  @Override
  public String toJavaType(int sqlType) {
    String type = super.toJavaType(sqlType);
    if (type == null) {
      if (sqlType == Types.NVARCHAR || sqlType == Types.NCHAR) {
        type = "String";
      }
    }

    return type;
  }

  @Override
  public boolean escapeTableNameOnExport() {
    return true;
  }

  @Override
  public String escapeColName(String colName) {
    return escapeIdentifier(colName);
  }

  @Override
  public String escapeTableName(String tableName) {
    // Return full table name including schema if needed
    if (schema != null && !schema.isEmpty()) {
      return escapeIdentifier(schema) + "." + escapeIdentifier(tableName);
    }

    return escapeIdentifier(tableName);
  }

  protected String escapeIdentifier(String identifier) {
    if (identifier == null) {
      return null;
    }
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  public String getSchema() {
    return schema;
  }

  /**
   * Extract extra args from parser.
   * This method remembers the schema.
   *
   * @param cmdLine
   */
  void applyExtraArguments(CommandLine cmdLine) {
    // Apply extra options
    if (cmdLine.hasOption(SCHEMA_ARG)) {
      String schemaName = cmdLine.getOptionValue(SCHEMA_ARG);
      LOG.info("We will use schema " + schemaName);

      this.schema = schemaName;
    }
  }

  /**
   * Create related options for Netezza extra parameters.
   *
   * @return
   */
  @SuppressWarnings("static-access")
  RelatedOptions getExtraOptions() {
    // Connection args (common)
    RelatedOptions extraOptions =
        new RelatedOptions("Netezza connector specific arguments");

    extraOptions.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("Optional schema name")
        .withLongOpt(SCHEMA_ARG).create());

    return extraOptions;
  }

  /**
   * Return a parser with extra args.
   * @param args
   * @return CommandLine or null if no arguments provided.
   */
  CommandLine getParser(String[] args) throws ParseException {
    if (parser != null) {
      return parser;
    }

    // No-op when no extra arguments are present
    if (args == null || args.length == 0) {
      return null;
    }

    // We do not need extended abilities of SqoopParser, so we're using
    // Gnu parser instead.
    CommandLineParser commandLineParser = new GnuParser();
    parser = commandLineParser.parse(getExtraOptions(), args, true);

    return parser;
  }

  /**
   * Parse extra arguments.
   *
   * @param args Extra arguments array
   * @throws ParseException
   */
  void parseExtraArgs(String[] args) throws ParseException {
    CommandLine cmdLine = getParser(args);

    if (cmdLine == null) {
      return;
    }

    // Apply parsed arguments
    applyExtraArguments(cmdLine);
  }
}
