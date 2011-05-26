// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;

/**
 * Test the Netezza EDW connector for direct mode imports.
 */
public class TestDirectNetezzaImport extends TestJdbcNetezzaImport {

  private static final Log LOG =
      LogFactory.getLog(TestDirectNetezzaImport.class.getName());

  protected String getDbFriendlyName() {
    return "directnetezza";
  }

  @Override
  /**
   * Create a SqoopOptions to connect to the manager.
   */
  public SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions options = super.getSqoopOptions(conf);

    // Use direct mode for this.
    options.setDirectMode(true);
    options.setNumMappers(1); // Point can be proven with 1 mapper.

    return options;
  }


  // This includes all the TestJdbcNetezzaImport tests. Also run the following
  // tests that demonstrate features specific to remote external tables.

  public void testRawComma() throws Exception {
    // If you try to import un-escaped data in Netezza, the JDBC connection will
    // hang. This tests that NetezzaImportJob sets the appropriate flag for us.
    final String TABLE_NAME = "COMMA_TABLE_2";
    createTable(conn, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'meep,beep'");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,meep\\,beep");
  }

  public void testNoAlternateEscapes() throws Exception {
    // Netezza claims that only '\\' may be used as an escape character.
    // Check that we're sanely guarding against this probability.
    // NetezzaImportJob should change our escape to '\\'.

    final String TABLE_NAME = "COMMA_TABLE_3";
    options.setEscapedBy('X');
    createTable(conn, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'meep,beep'");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,meep\\,beep");
  }

  public void testEscapeAlternateFieldDelim() throws Exception {
    // Set the field delimiter to tab. Make sure we auto-escape it.

    final String TABLE_NAME = "TAB_TABLE";
    options.setFieldsTerminatedBy('\t');
    createTable(conn, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'meep\tbeep'");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1\tmeep\\\tbeep");
  }

  public void testMultipleMappers() throws Exception {
    // Ensure that multiple input target files work.
    final String TABLE_NAME = "MULTI_TABLE";
    createTable(conn, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'foo'");
    addRow(conn, TABLE_NAME, "2", "'bar'");
    addRow(conn, TABLE_NAME, "3", "'baz'");
    addRow(conn, TABLE_NAME, "4", "'biff'");
    options.setNumMappers(2);
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 4);
    verifyImportLine(TABLE_NAME, "2,bar");
    verifyImportLine(TABLE_NAME, "1,foo");
    verifyImportLine(TABLE_NAME, "3,baz");
  }
  
  /**
   * This tests overriding a the Netezza specific MAXERRORS export argument.
   * This is an extra argument specified using sqoop's "extra argument" args
   * that come after a "--" arg.
   *
   * This is essentially the same test as testRawComma
   */
  public void testMaxErrors() throws Exception {
    // If you try to import un-escaped data in Netezza, the JDBC connection will
    // hang. This tests that NetezzaImportJob sets the appropriate flag for us.
    final String TABLE_NAME = "MAX_ERRORS";
    createTable(conn, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'meep,beep'");
    String[] extraArgs = { "--", "--" + DirectNetezzaManager.NZ_MAXERRORS_ARG,
        "2", };
    runImport(options, TABLE_NAME, extraArgs);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,meep\\,beep");
  }
}

