// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.Sqoop;
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

  /**
   * Create a SqoopOptions to connect to the manager.
   */
  @Override
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
    createTable(conn, null, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, null, TABLE_NAME, "1", "'meep,beep'");
    runImport(options, null, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,meep\\,beep");
  }

  public void testNoAlternateEscapes() throws Exception {
    // Netezza claims that only '\\' may be used as an escape character.
    // Check that we're sanely guarding against this probability.
    // NetezzaImportJob should change our escape to '\\'.

    final String TABLE_NAME = "COMMA_TABLE_3";
    options.setEscapedBy('X');
    createTable(conn, null, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, null, TABLE_NAME, "1", "'meep,beep'");
    runImport(options, null, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,meep\\,beep");
  }

  public void testEscapeAlternateFieldDelim() throws Exception {
    // Set the field delimiter to tab. Make sure we auto-escape it.

    final String TABLE_NAME = "TAB_TABLE";
    options.setFieldsTerminatedBy('\t');
    createTable(conn, null, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, null, TABLE_NAME, "1", "'meep\tbeep'");
    runImport(options, null, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1\tmeep\\\tbeep");
  }

  public void testMultipleMappers() throws Exception {
    // Ensure that multiple input target files work.
    final String TABLE_NAME = "MULTI_TABLE";
    createTable(conn, null, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, null, TABLE_NAME, "1", "'foo'");
    addRow(conn, null, TABLE_NAME, "2", "'bar'");
    addRow(conn, null, TABLE_NAME, "3", "'baz'");
    addRow(conn, null, TABLE_NAME, "4", "'biff'");
    options.setNumMappers(2);
    runImport(options, null, TABLE_NAME);
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
    createTable(conn, null, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, null, TABLE_NAME, "1", "'meep,beep'");
    // TODO verifying maxErrors 2 requires error generated from external
    // system.  We can check via visual inspection of the map query generated
    // by the map task.
    String[] extraArgs = { "--verbose", "--",
        "--" + DirectNetezzaManager.NZ_MAXERRORS_ARG, "2", };
    runImport(options, null, TABLE_NAME, extraArgs);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,meep\\,beep");
  }


  /**
   * This test creates a view and asserts that you cannot import from that
   * view because only table types are supported. Due to limitations of the
   * Sqoop framework, the exception message is compared to ensure that this
   * is indeed the case.
   * @throws Exception
   */
  public void testNoViewSupport() throws Exception {
    final String TABLE_NAME = "MY_TABLE";
    createTable(conn, null, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, null, TABLE_NAME, "1", "'meep,beep'");

    final String VIEW_NAME = "MY_VIEW";
    createView(conn, VIEW_NAME, "SELECT * FROM " + TABLE_NAME);
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true");
    String message = null;
    try {
      runImport(options, null, VIEW_NAME);
    } catch (RuntimeException ex) {
      message = ex.getMessage();
    }

    Assert.assertTrue(message != null && message.endsWith(": "
              + DirectNetezzaManager.ERROR_MESSAGE_TABLE_SUPPORT_ONLY));
  }

  public void testNullBehavior() throws Exception {
    // Ensure that we're correctly supporting NULL substitutions
    final String TABLE_NAME = "NULL_SUBSTITUTION";
    createTable(conn, null, TABLE_NAME, "INTEGER", "VARCHAR(32)", "INTEGER", "VARCHAR(32)");
    addRow(conn, null, TABLE_NAME, "1", "null", "null", "'value'");
    options.setNullStringValue("\\\\N");

    runImport(options, null, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,\\N,,value");
  }
}

