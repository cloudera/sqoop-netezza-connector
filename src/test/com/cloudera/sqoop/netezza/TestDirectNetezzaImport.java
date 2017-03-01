// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import junit.framework.JUnit4TestAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.SQLException;

/**
 * Test the Netezza EDW connector for direct mode imports.
 */
@RunWith(JUnit4.class)
public class TestDirectNetezzaImport extends TestJdbcNetezzaImport {

  private static final Log LOG =
      LogFactory.getLog(TestDirectNetezzaImport.class.getName());

  protected String getDbFriendlyName() {
    return "directnetezza";
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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

  @Test
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

  @Test
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

  @Test
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

  @Test
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
  @Test
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
  @Test
  public void testNoViewSupportWithCurrentSchema() throws Exception {
    final String TABLE_NAME = "MY_TABLE";
    createTable(conn, null, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, null, TABLE_NAME, "1", "'meep,beep'");

    final String VIEW_NAME = "MY_VIEW";
    createView(conn, null, VIEW_NAME, "SELECT * FROM " + TABLE_NAME);
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true");

    thrown.expect(RuntimeException.class);
    thrown.expectMessage(DirectNetezzaManager.ERROR_MESSAGE_TABLE_SUPPORT_ONLY);
    runImport(options, null, VIEW_NAME);
  }

  @Test
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

  @Test
  public void testImportTableWithCurrentSchema() throws Exception {
    createAndVerifyTestTableWithSpecificSchema(null);
  }

  @Test
  public void testImportTableWithCustomSchema() throws Exception {
    createAndVerifyTestTableWithSpecificSchema("MY_SCHEMA");
  }

  @Test
  public void testNoViewSupportWithCustomSchema() throws Exception {
    final String TABLE_NAME = "MY_TABLE";
    createTestTableWithSpecificNameAndSchema(null, TABLE_NAME);

    final String SCHEMA_NAME = "MY_SCHEMA";
    final String VIEW_NAME = "MY_VIEW";
    createView(conn, SCHEMA_NAME, VIEW_NAME, "SELECT * FROM " + TABLE_NAME);

    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true");

    expectExceptionOnViewImport(SCHEMA_NAME, VIEW_NAME);
  }

  @Test
  public void testTableAndViewWithTheSameNameUnderDifferentSchemas() throws Exception {
    final String TABLE_AND_VIEW_NAME = "MY_TABLE_AND_VIEW";
    createTestTableWithSpecificNameAndSchema(null, TABLE_AND_VIEW_NAME);

    final String SCHEMA_NAME = "MY_SCHEMA";
    createView(conn, SCHEMA_NAME, TABLE_AND_VIEW_NAME, "SELECT * FROM " + TABLE_AND_VIEW_NAME);

    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true");

    expectExceptionOnViewImport(SCHEMA_NAME, TABLE_AND_VIEW_NAME);
  }

  public void createAndVerifyTestTableWithSpecificSchema(String schemaName) throws Exception {
    createTestTableWithSpecificNameAndSchema(schemaName, "TEST_TABLE");

    runImport(options, schemaName, "TEST_TABLE");
    verifyImportCount("TEST_TABLE", 1);
    verifyImportLine("TEST_TABLE", "1,test1\\,test2");
  }

  public void createTestTableWithSpecificNameAndSchema(String schemaName, String tableName) throws SQLException{
    createTable(conn, schemaName, tableName, "INTEGER", "VARCHAR(32)");
    addRow(conn, schemaName, tableName, "1", "'test1,test2'");
  }

  public void expectExceptionOnViewImport(String schemaName, String tableName) throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(DirectNetezzaManager.ERROR_MESSAGE_TABLE_SUPPORT_ONLY);
    runImport(options, schemaName, tableName);
  }

  //workaround: ant kept falling back to JUnit3
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(TestDirectNetezzaImport.class);
  }
}

