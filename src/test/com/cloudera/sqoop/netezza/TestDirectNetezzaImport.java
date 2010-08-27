// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;

import junit.framework.TestCase;

/**
 * Test the Netezza EDW connector for direct mode imports.
 */
public class TestDirectNetezzaImport extends TestCase {

  private static Log LOG =
      LogFactory.getLog(TestDirectNetezzaImport.class.getName());

  private Configuration conf;
  private SqoopOptions options;
  private ConnManager manager;
  private Connection conn;

  @Override
  public void setUp() throws IOException, InterruptedException, SQLException {
    conf = NzTestUtil.initConf(new Configuration()); 
    options = getSqoopOptions(conf);
    manager = NzTestUtil.getNzManager(options);
    conn = manager.getConnection();
  }

  @Override
  public void tearDown() throws SQLException {
    if (null != conn) {
      this.conn.close();
    }
  }

  protected String getDbFriendlyName() {
    return "directnetezza";
  }

  /** Base directory for all temporary data. */
  public static final String TEMP_BASE_DIR;

  /** Where to import table data to in the local filesystem for testing. */
  public static final String LOCAL_WAREHOUSE_DIR;

  // Initializer for the above.
  static {
    String tmpDir = System.getProperty("test.build.data", "/tmp/");
    if (!tmpDir.endsWith(File.separator)) {
      tmpDir = tmpDir + File.separator;
    }

    TEMP_BASE_DIR = tmpDir;
    LOCAL_WAREHOUSE_DIR = TEMP_BASE_DIR + "sqoop/warehouse";
  }

  /**
   * Create a SqoopOptions to connect to the manager.
   */
  public SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions options = new SqoopOptions(conf);
    NzTestUtil.initSqoopOptions(options);
    options.setDirectMode(true);
    options.setNumMappers(1);

    return options;
  }
  
  private void createTable(Connection c, String tableName, String... colTypes)
      throws SQLException {

    if (null == colTypes || colTypes.length == 0) {
      throw new SQLException("must have at least one column");
    }

    NzTestUtil.dropTableIfExists(c, tableName);
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(tableName);
    sb.append(" (");
    boolean first = true;
    for (int i = 0; i < colTypes.length; i++) {
      if (!first) {
        sb.append(", ");
      }
      first = false;

      sb.append("col" + i);
      sb.append(" ");
      sb.append(colTypes[i]);
    }
    sb.append(" )");

    PreparedStatement stmt = null;
    try {
      stmt = c.prepareStatement(sb.toString());
      stmt.executeUpdate();
      c.commit();
    } finally {
      if (null != stmt)  {
        stmt.close();
      }
    }
  }

  private void addRow(Connection c, String tableName, String... values)
      throws SQLException {

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(tableName);

    sb.append(" VALUES (");
    boolean first = true;
    for (String val : values) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(val);
    }
    sb.append(")");

    PreparedStatement stmt = null;
    try {
      stmt = c.prepareStatement(sb.toString());
      stmt.executeUpdate();
      c.commit();
    } finally {
      if (null != stmt)  {
        stmt.close();
      }
    }
  }

  private void runImport(SqoopOptions options, String tableName) throws Exception {
    options.setTableName(tableName);

    Path warehousePath = new Path(LOCAL_WAREHOUSE_DIR);
    Path targetPath = new Path(warehousePath, tableName);
    options.setTargetDir(targetPath.toString());

    SqoopTool importTool = new ImportTool();
    Sqoop sqoop = new Sqoop(importTool, options.getConf(), options);
    int ret = Sqoop.runSqoop(sqoop, new String[0]);
    if (0 != ret) {
      throw new Exception("Non-zero return from Sqoop: " + ret);
    }
  }

  /** Fail the test if the files in the tableName directory don't
   * have the expected number of lines.
   */
  private void verifyImportCount(String tableName, int expectedCount)
      throws IOException {
    Path warehousePath = new Path(LOCAL_WAREHOUSE_DIR);
    Path targetPath = new Path(warehousePath, tableName);
   
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FileStatus [] files = fs.listStatus(targetPath);

    if (null == files || files.length == 0) {
      assertEquals("Got multiple files; expected none", 0, expectedCount);
    }

    int numLines = 0;
    for (FileStatus stat : files) {
      Path p = stat.getPath();
      if (p.getName().startsWith("part-")) {
        // Found a legit part of the output.
        BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(p)));
        try {
          while (null != r.readLine()) {
            numLines++;
          }
        } finally {
          r.close();
        }
      }
    }

    assertEquals("Got unexpected number of lines back", expectedCount,
        numLines);
  }

  /**
   * @return true if the file specified by path 'p' contains 'line'.
   */
  private boolean checkFileForLine(FileSystem fs, Path p, String line)
      throws IOException {
    BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(p)));
    try {
      while (true) {
        String in = r.readLine();
        if (null == in) {
          break; // done with the file.
        }

        if (line.equals(in)) {
          return true;
        }
      }
    } finally {
      r.close();
    }

    return false;
  }

  /**
   * Returns true if a specific line exists in the import files for the table.
   */
  private boolean hasImportLine(String tableName, String line) throws IOException {
    Path warehousePath = new Path(LOCAL_WAREHOUSE_DIR);
    Path targetPath = new Path(warehousePath, tableName);
   
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FileStatus [] files = fs.listStatus(targetPath);

    if (null == files || files.length == 0) {
      fail("Got no import files!");
    }

    for (FileStatus stat : files) {
      Path p = stat.getPath();
      if (p.getName().startsWith("part-")) {
        if (checkFileForLine(fs, p, line)) {
          // We found the line. Nothing further to do.
          return true;
        }
      }
    }

    return false;
  }

  /** Verify that a specific line exists in the import files for the table. */
  private void verifyImportLine(String tableName, String line)
      throws IOException {
    if (!hasImportLine(tableName, line)) {
      fail("Could not find line " + line + " in table " + tableName);
    }
  }

  /**
   * Verify that a specific line has been excluded from the import files for
   * the table.
   */
  private void verifyMissingLine(String tableName, String line)
      throws IOException {
    if (hasImportLine(tableName, line)) {
      fail("Found unexpected (intentionally excluded) line " + line
          + " in table " + tableName);
    }
  }

  public void testBasicDirectImport() throws Exception {

    final String TABLE_NAME = "BASIC_DIRECT_IMPORT";
    createTable(conn, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'meep'");
    addRow(conn, TABLE_NAME, "2", "'beep'");
    addRow(conn, TABLE_NAME, "3", "'foo'");
    addRow(conn, TABLE_NAME, "4", "'bar'");

    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 4);
    verifyImportLine(TABLE_NAME, "1,meep");
    verifyImportLine(TABLE_NAME, "2,beep");
    verifyImportLine(TABLE_NAME, "3,foo");
    verifyImportLine(TABLE_NAME, "4,bar");
  }

  public void testDateImport() throws Exception {
    final String TABLE_NAME = "DATE_TABLE";
    createTable(conn, TABLE_NAME, "INTEGER", "DATE");
    Date d = new Date(System.currentTimeMillis());
    addRow(conn, TABLE_NAME, "1", "'" + d.toString() + "'");

    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1," + d.toString());
  }

  public void testTimeImport() throws Exception {
    final String TABLE_NAME = "TIME_TABLE";
    createTable(conn, TABLE_NAME, "INTEGER", "TIME");
    Time t = new Time(System.currentTimeMillis());
    addRow(conn, TABLE_NAME, "1", "'" + t.toString() + "'");

    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1," + t.toString());
  }

  public void testTimestampImport() throws Exception {
    final String TABLE_NAME = "TS_TABLE";
    createTable(conn, TABLE_NAME, "INTEGER", "TIMESTAMP");
    Timestamp t = new Timestamp(System.currentTimeMillis());
    addRow(conn, TABLE_NAME, "1", "'" + t.toString() + "'");

    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1," + t.toString());
  }

  public void testLargeNumber() throws Exception {
    final String TABLE_NAME = "BIGNUM_TABLE";
    createTable(conn, TABLE_NAME, "INTEGER", "DECIMAL (30,8)");
    String valStr = "12345678965341.627331";
    addRow(conn, TABLE_NAME, "1", valStr);

    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    // import should pad to 8 significant figures after the decimal pt.
    verifyImportLine(TABLE_NAME, "1," + valStr + "00");
  }

  public void testEscapedComma() throws Exception {
    final String TABLE_NAME = "COMMA_TABLE_2";
    options.setEscapedBy('\\');
    createTable(conn, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'meep,beep'");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,meep\\,beep");
  }

  public void testRawComma() throws Exception {
    // If you try to import un-escaped data in Netezza, the JDBC connection will
    // hang. This tests that NetezzaImportJob sets the appropriate flag for us.
    final String TABLE_NAME = "COMMA_TABLE";
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

  public void testUserConditions() throws Exception {
    // Test that a user-specified where clause works.

    final String TABLE_NAME = "WHERE_TABLE";
    createTable(conn, TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'foo'");
    addRow(conn, TABLE_NAME, "2", "'bar'");
    addRow(conn, TABLE_NAME, "3", "'baz'");
    options.setWhereClause("col0 = 2");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "2,bar");
    verifyMissingLine(TABLE_NAME, "1,foo");
    verifyMissingLine(TABLE_NAME, "3,baz");
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
}

