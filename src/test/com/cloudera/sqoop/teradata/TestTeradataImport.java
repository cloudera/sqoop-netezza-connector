// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import junit.framework.TestCase;

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

/**
 * Test the Teradata EDW connector for basic jdbc mode imports.
 */
public class TestTeradataImport extends TestCase {

  protected Configuration conf;
  protected SqoopOptions options;
  protected ConnManager manager;
  protected Connection conn;
  private static final Log LOG = LogFactory
  .getLog(TdTestUtil.class.getName());

  /*
   * (non-Javadoc)
   *
   * @see junit.framework.TestCase#setUp()
   */
  @Override
  public void setUp() throws IOException, InterruptedException, SQLException {
    conf = TdTestUtil.initConf(new Configuration());
    options = getSqoopOptions(conf);
    manager = TdTestUtil.getTdManager(options);
    conn = manager.getConnection();
  }

  /*
   * (non-Javadoc)
   *
   * @see junit.framework.TestCase#tearDown()
   */
  @Override
  public void tearDown() throws SQLException {
    if (null != conn) {
      this.conn.close();
    }
  }

  /**
   * @return a string friendly name for the TD EDW.
   */
  protected String getEdwFriendlyName() {
    return "td";
  }

  /** Base directory for all temporary data. */
  public static final String TEMP_BASE_DIR;

  /** Where to import table data to in the local file system for testing. */
  public static final String LOCAL_WAREHOUSE_DIR;

  // Initializer.
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
   *
   * @param config
   *          The configuration object
   * @return
   */
  public SqoopOptions getSqoopOptions(Configuration config) {
    SqoopOptions sqoopOptions = new SqoopOptions(config);
    TdTestUtil.initSqoopOptions(sqoopOptions);
    sqoopOptions.setNumMappers(2);
    return sqoopOptions;
  }

  /**
   * @param tableName
   * @param colTypes
   * @throws Exception 
   */
  protected void createTable(String tableName, String... colTypes)
      throws Exception {
    if (null == colTypes || colTypes.length == 0) {
      throw new SQLException("must have at least one column");
    }
    TdTestUtil.dropTableIfExists(tableName);
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
    Connection connection = null;
    Statement statement = null;
    try {
      Class.forName("com.teradata.jdbc.TeraDriver");
      connection = DriverManager.getConnection(TdTestUtil.getConnectString(),
          TdTestUtil.TERADATA_USER, TdTestUtil.TERADATA_PASS);
      connection.setAutoCommit(false);
      statement = connection.createStatement();
      LOG.debug("Trying to execute query: " + sb.toString());
      statement.executeQuery(sb.toString());
      connection.commit();
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (SQLException e1) {
        LOG.debug(e.getMessage(), e);
      }
      throw e;
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        LOG.debug(e.getMessage(), e);
      }
    }
  }

  /**
   * @param c
   * @param tableName
   * @param values
   * @throws SQLException
   */
  protected void addRow(Connection c, String tableName, String... values)
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
      if (null != stmt) {
        stmt.close();
      }
    }
  }

  /**
   * @param sqoopOptions
   * @param tableName
   * @throws Exception
   */
  protected void runImport(SqoopOptions sqoopOptions, String tableName)
      throws Exception {
    sqoopOptions.setTableName(tableName);
    Path warehousePath = new Path(LOCAL_WAREHOUSE_DIR);
    Path targetPath = new Path(warehousePath, tableName);
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    if (localFs.exists(targetPath)) {
      localFs.delete(targetPath, true);
    }
    sqoopOptions.setTargetDir(targetPath.toString());
    SqoopTool importTool = new ImportTool();
    Sqoop sqoop = new Sqoop(importTool, sqoopOptions.getConf(), sqoopOptions);
    String[] args = new String[1];
    args[0] = "--verbose";
    int ret = Sqoop.runSqoop(sqoop, args);
    if (0 != ret) {
      throw new Exception("Non-zero return from Sqoop: " + ret);
    }
  }

  /**
   * Fail the test if the files in the tableName directory don't have the
   * expected number of lines.
   *
   * @param tableName
   * @param expectedCount
   * @throws IOException
   */
  protected void verifyImportCount(String tableName, int expectedCount)
      throws IOException {
    Path warehousePath = new Path(LOCAL_WAREHOUSE_DIR);
    Path targetPath = new Path(warehousePath, tableName);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FileStatus[] files = fs.listStatus(targetPath);
    if (null == files || files.length == 0) {
      assertEquals("Got multiple files; expected none", 0, expectedCount);
    }
    int numLines = 0;
    for (FileStatus stat : files) {
      Path p = stat.getPath();
      if (p.getName().startsWith("part-")) {
        BufferedReader r = new BufferedReader(new InputStreamReader(fs
            .open(p)));
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
   * @param fs
   * @param p
   * @param line
   * @return true if the file specified by path 'p' contains 'line'.
   * @throws IOException
   */
  protected boolean checkFileForLine(FileSystem fs, Path p, String line)
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
   * @param tableName
   * @param line
   * @return true if a specific line exists in the import files for the table.
   * @throws IOException
   */
  protected boolean hasImportLine(String tableName, String line)
      throws IOException {
    Path warehousePath = new Path(LOCAL_WAREHOUSE_DIR);
    Path targetPath = new Path(warehousePath, tableName);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    FileStatus[] files = fs.listStatus(targetPath);
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

  /**
   * Verify that a specific line exists in the import files for the table.
   *
   * @param tableName
   * @param line
   * @throws IOException
   */
  protected void verifyImportLine(String tableName, String line)
      throws IOException {
    if (!hasImportLine(tableName, line)) {
      fail("Could not find line " + line + " in table " + tableName);
    }
  }

  /**
   * Verify that a specific line has been excluded from the import files for
   * the table.
   *
   * @param tableName
   * @param line
   * @throws IOException
   */
  protected void verifyMissingLine(String tableName, String line)
      throws IOException {
    if (hasImportLine(tableName, line)) {
      fail("Found unexpected (intentionally excluded) line " + line
          + " in table " + tableName);
    }
  }

  /**
   * @throws Exception
   */
  public void testBasicDirectImport() throws Exception {
    final String TABLE_NAME = "BASIC_DIRECT_IMPORT";
    createTable(TABLE_NAME, "INTEGER", "VARCHAR(32)");
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

  /**
   * @throws Exception
   */
  public void testDateImport() throws Exception {
    final String TABLE_NAME = "DATE_TABLE";
    createTable(TABLE_NAME, "INTEGER", "DATE");
    Date d = new Date(System.currentTimeMillis());
    addRow(conn, TABLE_NAME, "1", "'" + d.toString() + "'");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1," + d.toString());
  }

  /**
   * @throws Exception
   */
  public void testTimeImport() throws Exception {
    final String TABLE_NAME = "TIME_TABLE";
    createTable(TABLE_NAME, "INTEGER", "TIME");
    Time t = new Time(System.currentTimeMillis());
    addRow(conn, TABLE_NAME, "1", "'" + t.toString() + "'");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1," + t.toString());
  }

  /**
   * @throws Exception
   */
  public void testTimestampImport() throws Exception {
    final String TABLE_NAME = "TS_TABLE";
    createTable(TABLE_NAME, "INTEGER", "TIMESTAMP");
    Timestamp t = new Timestamp(System.currentTimeMillis());
    addRow(conn, TABLE_NAME, "1", "'" + t.toString() + "'");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1," + t.toString());
  }

  /**
   * @throws Exception
   */
  public void testLargeNumber() throws Exception {
    final String TABLE_NAME = "BIGNUM_TABLE";
    createTable(TABLE_NAME, "INTEGER", "DECIMAL (30,8)");
    String valStr = "12345678965341.627331";
    addRow(conn, TABLE_NAME, "1", valStr);
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1," + valStr + "00");
  }

  /**
   * @throws Exception
   */
  public void testEscapedComma() throws Exception {
    final String TABLE_NAME = "COMMA_TABLE";
    options.setEscapedBy('\\');
    createTable(TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'meep,beep'");
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 1);
    verifyImportLine(TABLE_NAME, "1,meep\\,beep");
  }

}
