// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.tool.ExportTool;
import com.cloudera.sqoop.tool.SqoopTool;

/**
 * Test the Teradata EDW connector for basic jdbc mode exports.
 */
public class TestTeradataExport extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestTeradataExport.class
      .getName());

  private static int tableId;
  private ConnManager mgr;

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
   * @return the table prefix
   */
  protected String getTablePrefix() {
    return "TD_TBL_";
  }

  /**
   * @return the table name
   */
  protected String getTableName() {
    return getTablePrefix() + tableId;
  }

  /**
   * @return the configuration
   */
  protected Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    return TdTestUtil.initConf(conf);
  }

  /**
   * @return the SqoopOptions Object
   */
  protected SqoopOptions getSqoopOptions() {
    SqoopOptions options = new SqoopOptions(getConf());
    return TdTestUtil.initSqoopOptions(options);
  }

  /*
   * (non-Javadoc)
   *
   * @see junit.framework.TestCase#setUp()
   */
  public void setUp() {
    tableId++;
    SqoopOptions options = getSqoopOptions();
    try {
      mgr = TdTestUtil.getTdManager(options);
      TdTestUtil.dropTableIfExists(getTableName());
    } catch (Exception e) {
      fail("Exception during setup: " + e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see junit.framework.TestCase#tearDown()
   */
  public void tearDown() {
    if (null != mgr) {
      try {
        mgr.close();
      } catch (SQLException sqlE) {
        LOG.warn("SQL Exception closing manager: " + sqlE);
      }
    }
  }

  /**
   * @return the base path where we are allowed to write data files.
   */
  protected Path getBasePath() {
    return new Path(LOCAL_WAREHOUSE_DIR);
  }

  /**
   * @param conf
   * @param path
   * @param text
   * @throws IOException
   */
  protected void writeFileWithLine(Configuration conf, Path path, String text)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(path)) {
      fs.delete(path, false);
    }
    OutputStream os = fs.create(path);
    byte[] bytes = text.getBytes("UTF-8");
    os.write(bytes, 0, bytes.length);
    os.close();
  }

  /**
   * @param typeName
   * @throws SQLException
   */
  protected void createTableForType(String typeName) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(getTableName());
    sb.append("( id INT NOT NULL, val ");
    sb.append(typeName);
    sb.append(")");
    String s = sb.toString();
    LOG.info("Creating table: " + s);
    Connection conn = mgr.getConnection();
    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(s);
      ps.executeUpdate();
      conn.commit();
    } finally {
      if (null != ps) {
        ps.close();
      }
    }
  }

  /**
   * Checks that a result matches the expected result.
   */
  public interface Checker {
    /**
     * Given a ResultSet already aligned via next() on the first row of the
     * results, check that the row matches what we expect it to. Calls fail()
     * if an unexpected result is present.
     */
    void check(ResultSet rs) throws SQLException;
  }

  /**
   * Check that the value column for a given row has the expected result in
   * it, via the Checker object passed in.
   *
   * @param id
   * @param checker
   * @throws SQLException
   */
  protected void checkValForId(int id, Checker checker) throws SQLException {
    Connection c = mgr.getConnection();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = c.prepareStatement("SELECT val FROM " + getTableName()
          + " WHERE id = ?");
      ps.setInt(1, id);
      rs = ps.executeQuery();
      if (!rs.next()) {
        fail("Expected a result!");
      }
      checker.check(rs);
      if (rs.next()) {
        fail("Did not expect multiple results");
      }
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqlE) {
          LOG.warn("SQLException closing resultset: " + sqlE);
        }
      }
      if (null != ps) {
        try {
          ps.close();
        } catch (SQLException sqlE) {
          LOG.warn("SQLException closing prepared stmt: " + sqlE);
        }
      }
    }
  }

  /**
   * @param options
   * @param p
   * @throws Exception
   */
  protected void runExport(SqoopOptions options, Path p) throws Exception {
    options.setExplicitDelims(true);
    options.setExportDir(p.toString());
    options.setInputLinesTerminatedBy('\n');
    options.setInputFieldsTerminatedBy(',');
    options.setInputEscapedBy('\\');
    options.setCodeOutputDir(TEMP_BASE_DIR);
    options.setNumMappers(1);
    options.setTableName(getTableName());
    options.setTmpDir(TEMP_BASE_DIR);
    SqoopTool exportTool = new ExportTool();
    Sqoop sqoop = new Sqoop(exportTool, options.getConf(), options);
    int ret = Sqoop.runSqoop(sqoop, new String[0]);
    if (0 != ret) {
      fail("Non-zero return from Sqoop: " + ret);
    }
  }

  /**
   * @throws Exception
   */
  public void testIntExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    createTableForType("INT");
    Path p = new Path(getBasePath(), "intx.txt");
    writeFileWithLine(conf, p, "1,42");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals(42, rs.getInt(1));
      }
    });
  }

  /**
   * @throws Exception
   */
  public void testNullIntExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    createTableForType("INT");
    Path p = new Path(getBasePath(), "intx2.txt");
    writeFileWithLine(conf, p, "1,null");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        rs.getInt(1);
        assertTrue(rs.wasNull());
      }
    });
  }

  /**
   * @throws Exception
   */
  public void testStringExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    createTableForType("VARCHAR(64)");
    Path p = new Path(getBasePath(), "strX.txt");
    writeFileWithLine(conf, p, "1,bleh");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals("bleh", rs.getString(1));
      }
    });
  }

  /**
   * @throws Exception
   */
  public void testStringExport2() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    createTableForType("VARCHAR(64)");
    Path p = new Path(getBasePath(), "strY.txt");
    writeFileWithLine(conf, p, "1,bl\\,eh");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals("bl,eh", rs.getString(1));
      }
    });
  }

  /**
   * @throws Exception
   */
  public void testDateExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    final Date DATE = new Date(System.currentTimeMillis());
    createTableForType("DATE");
    Path p = new Path(getBasePath(), "date.txt");
    writeFileWithLine(conf, p, "1," + DATE);
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals(DATE.toString(), rs.getDate(1).toString());
      }
    });
  }

  /**
   * @throws Exception
   */
  public void testTimeExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    final Time TIME = new Time(System.currentTimeMillis());
    createTableForType("TIME");
    Path p = new Path(getBasePath(), "time.txt");
    writeFileWithLine(conf, p, "1," + TIME);
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals(TIME.toString(), rs.getTime(1).toString());
      }
    });
  }

  /**
   * @throws Exception
   */
  public void testTimestampExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    final Timestamp TS = new Timestamp(System.currentTimeMillis());
    createTableForType("TIMESTAMP");
    Path p = new Path(getBasePath(), "timestamp.txt");
    writeFileWithLine(conf, p, "1," + TS);
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals(TS, rs.getTimestamp(1));
      }
    });
  }

  /**
   * @throws Exception
   */
  public void testFloatExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    createTableForType("NUMERIC(12,4)");
    Path p = new Path(getBasePath(), "intx.txt");
    writeFileWithLine(conf, p, "1,3.1416");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals(3.1416f, rs.getFloat(1));
      }
    });
  }
}
