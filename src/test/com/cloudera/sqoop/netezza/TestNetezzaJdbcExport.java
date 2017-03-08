// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

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

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;

import com.cloudera.sqoop.manager.ConnManager;

import com.cloudera.sqoop.tool.ExportTool;
import com.cloudera.sqoop.tool.SqoopTool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test exports over JDBC to Netezza.
 */
public class TestNetezzaJdbcExport {

  public static final Log LOG = LogFactory.getLog(
        TestNetezzaJdbcExport.class.getName());

  protected static final float EPSILON = 0.001F;

  private static int tableId;
  private ConnManager mgr;
  private String schema;

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

  protected String getTablePrefix() {
    return "NZ_TBL_";
  }

  protected String getTableName() {
    return getTablePrefix() + tableId;
  }

  protected String getSchema() {
    return schema;
  }

  protected void setSchema(String schema) {
    this.schema = schema;
  }

  protected Configuration getConf() {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    return NzTestUtil.initConf(conf);
  }

  protected SqoopOptions getSqoopOptions() {
    SqoopOptions options =
      NzTestUtil.initSqoopOptions(new SqoopOptions(getConf()));
    options.setInputFieldsTerminatedBy(',');
    return options;
  }

  @Before
  public void setUp() {
    tableId++;
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "true");
    SqoopOptions options = getSqoopOptions();
    try {
      mgr = NzTestUtil.getNzManager(options);
      NzTestUtil.dropTableIfExists(mgr.getConnection(), getTableName());
    } catch (Exception e) {
      fail("Exception during setup: " + e);
    }

  }

  @After
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
   * Return the base path where we are allowed to write data files.
   */
  protected Path getBasePath() {
    return new Path(LOCAL_WAREHOUSE_DIR);
  }

  protected void writeFileWithLine(Configuration conf, Path path, String text)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(path)) {
      fs.delete(path, false);
    }
    OutputStream os = fs.create(path);
    byte [] bytes = text.getBytes("UTF-8");
    os.write(bytes, 0, bytes.length);
    os.close();
  }

  protected void createSchema(String schema) throws SQLException {
    Connection conn = mgr.getConnection();
    NzTestUtil.dropSchemaIfExists(conn, schema);
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE SCHEMA ");
    sb.append(schema);

    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(sb.toString());
      stmt.executeUpdate();
      conn.commit();
    } finally {
      if (null != stmt)  {
        stmt.close();
      }
    }
  }

  protected void createTableForType(String typeName) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    if (getSchema() != null) {
      createSchema(getSchema());
      sb.append(getSchema());
      sb.append(".");
    }
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
     * results, check that the row matches what we expect it to. Calls
     * fail() if an unexpected result is present.
     */
    void check(ResultSet rs) throws SQLException;
  }

  /**
   * Check that the value column for a given row has the expected
   * result in it, via the Checker object passed in.
   */
  protected void checkValForId(int id, Checker checker) throws SQLException {
    Connection c = mgr.getConnection();
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      String query = "SELECT val FROM ";
      if (getSchema() != null) {
        query += getSchema() + ".";
      }
      query += getTableName() + " WHERE id = ?";

      ps = c.prepareStatement(query);
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

  protected void runExport(SqoopOptions options, Path p) throws Exception {
    runExport(options, p, new String[0]);
  }

  protected void runExport(SqoopOptions options, Path p, String[] sqoopArgs)
    throws Exception {
    options.setExplicitOutputDelims(true);
    options.setExplicitInputDelims(true);
    options.setExportDir(p.toString());
    options.setInputLinesTerminatedBy('\n');
    options.setInputEscapedBy('\\');
    options.setCodeOutputDir(TEMP_BASE_DIR);
    options.setNumMappers(1);
    options.setTableName(getTableName());
    options.setTmpDir(TEMP_BASE_DIR);

    SqoopTool exportTool = new ExportTool();
    Sqoop sqoop = new Sqoop(exportTool, options.getConf(), options);
    int ret = Sqoop.runSqoop(sqoop, sqoopArgs);
    if (0 != ret) {
      fail("Non-zero return from Sqoop: " + ret);
    }
  }

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testFloatExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();

    createTableForType("NUMERIC(12,4)");
    Path p = new Path(getBasePath(), "intx.txt");
    writeFileWithLine(conf, p, "1,3.1416");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals(3.1416f, rs.getFloat(1), EPSILON);
      }
    });
  }

  @Test
  public void testNVarCharExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();

    createTableForType("NVARCHAR(64)");
    Path p = new Path(getBasePath(), "strX.txt");
    writeFileWithLine(conf, p, "1,bleh");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals("bleh", rs.getString(1));
      }
    });
  }

  @Test
  public void testNCharExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();

    createTableForType("NCHAR");
    Path p = new Path(getBasePath(), "charY.txt");
    writeFileWithLine(conf, p, "1,b");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals("b", rs.getString(1));
      }
    });
  }

  @Test
  public void testUTFExport() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();

    createTableForType("NVARCHAR(64)");
    Path p = new Path(getBasePath(), "strY.txt");
    writeFileWithLine(conf, p, "1,žluťoučký kůň"); // Yellow horse in Czech
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals("žluťoučký kůň", rs.getString(1)); // Yellow horse in Czech
      }
    });
  }

  @Test
  public void testIntExportWithDifferentSchema() throws Exception {
    if (NzTestUtil.supportsMultipleSchema(mgr.getConnection())) {
      final String schema = "EXPORT_SCHEMA";
      setSchema(schema);

      SqoopOptions options = getSqoopOptions();
      Configuration conf = options.getConf();

      createTableForType("INT");
      Path p = new Path(getBasePath(), "intx.txt");
      writeFileWithLine(conf, p, "1,42");
      runExport(options, p, new String[]{"--", "--schema", schema});
      checkValForId(1, new Checker() {
        public void check(ResultSet rs) throws SQLException {
          assertEquals(42, rs.getInt(1));
        }
      });
    }
  }
}


