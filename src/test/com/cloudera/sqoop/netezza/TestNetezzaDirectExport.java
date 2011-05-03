// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.SqoopOptions;

/**
 * Test exports over FIFO to Netezza.
 */
public class TestNetezzaDirectExport extends TestNetezzaJdbcExport {

  public static final Log LOG = LogFactory.getLog(TestNetezzaDirectExport.class
      .getName());

  protected String getTablePrefix() {
    return "NZ_DIRECT_TBL_";
  }

  protected SqoopOptions getSqoopOptions() {
    SqoopOptions options = super.getSqoopOptions();
    options.setDirectMode(true);
    return options;
  }

  public void testFloatExportWithSubstitutedFieldDelimiters() throws Exception {
    SqoopOptions options = getSqoopOptions();
    options.setInputFieldsTerminatedBy('|');
    Configuration conf = options.getConf();

    createTableForType("NUMERIC(12,4)");
    Path p = new Path(getBasePath(), "inty.txt");
    writeFileWithLine(conf, p, "1|3.1416");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals(3.1416f, rs.getFloat(1));
      }
    });
  }

  public void testTruncString() throws Exception {
    // Write a field that is longer than the varchar len. verify that it is
    // truncated.

    SqoopOptions options = getSqoopOptions();
    options.setInputFieldsTerminatedBy('|');
    Configuration conf = options.getConf();

    createTableForType("VARCHAR(8)");
    Path p = new Path(getBasePath(), "toolong.txt");
    writeFileWithLine(conf, p, "1|this string is too long");
    runExport(options, p);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals("this str", rs.getString(1));
      }
    });
  }
}
