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

  /**
   * This tests overriding a the Netezza specific MAXERRORS export argument.
   * This is an extra argument specified using sqoop's "extra argument" args 
   * that come after a "--" arg.
   */
  public void testMaxErrors() throws Exception {
    SqoopOptions options = getSqoopOptions();
    options.setInputFieldsTerminatedBy('|');

    // Although it would seem cleaner to do an options.setExtraArgs({...}) call
    // here, this will not work because the Tool resets the value, instead we
    // pass extra args to the runExport method, and let the Tool's parser handle
    // it.

    Configuration conf = options.getConf();
    createTableForType("NUMERIC(12,4)");
    Path p = new Path(getBasePath(), "inty.txt");
    writeFileWithLine(conf, p, "1|3.1416");
    String[] extraArgs = { "--", "--" + DirectNetezzaManager.NZ_MAXERRORS_ARG,
        "2", };
    runExport(options, p, extraArgs);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals(3.1416f, rs.getFloat(1));
      }
    });
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
