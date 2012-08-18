// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.File;
import java.io.FileFilter;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;

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

  /**
   * This tests the option to specify a LOGDIR for direct import. This option
   * ends up creating a log directory if it does not exist and Netezza writes
   * the nzlog/nzbad files to this directory for all problems encountered.
   *
   * @throws Exception
   */
  public void testBadRecordsWithNonExistentLogDir() throws Exception {
    SqoopOptions options = getSqoopOptions();
    options.setInputFieldsTerminatedBy('|');
    Configuration conf = options.getConf();

    createTableForType("INTEGER");
    Path p = new Path(getBasePath(), "badrec.txt");
    writeFileWithLine(conf, p, "1|twenty");
    File f = File.createTempFile("test", "nzexport.tmp");
    String logDirPath = f.getAbsolutePath() + ".dir";
    f.delete();

    String[] extraArgs = { "--", "--nz-maxerrors", "2", "--nz-logdir",
        logDirPath, };

    runExport(options, p, extraArgs);

    File dir = new File(logDirPath);
    Assert.assertTrue(dir.exists());

    File[] badRecordFiles = dir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname != null
            && pathname.getName().toLowerCase().endsWith(".nzbad");
      } });

    Assert.assertNotNull(badRecordFiles);
    Assert.assertEquals(1, badRecordFiles.length);

    File[] logFiles = dir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname != null
            && pathname.getName().toLowerCase().endsWith(".nzlog");
      }

    });

    Assert.assertNotNull(logFiles);
    Assert.assertEquals(1, logFiles.length);

    // cleanup
    logFiles[0].delete();
    badRecordFiles[0].delete();
    dir.delete();
  }

  public void testNullSubstitutionString() throws Exception {
    // Ensure that we're correctly supporting NULL substitutions
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    options.setInputFieldsTerminatedBy('|');
    options.setInNullStringValue("\\\\N");

    createTableForType("VARCHAR(32)");

    Path p = new Path(getBasePath(), "null_substitution.txt");
    writeFileWithLine(conf, p, "1|\\N");

    runExport(options, p);

    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertNull(rs.getString(1));
      }
    });
  }}
