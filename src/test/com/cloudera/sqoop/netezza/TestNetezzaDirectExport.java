// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.File;
import java.io.FileFilter;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.cloudera.sqoop.Sqoop;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.SqoopOptions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.cloudera.sqoop.SqoopOptions.FileLayout.AvroDataFile;
import static com.cloudera.sqoop.SqoopOptions.FileLayout.ParquetFile;
import static com.cloudera.sqoop.SqoopOptions.FileLayout.SequenceFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

/**
 * Test exports over FIFO to Netezza.
 */
public class TestNetezzaDirectExport extends TestNetezzaJdbcExport {

  public static final Log LOG = LogFactory.getLog(TestNetezzaDirectExport.class.getName());

  protected String getTablePrefix() {
    return "NZ_DIRECT_TBL_";
  }

  protected SqoopOptions getSqoopOptions() {
    SqoopOptions options = super.getSqoopOptions();
    options.setDirectMode(true);
    return options;
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * This tests overriding a the Netezza specific MAXERRORS export argument.
   * This is an extra argument specified using sqoop's "extra argument" args
   * that come after a "--" arg.
   */
  @Test
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
        assertEquals(3.1416f, rs.getFloat(1), EPSILON);
      }
    });
  }

  @Test
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
        assertEquals(3.1416f, rs.getFloat(1), EPSILON);
      }
    });
  }

  @Test
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
  @Test
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

    String[] extraArgs = {
      "--",
      "--nz-maxerrors", "2",
      "--nz-logdir", logDirPath,
    };

    runExport(options, p, extraArgs);

    assertLogs(logDirPath, 1, 1);

    FileSystem lfs = FileSystem.getLocal(conf);
    lfs.delete(new Path(logDirPath), true);
  }

  @Test
  public void testUploadDir() throws Exception {
    SqoopOptions options = getSqoopOptions();
    options.setInputFieldsTerminatedBy('|');
    Configuration conf = options.getConf();

    createTableForType("INTEGER");
    Path p = new Path(getBasePath(), "badrec.txt");
    writeFileWithLine(conf, p, "1|twenty");
    writeFileWithLine(conf, p, "2|ten");
    File f = File.createTempFile("test", "nzexport.tmp");
    String logDirPath = f.getAbsolutePath() + ".dir";
    String uploadDirPath = f.getAbsolutePath() + ".upload.dir";
    f.delete();

    String[] extraArgs = {
      "--",
      "--nz-maxerrors", "2",
      "--nz-logdir", logDirPath,
      "--nz-uploaddir", uploadDirPath
    };

    runExport(options, p, extraArgs);

    assertLogs(logDirPath, 1, 1);
    assertLogs(uploadDirPath, 1, 1);

    FileSystem lfs = FileSystem.getLocal(conf);
    lfs.delete(new Path(logDirPath), true);
    lfs.delete(new Path(uploadDirPath), true);
  }

  /**
   * Assert Netezza logs.
   *
   * @param logDirPath Log directory
   * @param numBadFiles Number of bad files
   * @param numLogFiles Number of log files
   *
   * @throws Exception
   */
  public static void assertLogs(String logDirPath, int numBadFiles, int numLogFiles) throws Exception {
    File dir = new File(logDirPath);
    Assert.assertTrue(dir.exists());

    File[] badRecordFiles = dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname != null
          && pathname.getName().toLowerCase().endsWith(".nzbad");
      }
    });

    Assert.assertNotNull(badRecordFiles);
    Assert.assertEquals(numBadFiles, badRecordFiles.length);

    File[] logFiles = dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname != null
          && pathname.getName().toLowerCase().endsWith(".nzlog");
      }
    });

    Assert.assertNotNull(logFiles);
    Assert.assertEquals(numLogFiles, logFiles.length);
  }

  @Test
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
  }

  @Test
  public void testStringExportLowAscii() throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();

    String[] extraArgs = { "--", "--nz-ctrlchars", };

    createTableForType("VARCHAR(64)");
    Path p = new Path(getBasePath(), "strZ.txt");
    writeFileWithLine(conf, p, "1,a\tb\bc");
    runExport(options, p, extraArgs);
    checkValForId(1, new Checker() {
      public void check(ResultSet rs) throws SQLException {
        assertEquals("a\tb\bc", rs.getString(1));
      }
    });
  }

  @Test
  public void testExportFailsWithSequenceFile() throws Exception {
    expectExceptionWithSpecificFileLayout(SequenceFile);
  }

  @Test
  public void testExportFailsWithAvroDataFile() throws Exception {
    expectExceptionWithSpecificFileLayout(AvroDataFile);
  }

  @Test
  public void testExportFailsWithParquetFile() throws Exception {
    expectExceptionWithSpecificFileLayout(ParquetFile);
  }

  public void expectExceptionWithSpecificFileLayout(SqoopOptions.FileLayout fileLayout) throws Exception {
    SqoopOptions options = getSqoopOptions();
    Configuration conf = options.getConf();
    String passedArgument = null;
    options.setFileLayout(fileLayout);

    createTableForType("INTEGER");
    Path p = new Path(getBasePath(), "inty.txt");
    writeFileWithLine(conf, p, "1");

    if(fileLayout == SequenceFile){
      passedArgument = "--as-sequencefile";
    } else if(fileLayout == AvroDataFile) {
      passedArgument = "--as-avrodatafile";
    } else if(fileLayout == ParquetFile) {
      passedArgument = "--as-parquetfile";
    }

    String validationMessage = String.format("Unsupported argument with Netezza Connector: %s", passedArgument);

    thrown.expectCause(instanceOf(IllegalArgumentException.class));
    thrown.expectMessage(validationMessage);
    runExport(options, p);
  }
}
