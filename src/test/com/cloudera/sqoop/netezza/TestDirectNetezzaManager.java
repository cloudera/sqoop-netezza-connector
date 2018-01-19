// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.SqoopOptions.InvalidOptionsException;
import org.junit.Test;

import static com.cloudera.sqoop.netezza.util.NetezzaConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This tests some of the helper functions found in the Direct Netezza Manager.
 */
public class TestDirectNetezzaManager {

  /**
   * Test verifies that the "extra args" are parsed and present in conf's used
   * by MR jobs.
   *
   * @throws ParseException
   * @throws InvalidOptionsException
   */
  @Test
  public void testParseExtraArgs() throws ParseException,
      InvalidOptionsException {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
    String[] args = { "--" + DirectNetezzaManager.NZ_MAXERRORS_ARG, "1337" };
    Configuration conf = opts.getConf();
    CommandLine parser = m.getParser(args);
    m.applyCliOptions(parser, conf);

    assertEquals("1337", conf.get(DirectNetezzaManager.NZ_MAXERRORS_CONF));
  }

  /**
   * Test verifies that the "--nz-logdir" option is correctly parsed
   * and present in the configuration used by MR jobs.
   *
   * @throws ParseException
   * @throws InvalidOptionsException
   */
  @Test
  public void testParseExtraArgsLogDir() throws ParseException,
      InvalidOptionsException {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
    String[] args = { "--" + DirectNetezzaManager.NZ_LOGDIR_ARG, "/tmp/nz" };
    Configuration conf = opts.getConf();
    CommandLine parser = m.getParser(args);
    m.applyCliOptions(parser, conf);

    assertEquals("/tmp/nz", conf.get(DirectNetezzaManager.NZ_LOGDIR_CONF));
  }

  /**
   * Test verifies that bad "extra args" are parsed and present in conf's used
   * by MR jobs.
   *
   * @throws ParseException
   * @throws InvalidOptionsException
   */
  @Test
  public void testParseBadExtraArgs() throws ParseException,
      InvalidOptionsException {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
    String[] args = { "--" + DirectNetezzaManager.NZ_MAXERRORS_ARG,
        "notANumber", };
    Configuration conf = opts.getConf();
    try {
      CommandLine parser = m.getParser(args);
      m.applyCliOptions(parser, conf);
    } catch (NumberFormatException nfe) {
      // expected
      return;
    }
    fail("Expected number format exception");
  }

  /**
   * Test method propagateNullSubstituteValues to throw an exception in case
   * that user is trying to override NULL substitution value for non string
   * columns.
   *
   * @throws Exception
   */
  @Test
  public void testPropagateNullSubstituteValues() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
     try {
      m.propagateNullSubstituteValues("a", "b", opts.getConf());
    } catch (RuntimeException x) {
       // expected
       assertTrue(x.getMessage()
         .contains("Detected incompatible NULL substitution strings")
       );
      return;
    }
    fail("Expected to get exception about different input NULL strings");
  }

  /**
   * Test method propagateNullSubstituteValues for ignoring octal based escape
   * sequences.
   *
   * @throws Exception
   */
  @Test
  public void testPropagateNullSubstituteValuesOctalSequence()
    throws Exception {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
     try {
      m.propagateNullSubstituteValues("\\44", null, opts.getConf());
    } catch (RuntimeException x) {
       // expected
       assertTrue(x.getMessage()
         .contains("octal based escape sequences")
       );
      return;
    }
    fail("Expected to get exception about unsupported octal sequences");
  }

  /**
   * Test method propagateNullSubstituteValues for correct propagating.
   *
   * @throws Exception
   */
  @Test
  public void testPropagateNullSubstituteValuesCorrectness() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
    Configuration configuration = new Configuration();

    // Arbitrary string
    m.propagateNullSubstituteValues("cloudera", null, configuration);
    assertEquals("cloudera", configuration.get(PROPERTY_NULL_STRING));

    // De-escaping
    m.propagateNullSubstituteValues("\\\\N", null, configuration);
    assertEquals("\\N", configuration.get(PROPERTY_NULL_STRING));
  }

  @Test
  public void testValidateParameterCompatibilityHBase() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setHBaseTable("table");

    runTestValidateparameterCompatibility(opts, "--hbase-table");
  }

  @Test
  public void testValidateParameterCompatibilityAvro() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setFileLayout(SqoopOptions.FileLayout.AvroDataFile);

    runTestValidateparameterCompatibility(opts, "--as-avrodatafile");
  }

  @Test
  public void testValidateParameterCompatibilitySequence() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setFileLayout(SqoopOptions.FileLayout.SequenceFile);

    runTestValidateparameterCompatibility(opts, "--as-sequencefile");
  }

  @Test
  public void testValidateParameterCompatibilityParquet() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setFileLayout(SqoopOptions.FileLayout.ParquetFile);

    runTestValidateparameterCompatibility(opts, "--as-parquetfile");
  }

  private void runTestValidateparameterCompatibility(SqoopOptions opts, String param) throws Exception {
    DirectNetezzaManager m = new DirectNetezzaManager(opts);

    try {
      m.validateParameterCompatibility(opts);
      fail("Expected exception!");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(param));
    }
  }
}
