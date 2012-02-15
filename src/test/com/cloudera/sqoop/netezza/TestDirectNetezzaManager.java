// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import junit.framework.TestCase;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;

/**
 * This tests some of the helper functions found in the Direct Netezza Manager.
 */
public class TestDirectNetezzaManager extends TestCase {

  /**
   * Test verifies that the "extra args" are parsed and present in conf's used
   * by MR jobs.
   *
   * @throws ParseException
   * @throws InvalidOptionsException
   */
  public void testParseExtraArgs() throws ParseException,
      InvalidOptionsException {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
    String[] args = { "--" + DirectNetezzaManager.NZ_MAXERRORS_ARG, "1337" };
    Configuration conf = opts.getConf();
    m.parseExtraArgs(args, conf);

    assertEquals("1337", conf.get(DirectNetezzaManager.NZ_MAXERRORS_CONF));
  }

  /**
   * Test verifies that the "--nz-logdir" option is correctly parsed
   * and present in the configuration used by MR jobs.
   *
   * @throws ParseException
   * @throws InvalidOptionsException
   */
  public void testParseExtraArgsLogDir() throws ParseException,
      InvalidOptionsException {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
    String[] args = { "--" + DirectNetezzaManager.NZ_LOGDIR_ARG, "/tmp/nz" };
    Configuration conf = opts.getConf();
    m.parseExtraArgs(args, conf);

    assertEquals("/tmp/nz", conf.get(DirectNetezzaManager.NZ_LOGDIR_CONF));
  }

  /**
   * Test verifies that bad "extra args" are parsed and present in conf's used
   * by MR jobs.
   *
   * @throws ParseException
   * @throws InvalidOptionsException
   */
  public void testParseBadExtraArgs() throws ParseException,
      InvalidOptionsException {
    SqoopOptions opts = new SqoopOptions();
    DirectNetezzaManager m = new DirectNetezzaManager(opts);
    String[] args = { "--" + DirectNetezzaManager.NZ_MAXERRORS_ARG,
        "notANumber", };
    Configuration conf = opts.getConf();
    try {
      m.parseExtraArgs(args, conf);
    } catch (NumberFormatException nfe) {
      // expected
      return;
    }
    fail("Expected number format exception");
  }
}
