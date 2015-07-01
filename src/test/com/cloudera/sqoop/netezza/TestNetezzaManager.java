// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import junit.framework.TestCase;
import org.apache.commons.cli.ParseException;

/**
 * This tests some of the helper functions found in the Direct Netezza Manager.
 */
public class TestNetezzaManager extends TestCase {

  public void testParseExtraArgs() throws ParseException,
      InvalidOptionsException {
    SqoopOptions opts = new SqoopOptions();
    NetezzaManager m = new NetezzaManager(opts);

    assertNull(m.getSchema());

    String[] args = { "--" + NetezzaManager.SCHEMA_ARG, "1337"};
    opts.setExtraArgs(args);
    m.parseExtraArgs(opts.getExtraArgs());

    assertEquals("1337", m.getSchema());
  }

  public void testGetParser() throws ParseException {
    SqoopOptions opts = new SqoopOptions();
    NetezzaManager m = new NetezzaManager(opts);

    assertNull(m.getParser(null));
    assertNotNull(m.getParser(new String[]{"--test", "test"}));
    assertNotNull(m.getParser(null));
  }
}
