// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import org.apache.commons.cli.ParseException;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.SqoopOptions.InvalidOptionsException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * This tests some of the helper functions found in the Direct Netezza Manager.
 */
public class TestNetezzaManager {

  @Test
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

  @Test
  public void testGetParser() throws ParseException {
    SqoopOptions opts = new SqoopOptions();
    NetezzaManager m = new NetezzaManager(opts);

    assertNull(m.getParser(null));
    assertNotNull(m.getParser(new String[]{"--test", "test"}));
    assertNotNull(m.getParser(null));
  }
}
