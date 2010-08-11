// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import com.cloudera.sqoop.SqoopOptions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test exports over FIFO to Netezza.
 */
public class TestNetezzaDirectExport extends TestNetezzaJdbcExport {

  public static final Log LOG = LogFactory.getLog(
        TestNetezzaDirectExport.class.getName());

  protected String getTablePrefix() {
    return "NZ_DIRECT_TBL_";
  }

  protected SqoopOptions getSqoopOptions() {
    SqoopOptions options = super.getSqoopOptions();
    options.setDirectMode(true);
    return options;
  }
}


