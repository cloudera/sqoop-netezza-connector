// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata;

import com.cloudera.sqoop.SqoopOptions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test direct export for Teradata.
 */
public class TestDirectTeradataExport extends TestTeradataExport {

  public static final Log LOG = LogFactory.getLog(
        TestDirectTeradataExport.class.getName());

  /* (non-Javadoc)
   * @see com.cloudera.sqoop.teradata.TestTeradataExport#getTablePrefix()
   */
  protected String getTablePrefix() {
    return "TD_DIRECT_TBL_";
  }

  /* (non-Javadoc)
   * @see com.cloudera.sqoop.teradata.TestTeradataExport#getSqoopOptions()
   */
  protected SqoopOptions getSqoopOptions() {
    SqoopOptions options = super.getSqoopOptions();
    options.setDirectMode(true);
    return options;
  }
}


