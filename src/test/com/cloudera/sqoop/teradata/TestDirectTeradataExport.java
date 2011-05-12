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
   * @see com.cloudera.sqoop.teradata.TestTeradataExport#getTableSuffix()
   */
  protected String getTableSuffix() {
    // Should return an empty string, returning "_temp_0" to compare
    // against the temporary output table before merging. This should be
    // corrected after MAPREDUCE-2350 is resolved.
    if(TdTestUtil.TERADATA_EXPORT_TEMP_TABLE.equals("true")){
        return "_temp_0";
    } else {
        return super.getTableSuffix();
    }
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


