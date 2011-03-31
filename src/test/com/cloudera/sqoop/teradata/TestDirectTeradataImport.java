// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;

/**
 * Test the Teradata EDW connector for direct mode imports.
 */
public class TestDirectTeradataImport extends TestTeradataImport {

  private static final Log LOG = LogFactory
      .getLog(TestDirectTeradataImport.class.getName());

  /* (non-Javadoc)
   * @see com.cloudera.sqoop.teradata.TestTeradataImport#getEdwFriendlyName()
   */
  protected String getEdwFriendlyName() {
    return "directteradata";
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.cloudera.sqoop.teradata.TestTeradataImport#getSqoopOptions(org.apache
   * .hadoop.conf.Configuration)
   */
  @Override
  public SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions options = super.getSqoopOptions(conf);
    // Use direct mode for this.
    options.setDirectMode(true);
    options.setNumMappers(1); // Point can be proven with 1 mapper.
    return options;
  }

  /**
   * @throws Exception
   */
  public void testMultipleMappers() throws Exception {
    final String TABLE_NAME = "MULTI_TABLE";
    createTable(TABLE_NAME, "INTEGER", "VARCHAR(32)");
    addRow(conn, TABLE_NAME, "1", "'foo'");
    addRow(conn, TABLE_NAME, "2", "'bar'");
    addRow(conn, TABLE_NAME, "3", "'baz'");
    addRow(conn, TABLE_NAME, "4", "'biff'");
    options.setNumMappers(2);
    runImport(options, TABLE_NAME);
    verifyImportCount(TABLE_NAME, 4);
    verifyImportLine(TABLE_NAME, "2,bar");
    verifyImportLine(TABLE_NAME, "1,foo");
    verifyImportLine(TABLE_NAME, "3,baz");
  }
}
