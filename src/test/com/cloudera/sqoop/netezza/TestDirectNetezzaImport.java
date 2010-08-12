// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;

import junit.framework.TestCase;

/**
 * Test the Netezza EDW connector for direct mode imports.
 */
public class TestDirectNetezzaImport extends TestCase {

  private static Log LOG =
      LogFactory.getLog(TestDirectNetezzaImport.class.getName());

  protected String getDbFriendlyName() {
    return "directnetezza";
  }

  /**
   * Create a SqoopOptions to connect to the manager.
   */
  public SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions options = new SqoopOptions(conf);
    NzTestUtil.initSqoopOptions(options);
    options.setDirectMode(true);
    options.setNumMappers(1);

    return options;
  }
}

