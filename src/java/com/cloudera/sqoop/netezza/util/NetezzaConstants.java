// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.
package com.cloudera.sqoop.netezza.util;

/**
 * Constants that are used inside Netezza Sqoop connector.
 */
public final class NetezzaConstants {

  /**
   * Internal property that will be saved into mapreduce job to transfer text
   * that should be used to encode NULL values.
   */
  public static final String PROPERTY_NULL_STRING =
    "com.cloudera.sqoop.netezza.null.string";

  private NetezzaConstants() {
    // Not meant to be instantied
  }
}
