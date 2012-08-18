// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.manager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Trivial extension to class to maintain backwards compatiblity with old
 * EnterpriseManagerFactory.
 */
@Deprecated
public class EnterpriseManagerFactory extends NetezzaManagerFactory {

  public static final Log LOG = LogFactory.getLog(NetezzaManagerFactory.class
      .getName());

  public EnterpriseManagerFactory() {
    super();
    LOG.warn("EnterpriseManagerFactory has been deprecated.  "
        + "Use NetezzaManagerFactory instead.");
  }
}
