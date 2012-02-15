// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.
package com.cloudera.sqoop.netezza.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.netezza.DirectNetezzaManager;

public final class NetezzaUtil {

  /**
   * Creates the log directory specified by the configuration entry
   * {@link DirectNetezzaManager#NZ_LOGDIR_CONF}.
   *
   * @param conf the job configuration
   * @throws IOException if the directory could not be created correctly
   */
  public static void createLogDirectoryIfSpecified(Configuration conf)
      throws IOException {
    String logDir = conf.get(DirectNetezzaManager.NZ_LOGDIR_CONF);
    if (logDir != null && logDir.trim().length() > 0) {
      File logDirFile = new File(logDir);

      // Always call mkdirs to avoid a duplicate redundant exists() check
      // and also not depend upon the return value of this call.
      logDirFile.mkdirs();

      // The directory should exist now and must be writable
      if (!logDirFile.exists() || !logDirFile.isDirectory()
          || !logDirFile.canWrite()) {
        throw new IOException("Specified LOGDIR is invalid: " + logDir);
      }
    }
  }


  private NetezzaUtil() {
    // Disable explicit object creation
  }
}
