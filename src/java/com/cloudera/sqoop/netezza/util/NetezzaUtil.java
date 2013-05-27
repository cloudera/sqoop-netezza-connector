// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.
package com.cloudera.sqoop.netezza.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.netezza.DirectNetezzaManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Convenience methods for Netezza connector.
 */
public final class NetezzaUtil {

  public static final Log LOG = LogFactory.getLog(NetezzaUtil.class.getName());

  // List of items that needs to be de-escaped in order to be consistent with
  // upstream sqoop interpretation of the NULL string related parameters.
  private static final Map<String, String> REMOVE_ESCAPE_CHARS;

  static {
    // Build static map of escape characters that needs to be de-escaped.
    // http://docs.oracle.com/javase/specs/jls/se7/html/jls-3.html#jls-3.10.6
    REMOVE_ESCAPE_CHARS = new HashMap<String, String>();
    REMOVE_ESCAPE_CHARS.put("\\\\b", "\b");
    REMOVE_ESCAPE_CHARS.put("\\\\t", "\t");
    REMOVE_ESCAPE_CHARS.put("\\\\n", "\n");
    REMOVE_ESCAPE_CHARS.put("\\\\f", "\f");
    REMOVE_ESCAPE_CHARS.put("\\\\'", "'");
    REMOVE_ESCAPE_CHARS.put("\\\\\"", "\"");
    REMOVE_ESCAPE_CHARS.put("\\\\\\\\", "\\\\");
    // TODO(jarcec, optional): Deal with octal escape sequences?
  }

  /**
   * Creates the log directory specified by the configuration entry
   * {@link DirectNetezzaManager#NZ_LOGDIR_CONF}.
   *
   * @param conf the job configuration
   * @throws IOException if the directory could not be created correctly
   */
  public static void createLogDirectoryIfSpecified(Configuration conf)
      throws IOException {
    if (isNzLogEnabled(conf)) {
      String logDir = conf.get(DirectNetezzaManager.NZ_LOGDIR_CONF);
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

  /**
   * Uploads Netezza logs into HDFS directory.
   *
   * @param configuration Job configuration
   * @param prefix Prefix for all uploaded files
   */
  public static void uploadLogsToHdfsIfSpecified(Configuration configuration, String prefix) {
    if (!isNzLogEnabled(configuration)) {
      LOG.info("Netezza logging is disabled, skipping uploading logs.");
      return;
    }

    if(!isNzLogUploadEnabled(configuration)) {
      LOG.info("Netezza log upload is disabled, skipping uploading logs.");
      return;
    }

    String logDir = configuration.get(DirectNetezzaManager.NZ_LOGDIR_CONF);
    String uploadDir = configuration.get(DirectNetezzaManager.NZ_UPLOADDIR_CONF);

    FileSystem fs = null;
    try {
      fs = FileSystem.get(configuration);
    } catch (IOException e) {
      LOG.error("Can't upload logs to HDFS", e);
      return;
    }

    for(File file : new File(logDir).listFiles()) {
      LOG.info("Uploading file " + file);
      try {
        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(uploadDir, prefix + '-' + file.getName() ));
      } catch (IOException e) {
        LOG.error("Can't upload file: " + file, e);
      }
    }
  }

  public static boolean isNzLogEnabled(Configuration configuration) {
    String logDir = configuration.get(DirectNetezzaManager.NZ_LOGDIR_CONF);
    return logDir != null && logDir.trim().length() > 0;
  }

  public static boolean isNzLogUploadEnabled(Configuration configuration) {
    String logDir = configuration.get(DirectNetezzaManager.NZ_UPLOADDIR_CONF);
    return logDir != null && logDir.trim().length() > 0;
  }

  /**
   * De-escape all escape sequences presented in the string.
   *
   * @param str String to de-escape
   * @return String without escape sequences
   */
  public static String removeEscapeCharacters(String str) {
    for (Map.Entry<String, String> entry : REMOVE_ESCAPE_CHARS.entrySet()) {
      str = str.replaceAll(entry.getKey(), entry.getValue());
    }
    return str;
  }

  /**
   * Null safe comparison of two String objects.
   *
   * @param first First string object
   * @param second Second string object
   * @return
   */
  public static boolean nullSafeCompareTo(String first, String second) {
    if (first == null && second != null) { return false; }
    if (first != null && second == null) { return false; }
    if (first == null && second == null) { return true; }

    return first.equals(second);
  }

  private NetezzaUtil() {
    // Disable explicit object creation
  }
}
