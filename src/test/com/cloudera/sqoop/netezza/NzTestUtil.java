// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.ConnFactory;
import com.cloudera.sqoop.SqoopOptions;

import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.EnterpriseManagerFactory;

import com.cloudera.sqoop.util.AsyncSink;

/**
 * Utilities for testing Netezza.
 */
public class NzTestUtil {

  private static Log LOG = LogFactory.getLog(NzTestUtil.class.getName());

  /** Hostname in /etc/hosts for the Netezza test database. */
  public static final String NETEZZA_HOST = "nzhost";

  /** DB schema to use on the host. */
  public static final String NETEZZA_DB = "sqooptestdb";

  /** Netezza DB username. */
  public static final String NETEZZA_USER = "sqooptest";

  /** Netezza DB password. */
  public static final String NETEZZA_PASS = "sqooptest";

  // Due to a bug holding connections open within the same process, we need to
  // login as admin and clear state between tests using the 'nzsession'
  // executable.

  public static final String ADMIN_USER = "admin";
  public static final String ADMIN_PASS = "password";

  // System property setting the path to the nzsession executable.
  public static final String NZ_SESSION_PATH_KEY = "nz.session.path";
  public static final String DEFAULT_NZ_SESSION_PATH =
      "/usr/local/nz/bin/nzsession";

  public static String getConnectString() {
    return "jdbc:netezza://" + NzTestUtil.NETEZZA_HOST + "/"
        + NzTestUtil.NETEZZA_DB;
  }

  /**
   * Use the 'nzsession' program to clear out any persistent sessions.
   * This is a hack; somehow, the Connection.close() call occurring in
   * tearDown() is not actually closing the open netezza sessions. After
   * 32 connections open like this, subsequent tests will deadlock.
   * This method terminates the sessions forcefully.
   */
  public void clearNzSessions() throws IOException, InterruptedException {
    String pathToNzSession = System.getProperty(
        NZ_SESSION_PATH_KEY, DEFAULT_NZ_SESSION_PATH);

    // Run nzsession and capture a list of open transactions.
    ArrayList<String> args = new ArrayList<String>();
    args.add(pathToNzSession);
    args.add("-host");
    args.add(NETEZZA_HOST);
    args.add("-u");
    args.add(ADMIN_USER);
    args.add("-pw");
    args.add(ADMIN_PASS);

    Process p = Runtime.getRuntime().exec(args.toArray(new String[0]));
    InputStream is = p.getInputStream(); 
    LineBufferingAsyncSink sink = new LineBufferingAsyncSink();
    sink.processStream(is);

    // Wait for the process to end.
    int result = p.waitFor();
    if (0 != result) {
      throw new IOException("Session list command terminated with " + result);
    }

    // Collect all the stdout, and parse the output.
    // If the third whitespace-delimited token is the sqooptest user,
    // the the first token is the nzsession id. We should kill that id.
    sink.join();
    List<String> processList = sink.getLines();
    for (String processLine : processList) {
      if (null == processLine || processLine.length() == 0) {
        continue; // Ignore empty lines.
      }

      String [] tokens = processLine.split(" +");
      if (tokens.length < 3) {
        continue; // Not enough tokens on this line.
      }
      if (tokens[2].equalsIgnoreCase(NETEZZA_USER)) {
        // Found a match.
        killSession(tokens[0]);
      }
    }
  }

  private void killSession(String sessionIdStr)
      throws IOException, InterruptedException {
    String pathToNzSession = System.getProperty(
        NZ_SESSION_PATH_KEY, DEFAULT_NZ_SESSION_PATH);

    // Run nzsession and capture a list of open transactions.
    ArrayList<String> args = new ArrayList<String>();
    args.add(pathToNzSession);
    args.add("abort");
    args.add("-host");
    args.add(NETEZZA_HOST);
    args.add("-u");
    args.add(ADMIN_USER);
    args.add("-pw");
    args.add(ADMIN_PASS);
    args.add("-force");
    args.add("-id");
    args.add(sessionIdStr);

    Process p = Runtime.getRuntime().exec(args.toArray(new String[0]));
    int result = p.waitFor();
    if (0 != result) {
      LOG.error("Could not kill session; exit status " + result);
    }
  }

  /**
   * An AsyncSink that takes the contents of a stream and stores the
   * retrieved lines in an array.
   */
  public class LineBufferingAsyncSink extends AsyncSink {

    public final Log LOG = LogFactory.getLog(
        LineBufferingAsyncSink.class.getName());

    public LineBufferingAsyncSink() {
      this.lines = new ArrayList<String>();
    }

    private Thread child;
    private List<String> lines;

    public void processStream(InputStream is) {
      child = new BufferThread(is);
      child.start();
    }

    public int join() throws InterruptedException {
      child.join();
      return 0; // always successful.
    }

    public List<String> getLines() {
      return this.lines;
    }

    /**
     * Run a background thread that copies the contents of the stream
     * to the array buffer.
     */
    private class BufferThread extends Thread {

      private InputStream stream;

      BufferThread(final InputStream is) {
        this.stream = is;
      }

      public void run() {
        InputStreamReader isr = new InputStreamReader(this.stream);
        BufferedReader r = new BufferedReader(isr);

        try {
          while (true) {
            String line = r.readLine();
            if (null == line) {
              break; // stream was closed by remote end.
            }

            LineBufferingAsyncSink.this.lines.add(line);
          }
        } catch (IOException ioe) {
          LOG.error("IOException reading from stream: " + ioe.toString());
        }

        try {
          r.close();
        } catch (IOException ioe) {
          LOG.warn("Error closing stream in LineBufferingAsyncSink: "
              + ioe.toString());
        }
      }
    }
  }

  public static Configuration initConf(Configuration conf) {
    conf.set("sqoop.connection.factories",
        EnterpriseManagerFactory.class.getName());
    return conf;
  }

  public static SqoopOptions initSqoopOptions(SqoopOptions options) {
    options.setConnectString(NzTestUtil.getConnectString());
    options.setUsername(NzTestUtil.NETEZZA_USER);
    options.setPassword(NzTestUtil.NETEZZA_PASS);

    return options;
  }

  public static ConnManager getNzManager(SqoopOptions options)
      throws IOException {
    initSqoopOptions(options);
    ConnFactory cf = new ConnFactory(options.getConf());
    return cf.getManager(options);
  }

  public static void dropTableIfExists(Connection conn, String tableName)
      throws SQLException {
    PreparedStatement s = null;
    try {
      s = conn.prepareStatement("DROP TABLE " + tableName);
      s.executeUpdate();
      conn.commit();
    } catch (SQLException sqlE) {
      // DROP TABLE may not succeed; the table might not exist. Just continue.
      LOG.warn("Ignoring SQL Exception dropping table " + tableName
          + " : " + sqlE);

      // Clear current query state.
      conn.rollback();
    } finally {
      if (null != s) {
        s.close();
      }
    }
  }
}

