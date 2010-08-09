/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import com.cloudera.sqoop.SqoopOptions;

import com.cloudera.sqoop.manager.EnterpriseManagerFactory;
import com.cloudera.sqoop.testutil.ManagerCompatTestCase;

import com.cloudera.sqoop.util.AsyncSink;

/**
 * Test the Netezza EDW connector against the standard Sqoop compatibility
 * test.
 */
public class TestNetezza extends ManagerCompatTestCase {

  private Log log = LogFactory.getLog(TestNetezza.class.getName());

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

  @Override
  protected Log getLogger() {
    return log;
  }

  @Override
  protected String getDbFriendlyName() {
    return "netezza";
  }

  @Override
  public String getConnectString() {
    return "jdbc:netezza://" + NETEZZA_HOST + "/" + NETEZZA_DB;
  }

  @Override
  /**
   * {@inheritDoc}
   *
   * <p>Set the netezza user/pass to use.</p>
   */
  public SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions options = super.getSqoopOptions(conf);
    options.setUsername(NETEZZA_USER);
    options.setPassword(NETEZZA_PASS);

    return options;
  }

  @Override
  /**
   * {@inheritDoc}
   *
   * <p>Configure the ManagerFactory to use.</p> 
   */
  public Configuration getConf() {
    Configuration conf = super.getConf();
    conf.set("sqoop.connection.factories",
        EnterpriseManagerFactory.class.getName());
    return conf;
  }

  @Override
  public void dropTableIfExists(String tableName) throws SQLException {
    Connection conn = getManager().getConnection();
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

  @Override
  protected boolean supportsClob() {
    return false;
  }

  @Override
  protected boolean supportsBlob() {
    return false;
  }

  @Override
  protected boolean supportsVarBinary() {
    return false;
  }

  @Override
  protected boolean supportsLongVarChar() {
    return false;
  }

  @Override
  protected boolean supportsBoolean() {
    // Boolean split columns are not supported by NZ; it does not understand
    // the MIN() function over a boolean column.
    return false;
  }

  @Override
  protected String getTinyIntType() {
    return "BYTEINT";
  }

  // CHAR() types are space-padded in Netezza.

  @Override
  protected String getFixedCharDbOut(int fieldWidth, String asInserted) {
    return padString(fieldWidth, asInserted);
  }

  @Override
  protected String getFixedCharSeqOut(int fieldWidth, String asInserted) {
    return padString(fieldWidth, asInserted);
  }

  @Override
  protected String getTimestampDbOutput(String tsAsInserted) {
    // Default getTimestampDbOutput() adds a lot of zero-padding;
    // SeqOutput() does not. We need the SeqOutput behavior here.
    return getTimestampSeqOutput(tsAsInserted);
  }

  /**
   * Return the input decimalStr padded to 'precision'
   * elements to the right of the dot.
   *
   * Example inputs: "10", "10.", "10.4", "10.031"
   */
  private String zeroPad(String decimalStr, int precision) {
    if (null == decimalStr || "null".equals(decimalStr)) {
      return decimalStr;
    }
   
    StringBuilder sb = new StringBuilder();
    sb.append(decimalStr);

    int dotPos = decimalStr.indexOf(".");
    int remaining;
    if (-1 == dotPos) {
      sb.append(".");
      remaining = precision;
    } else {
      remaining = precision - (decimalStr.length() - dotPos - 1);
    }

    for (int i = 0; i < remaining; i++) {
      sb.append("0");
    }

    return sb.toString();
  }

  @Override
  protected String getNumericDbOutput(String asInserted) {
    return zeroPad(asInserted, 5);
  }

  @Override
  protected String getDecimalDbOutput(String asInserted) {
    return zeroPad(asInserted, 5);
  }

  @Override
  public void tearDown() {
    super.tearDown();
    try {
      clearNzSessions();
    } catch (Exception e) {
      LOG.warn("Could not clear nzsessions: " + e);
    }
  }

  /**
   * Use the 'nzsession' program to clear out any persistent sessions.
   * This is a hack; somehow, the Connection.close() call occurring in
   * tearDown() is not actually closing the open netezza sessions. After
   * 32 connections open like this, subsequent tests will deadlock.
   * This method terminates the sessions forcefully.
   */
  private void clearNzSessions() throws IOException, InterruptedException {
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
}

