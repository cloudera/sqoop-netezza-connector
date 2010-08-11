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

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.EnterpriseManagerFactory;
import com.cloudera.sqoop.testutil.ManagerCompatTestCase;

/**
 * Test the Netezza EDW connector against the standard Sqoop compatibility
 * test.
 */
public class TestNetezzaImportCompat extends ManagerCompatTestCase {

  private Log log = LogFactory.getLog(TestNetezza.class.getName());

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
    return NzTestUtil.getConnectString();
  }

  @Override
  /**
   * {@inheritDoc}
   *
   * <p>Set the netezza user/pass to use.</p>
   */
  public SqoopOptions getSqoopOptions(Configuration conf) {
    SqoopOptions options = super.getSqoopOptions(conf);
    options.setUsername(NzTestUtil.NETEZZA_USER);
    options.setPassword(NzTestUtil.NETEZZA_PASS);

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
    NzTestUtil.dropTableIfExists(conn, tableName);
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
      new NzTestUtil().clearNzSessions();
    } catch (Exception e) {
      log.warn("Could not clear nzsessions: " + e);
    }
  }
}

