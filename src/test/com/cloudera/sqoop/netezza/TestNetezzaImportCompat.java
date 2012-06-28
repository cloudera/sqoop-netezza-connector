// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.EnterpriseManagerFactory;
import com.cloudera.sqoop.testutil.ManagerCompatTestCase;

/**
 * Test the Netezza EDW connector against the standard Sqoop compatibility
 * test.
 */
public class TestNetezzaImportCompat extends ManagerCompatTestCase {

  private Log log = LogFactory.getLog(TestNetezzaImportCompat.class.getName());

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

  protected String getFixedCharDbOut(int fieldWidth, String asInserted) {
    return padString(fieldWidth, asInserted);
  }

  @Override
  protected String getFixedCharSeqOut(int fieldWidth, String asInserted) {
    return padString(fieldWidth, asInserted);
  }

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

  protected String getNumericDbOutput(String asInserted) {
    return zeroPad(asInserted, 5);
  }

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
  
  static final String STRING_VAL_IN = "'this is a short string'";
  static final String STRING_VAL_OUT = "this is a short string";
  
  @Test
  public void testNVarCharStringCol1() {
    verifyType("NVARCHAR(32)", STRING_VAL_IN, STRING_VAL_OUT);
  }

  
  @Test
  public void testNVarCharEmptyStringCol() {   
    verifyType("NVARCHAR(32)", "''", "");   
  }

  @Test
  public void testNVarCharNullStringCol() {
      verifyType("NVARCHAR(32)", "NULL", null);
  }

  @Test
  public void testNumeric1() {
    verifyType(getNumericType(), "1",
        getNumericSeqOutput("1.00000"));
  }

  @Test
  public void testNumeric2() {
    verifyType(getNumericType(), "-10",
        getNumericSeqOutput("-10.00000"));
  }

  @Test
  public void testDecimal1() {
    verifyType(getDecimalType(), "1",
        getDecimalSeqOutput("1.00000"));
  }

  @Test
  public void testDecimal2() {
    verifyType(getDecimalType(), "-10",
        getDecimalSeqOutput("-10.00000"));
  }

}

