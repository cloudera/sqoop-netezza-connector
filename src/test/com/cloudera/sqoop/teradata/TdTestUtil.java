// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.ConnFactory;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.EnterpriseManagerFactory;
import com.cloudera.sqoop.metastore.JobData;
import com.cloudera.sqoop.tool.ImportTool;

/**
 * Utilities for testing Teradata.
 */
public class TdTestUtil {

  private static final Log LOG = LogFactory
      .getLog(TdTestUtil.class.getName());

  protected TdTestUtil(){
    // prevents calls from subclass
    throw new UnsupportedOperationException();
  }

  /** Hostname in /etc/hosts for the Teradata test database. */
  public static final String TERADATA_HOST = System.getProperty(
      "sqoop.teradata.host", "td_host");

  /** DB schema to use on the host. */
  public static final String TERADATA_DB = System.getProperty(
      "sqoop.teradata.database", "td_database");

  /** Teradata DB username. */
  public static final String TERADATA_USER = System.getProperty(
      "sqoop.teradata.user", "td_user");

  /** Teradata DB password. */
  public static final String TERADATA_PASS = System.getProperty(
      "sqoop.teradata.password", "td_password");

  /**
   * A property that determines, in the direct export use case, if results are
   * to be compared to the temporary table before merging or to the final output
   * table. This property is required to be set to "true" during testing because
   * of a Hadoop issue MAPREDUCE-2350.
   */
  public static final String TERADATA_EXPORT_TEMP_TABLE = System.getProperty(
      "sqoop.teradata.export.temp_table", "false");

  private static final String SPLITBY_COLUMN = "col0";

  /**
   * @return The connection string for connecting to the Teradata test
   *         instance
   */
  public static String getConnectString() {
    return "jdbc:teradata://" + TdTestUtil.TERADATA_HOST + "/"
        + TdTestUtil.TERADATA_DB;
  }

  /**
   * @param conf
   *          The configuration object to be initialized
   * @return The initialized Configuration
   */
  public static Configuration initConf(Configuration conf) {
    conf.set("sqoop.connection.factories", EnterpriseManagerFactory.class
        .getName());
    return conf;
  }

  /**
   * @param options
   *          The SqoopOptions object to be initialized
   * @return the initialized SqoopOptions
   */
  public static SqoopOptions initSqoopOptions(SqoopOptions options) {
    options.setConnectString(TdTestUtil.getConnectString());
    options.setUsername(TdTestUtil.TERADATA_USER);
    options.setPassword(TdTestUtil.TERADATA_PASS);
    options.setSplitByCol(TdTestUtil.SPLITBY_COLUMN);
    return options;
  }

  /**
   * @param options
   *          The SqoopOptions object
   * @return The connection manager
   * @throws IOException
   */
  public static ConnManager getTdManager(SqoopOptions options)
      throws IOException {
    initSqoopOptions(options);
    ConnFactory cf = new ConnFactory(options.getConf());
    return cf.getManager(new JobData(options, new ImportTool()));
  }

  /**
   * @param tableName
   *          The table name to be dropped
   * @throws IOException
   * @throws SQLException 
   * @throws ClassNotFoundException 
   */
  public static void dropTableIfExists(String tableName) throws IOException,
      ClassNotFoundException, SQLException {
    Connection connection = null;
    Statement statement = null;
    String queryString = "DROP TABLE " + tableName;
    try {
      Class.forName("com.teradata.jdbc.TeraDriver");
      connection = DriverManager
      .getConnection(getConnectString(), TERADATA_USER, TERADATA_PASS);
      connection.setAutoCommit(false);
      statement = connection.createStatement();
      LOG.debug("Trying to execute query: " + queryString);
      statement.executeUpdate(queryString);
      connection.commit();
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (SQLException e1) {
        LOG.debug(e.getMessage(), e);
      }
      LOG.debug("Drop table exception ignored: " + e);
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
        if (connection != null) {
          connection.close();
        }
      } catch (SQLException e) {
        LOG.debug(e.getMessage(), e);
      }
    }
  }

}
