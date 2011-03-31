// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages query executions against TD EDW.
 */
public class TDBQueryExecutor {

  private ResultSet dbResultSet = null;
  private Statement dbStatement = null;
  private Connection dbConnection = null;
  public static final Log LOG = LogFactory.getLog(TDBQueryExecutor.class
      .getName());

  /**
   * @param connection
   */
  public TDBQueryExecutor(Connection connection) {
    this.dbConnection = connection;
  }

  /**
   * @param queryString
   * @return
   * @throws SQLException
   */
  public ResultSet executeQuery(final String queryString) throws SQLException {
    dbConnection.setAutoCommit(false);
    dbConnection
    .setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    dbStatement = dbConnection.createStatement(
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    LOG.debug("Trying to execute query: " + queryString);
    try {
      dbResultSet = dbStatement.executeQuery(queryString);
      dbConnection.commit();
    } catch (SQLException se){
      dbConnection.rollback();
      throw se;
    }
    return dbResultSet;
  }

  /**
   * @param queryString
   * @return
   * @throws SQLException 
   */
  public int executeUpdate(final String queryString) throws SQLException {
    int r = 0;
    dbConnection.setAutoCommit(false);
    dbConnection
    .setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    dbStatement = dbConnection.createStatement(
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    LOG.debug("Trying to execute query: " + queryString);
    try {
      r = dbStatement.executeUpdate(queryString);
      dbConnection.commit();
    } catch (SQLException se){
      dbConnection.rollback();
      throw se;
    }
    return r;
  }

  /**
   * @throws IOException
   * @throws SQLException 
   */
  public void close() throws IOException, SQLException {
      if (dbResultSet != null) {
        dbResultSet.close();
      }
      if (dbStatement != null) {
        dbStatement.close();
      }
  }

}
