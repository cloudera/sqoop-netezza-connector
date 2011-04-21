// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;

/**
 * Manages query executions against TD EDW.
 */
public class TDBQueryExecutor {

  private DBConfiguration dbConf;
  public static final Log LOG = LogFactory.getLog(TDBQueryExecutor.class
      .getName());

  /**
   * @param connection
   */
  public TDBQueryExecutor(DBConfiguration dbConfiguration) {
    this.dbConf = dbConfiguration;
  }

  /**
   * @param queryString
   * @return
   * @throws Exception 
   */
  public ResultSet executeQuery(final String queryString) throws Exception {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      connection = dbConf.getConnection();
      connection.setAutoCommit(false);
      statement = connection.createStatement();
      LOG.debug("Trying to execute query: " + queryString);
      resultSet = statement.executeQuery(queryString);
      connection.commit();
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (SQLException e1) {
        LOG.debug(e.getMessage(), e);
      }
      throw e;
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
    return resultSet;
  }

  /**
   * @param queryString
   * @return
   * @throws Exception 
   */
  public int executeUpdate(final String queryString) throws Exception {
    Connection connection = null;
    Statement statement = null;
    int r = 0;
    try {
      connection = dbConf.getConnection();
      connection.setAutoCommit(false);
      statement = connection.createStatement();
      LOG.debug("Trying to execute query: " + queryString);
      r = statement.executeUpdate(queryString);
      connection.commit();
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (SQLException e1) {
        LOG.debug(e.getMessage(), e);
      }
      throw e;
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
    return r;
  }

}
