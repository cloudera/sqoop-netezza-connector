// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages temporary table(s) for TD import and export. Encapsulates the logic
 * for creating such tables, populating them and generating the parameterized
 * queries
 */
public class TemporaryTableGenerator {

  public static final Log LOG = LogFactory
      .getLog(TemporaryTableGenerator.class.getName());
  // private Configuration conf;
  private String tableName;
  private String tempTableName;
  private String[] colNames;
  private int mapperNum;
  private TDBQueryExecutor queryExecutor;
  private Connection connection;

  /**
   * @param conf
   * @param tableName
   * @param tempTableName
   * @param connString
   * @param username
   * @param password
   * @param colTypes
   * @param mapperNum
   */
  public TemporaryTableGenerator(String tableName, String tempTableName,
      Connection connection, String[] colNames, int mapperNum) {
    this.tableName = tableName;
    this.tempTableName = tempTableName;
    this.connection = connection;
    this.colNames = colNames;
    this.mapperNum = mapperNum;
  }

  public int createExportTempTable() throws IOException, SQLException {
    queryExecutor = new TDBQueryExecutor(connection);
    // Drop the table if it exists
    try {
      queryExecutor.executeUpdate("DROP TABLE " + this.tempTableName);
    } catch (SQLException se) {
      // Do nothing because the table likely does not exist yet!
      LOG.debug("Drop table exception ignored: " + se);
    }
    String createTableQuery = "CREATE TABLE " + this.tempTableName + " as "
        + this.tableName + " WITH NO DATA";
    int r = 0;
    r = queryExecutor.executeUpdate(createTableQuery);
    LOG.debug("Query executed, return=" + r);
    queryExecutor.close();
    return r;
  }

  public void createImportTempTable() throws IOException, SQLException {
    queryExecutor = new TDBQueryExecutor(connection);
    // Drop the table if it exists
    try {
      queryExecutor.executeUpdate("DROP TABLE " + this.tempTableName);
    } catch (SQLException se) {
      // Do nothing because the table likely does not exist yet!
      LOG.debug("Drop table exception ignored: " + se);
    }
    // create an empty copy of the input table
    String createTableQuery = "CREATE TABLE " + this.tempTableName + " AS "
        + this.tableName + " WITH NO DATA";
    queryExecutor.executeUpdate(createTableQuery);
    // Add the index column to the created table
    String addIndxColumnQuery = "ALTER TABLE " + this.tempTableName
        + " ADD index_column INTEGER";
    queryExecutor.executeUpdate(addIndxColumnQuery);
    // Modify primary index and partitioning information
    String pIndxQuery = "ALTER TABLE "
        + this.tempTableName
        + " MODIFY PRIMARY INDEX( index_column ) PARTITION BY RANGE_N( CAST( "
        + "( index_column MOD " + this.mapperNum + " ) AS INTEGER ) BETWEEN "
        + "0 AND " + (this.mapperNum - 1) + " EACH 1 )";
    queryExecutor.executeUpdate(pIndxQuery);
    queryExecutor.close();
  }

  public int populateImportTempTable() throws SQLException, IOException {
    queryExecutor = new TDBQueryExecutor(connection);
    String insertQuery = getInsertQuery();
    int r = queryExecutor.executeUpdate(insertQuery);
    queryExecutor.close();
    return r;
  }

  /**
   * @return
   */
  private String getInsertQuery() {
    StringBuilder sb = new StringBuilder();
    sb.append("insert into " + this.tempTableName + " select ");
    for (String colName : colNames) {
      sb.append(colName + ", ");
    }
    sb.append("random(0," + (this.mapperNum - 1) + ") as index_column from "
        + this.tableName);
    return sb.toString();
  }

  /**
   * @return
   */
  public String getParameterizedQuery() {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    String delim = "";
    for (String colName : colNames) {
      sb.append(delim);
      sb.append(colName);
      delim = ", ";
    }
    sb.append(" FROM " + this.tempTableName + " WHERE index_column = %N%");
    return sb.toString();
  }

}
