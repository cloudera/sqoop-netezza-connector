// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;

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
  private DBConfiguration dbConf;

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
      DBConfiguration dbConfiguration, String[] columnNames, int mapperNum) {
    this.tableName = tableName;
    this.tempTableName = tempTableName;
    this.dbConf = dbConfiguration;
    if (null != columnNames) {
      colNames = new String[columnNames.length];
      System.arraycopy(columnNames, 0, colNames, 0, columnNames.length);
    }
    this.mapperNum = mapperNum;
  }

  public int createExportTempTable() throws Exception {
    queryExecutor = new TDBQueryExecutor(dbConf);
    // Drop the table if it exists
    try {
      queryExecutor.executeUpdate("DROP TABLE " + this.tempTableName);
    } catch (Exception se) {
      // Do nothing because the table likely does not exist yet!
      LOG.debug("Drop table exception ignored: " + se);
    }
    String createTableQuery = "CREATE TABLE " + this.tempTableName + " as "
        + this.tableName + " WITH NO DATA";
    int r = queryExecutor.executeUpdate(createTableQuery);
    LOG.debug("Query executed, return=" + r);
    return r;
  }

  public void createImportTempTable() throws Exception {
    queryExecutor = new TDBQueryExecutor(dbConf);
    // Drop the table if it exists
    try {
      queryExecutor.executeUpdate("DROP TABLE " + this.tempTableName);
    } catch (Exception se) {
      // Do nothing because the table likely does not exist yet!
      LOG.debug("Drop table exception ignored: " + se);
    }
    // create an empty copy of the input table
    String createTableQuery = "CREATE TABLE " + this.tempTableName + " AS "
        + this.tableName + " WITH NO DATA";
    queryExecutor.executeUpdate(createTableQuery);
    // Add the index column to the created table
    String addIndxColumnQuery = "ALTER TABLE " + this.tempTableName + " ADD "
        + TeradataConstants.INDEX_COLUMN + " INTEGER";
    queryExecutor.executeUpdate(addIndxColumnQuery);
    // Modify primary index and partitioning information
    String pIndxQuery = "ALTER TABLE " + this.tempTableName
        + " MODIFY NOT UNIQUE PRIMARY INDEX( "
        + TeradataConstants.INDEX_COLUMN + " ) PARTITION BY RANGE_N( CAST( "
        + "( " + TeradataConstants.INDEX_COLUMN + " MOD " + this.mapperNum
        + " ) AS INTEGER ) BETWEEN " + "0 AND " + (this.mapperNum - 1)
        + " EACH 1 )";
    queryExecutor.executeUpdate(pIndxQuery);
  }

  public int populateImportTempTable() throws Exception {
    queryExecutor = new TDBQueryExecutor(dbConf);
    String insertQuery = getInsertQuery();
    int r = queryExecutor.executeUpdate(insertQuery);
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
    sb.append("random(0," + (this.mapperNum - 1) + ") as "
        + TeradataConstants.INDEX_COLUMN + " from " + this.tableName);
    LOG.debug("Insert query:" + sb.toString());
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
    sb.append(" FROM " + this.tempTableName + " WHERE "
        + TeradataConstants.INDEX_COLUMN + " = %N%");
    return sb.toString();
  }

}
