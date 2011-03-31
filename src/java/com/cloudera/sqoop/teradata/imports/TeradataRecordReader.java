// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.imports;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBRecordReader;

public class TeradataRecordReader<LongWritable, T> extends
    DataDrivenDBRecordReader {

  /**
   * @param split
   * @param inputClass
   * @param conf
   * @param conn
   * @param dbConfig
   * @param cond
   * @param fields
   * @param table
   * @param dbProduct
   * @throws SQLException
   */
  public TeradataRecordReader(TeradataInputSplit split, Class inputClass,
      Configuration conf, Connection conn, DBConfiguration dbConfig,
      String cond, String[] fields, String table, String dbProduct)
      throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table,
        dbProduct);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * com.cloudera.sqoop.mapreduce.db.DataDrivenDBRecordReader#getSelectQuery()
   */
  @Override
  protected String getSelectQuery() {
    int partitionNum = getConf().getInt("mapred.task.partition", 0);
    String parametrizedQuery = getConf().get("parameterized.sql.query");
    String query = parametrizedQuery.replace("%N%", String
        .valueOf(partitionNum));
    return query;
  }

}
