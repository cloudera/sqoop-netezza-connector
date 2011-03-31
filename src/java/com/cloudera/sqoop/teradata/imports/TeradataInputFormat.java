// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.imports;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import com.cloudera.sqoop.teradata.util.TemporaryTableGenerator;

public class TeradataInputFormat<T extends DBWritable> extends
    DataDrivenDBInputFormat<T> {

  private static int numMappers;
  private static TemporaryTableGenerator tempTableGenerator;

  /*
   * (non-Javadoc)
   * 
   * @see com.cloudera.sqoop.mapreduce.db.DataDrivenDBInputFormat#
   * createDBRecordReader(com.cloudera.sqoop.mapreduce.db.DBInputFormat.
   * DBInputSplit, org.apache.hadoop.conf.Configuration)
   */
  @Override
  protected RecordReader<LongWritable, T> createDBRecordReader(
      DBInputSplit split, Configuration conf) throws IOException {
    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    String dbProductName = getDBProductName();
    TeradataRecordReader<LongWritable, T> teradataRecordReader = null;
    // Generic reader.
    try {
      teradataRecordReader = new TeradataRecordReader<LongWritable, T>(
          (TeradataInputSplit) split, inputClass, getConf(), getConnection(),
          dbConf, dbConf.getInputConditions(), dbConf.getInputFieldNames(),
          dbConf.getInputTableName(), dbProductName);
    } catch (SQLException se) {
      throw new IOException(se);
    }
    return teradataRecordReader;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.
   * mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < numMappers; i++) {
      splits.add(new TeradataInputSplit(i));
    }
    return splits;
  }

  /**
   * Creates a temporary table based on the input table, and creates a
   * parameterized query to be used by the record readers.
   * 
   * @param conf
   * @param tableName
   * @param intermediateTableName
   * @param sqlColNames
   * @throws IOException
   * @throws SQLException
   * @throws ClassNotFoundException 
   */
  public static void setInput(Job job, String tableName, int mappersNum,
      String[] colNames) throws IOException, SQLException,
      ClassNotFoundException {
    Configuration conf = job.getConfiguration();
    String intermediateTableName = getIntermediateTableName(tableName, conf);
    DBConfiguration dbConf = new DBConfiguration(conf);
    job.setInputFormatClass(TeradataInputFormat.class);
    numMappers = mappersNum;
    job.getConfiguration().set("input.table.name", tableName);
    job.getConfiguration().set("temporary.table.name", intermediateTableName);
    tempTableGenerator = new TemporaryTableGenerator(tableName,
        intermediateTableName, dbConf.getConnection(), colNames, numMappers);
    tempTableGenerator.createImportTempTable();
    tempTableGenerator.populateImportTempTable();
    // Generate a parameterized query for map tasks
    job.getConfiguration().set("parameterized.sql.query",
        tempTableGenerator.getParameterizedQuery());
  }

  /**
   * 
   * @param tableName
   *          the table name
   * @return
   */
  private static String getIntermediateTableName(String tableName, Configuration conf) {
    return tableName + conf.get("teradata.import.table_suffix", "_temp");
  }

}
