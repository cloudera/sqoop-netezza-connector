// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.sqoop.mapreduce.DBWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.mapreduce.db.DBSplitter;
import org.apache.sqoop.mapreduce.db.DataDrivenDBInputFormat;
import org.apache.sqoop.config.ConfigurationHelper;

/**
 * An InputFormat that uses a netezza-specific partitioning strategy
 * for tables.
 */
public class NetezzaJdbcInputFormat<T extends DBWritable>
    extends DataDrivenDBInputFormat<T> implements Configurable {

  public static final Log LOG = LogFactory.getLog(
      NetezzaJdbcInputFormat.class.getName());

  private class DataSliceIdSplitter implements DBSplitter {
    @Override
    public List<InputSplit> split(Configuration conf, ResultSet rs,
        String splitByCol) {
      List<InputSplit> splits = new ArrayList<InputSplit>();
      // This strategy is very simple. Each table has a virtual column named
      // DATASLICEID specifying its locality. We want to have a split
      // correspond to all data on a single DATASLICEID. We just enumerate a
      // set of splits based purely on the number of tasks.
      int targetNumTasks = ConfigurationHelper.getConfNumMaps(conf);
      for (int i = 0; i < targetNumTasks; i++) {
        splits.add(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(
            "MOD(DATASLICEID, " + targetNumTasks + ") = " + i,
            "1=1"));
      }

      return splits;
    }
  }

  @Override
  protected RecordReader<LongWritable, T> createDBRecordReader(
      DBInputSplit split, Configuration conf) throws IOException {

    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    String dbProductName = getDBProductName();

    LOG.debug("Creating db record reader for db product: " + dbProductName);

    try {
      return new NetezzaRecordReader<T>(split, inputClass,
          conf, getConnection(), dbConf, dbConf.getInputConditions(),
          dbConf.getInputFieldNames(), dbConf.getInputTableName(),
          dbProductName);
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  /** {@inheritDoc} */
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    int targetNumTasks = ConfigurationHelper.getJobNumMaps(job);
    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    if (1 == targetNumTasks || dbConf.getInputTableName() == null) {
      // We're using either a singleton split, or we are partitioning a
      // free-form user query. Both of these are handled by the usual route in
      // DataDrivenDBInputFormat.
      return super.getSplits(job);
    }

    // Use the DATASLICEID-based splitter.
    return new DataSliceIdSplitter().split(job.getConfiguration(), null,
        null);
  }
}
