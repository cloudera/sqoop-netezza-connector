/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
import org.apache.sqoop.mapreduce.DBWritable;

import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import org.apache.sqoop.mapreduce.db.DataDrivenDBRecordReader;

/**
 * A RecordReader that reads records from Netezza.
 */
public class NetezzaRecordReader<T extends DBWritable>
    extends DataDrivenDBRecordReader<T> {

  private static final Log LOG = LogFactory.getLog(NetezzaRecordReader.class);

  public NetezzaRecordReader(DBInputFormat.DBInputSplit split,
                             Class<T> inputClass, Configuration conf, Connection conn,
                             DBConfiguration dbConfig, String cond, String [] fields, String table,
                             String dbProduct) throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table, dbProduct);
  }

  /** Returns the query for selecting the records from Netezza. */
  protected String getSelectQuery() {
    String query = super.getSelectQuery();
    if (getDBConf().getInputQuery() == null) {
      // If there is no user-defined query, we construct a default select query
      // as follows:
      //  SELECT <columns> FROM <table name> AS <table name>
      // However, in Netezza 'AS <table name>' can cause a syntax error if table
      // name is a qualified name. Since the AS clause is not necessary, we
      // remove it.
      query = query.replace(" AS " + getTableName(), "");
    }
    return query;
  }
}
