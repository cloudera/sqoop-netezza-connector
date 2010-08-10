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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.util.ExportException;

/**
 * Uses remote external tables to import/export bulk data to/from Netezza.
 */
public class DirectNetezzaManager extends NetezzaManager {

  public static final Log LOG = LogFactory.getLog(
      DirectNetezzaManager.class.getName());

  public DirectNetezzaManager(final SqoopOptions opts) {
    super(opts);
  }

  @Override
  /**
   *  Export the table from HDFS to NZ by using remote external
   *  tables to insert the data back into the database.
   */
  public void exportTable(ExportJobContext context)
      throws IOException, ExportException {
    context.setConnManager(this);
    NetezzaExportJob exportJob = new NetezzaExportJob(context);
    exportJob.runExport();
  }
}

