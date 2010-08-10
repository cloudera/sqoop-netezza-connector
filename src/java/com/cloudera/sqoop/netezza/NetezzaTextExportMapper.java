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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Netezza exporter which accepts SqoopRecords (e.g., from
 * SequenceFiles) to emit to the database.
 */
public class NetezzaTextExportMapper
    extends NetezzaExportMapper<LongWritable, Text> {

  /**
   * Export the table to netezza.
   *
   * Expects one line of text (one record) as the value. Ignores the key.
   */
  @Override
  public void map(LongWritable key, Text val, Context context)
      throws IOException, InterruptedException {

    writeRecord(val);

    // We don't emit anything to the OutputCollector because we wrote
    // straight to the fifo. Send a progress indicator to prevent a timeout.
    context.progress();
  }
}
