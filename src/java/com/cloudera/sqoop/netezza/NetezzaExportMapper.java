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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.sqoop.io.NamedFifo;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.ExportJobBase;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.util.TaskId;

/**
 * Mapper that writes to a named FIFO which will be used to export rows
 * from HDFS to Netezza at high speed.
 *
 * map() methods are actually provided by subclasses that read from
 * SequenceFiles (containing existing SqoopRecords) or text files
 * (containing delimited lines) and deliver these results to the fifo.
 */
public class NetezzaExportMapper<KEYIN, VALIN>
    extends Mapper<KEYIN, VALIN, NullWritable, NullWritable> {

  public static final Log LOG = LogFactory.getLog(
      NetezzaExportMapper.class.getName());

  private Configuration conf;

  /** The FIFO being used to communicate with netezza. */
  private File fifoFile;

  /** The JDBC connection to Netezza. */
  private Connection conn;

  /** The OutputStream we are using to write the fifo data. */
  private OutputStream exportStream;
  
  /** Object that holds/parses a record of the user's input. */
  private SqoopRecord inputRecord;

  /** Delimiters to use for Netezza */
  private DelimiterSet outputDelimiters;

  /**
   * Create a named FIFO, and bind the JDBC connection to the FIFO.
   * A File object representing the FIFO is in 'fifoFile'.
   */
  private void initImportProcess() throws IOException {
    // Create the FIFO where we'll put the data.
    File taskAttemptDir = TaskId.getLocalWorkPath(conf);
    this.fifoFile = new File(taskAttemptDir, "netezza.txt");

    NamedFifo nf = new NamedFifo(this.fifoFile);
    nf.create();

    this.exportStream = new FileOutputStream(nf.getFile());

    // Use JDBC to connect to the database.
    DBConfiguration dbConf = new DBConfiguration(conf);
    try {
      this.conn = dbConf.getConnection();
    } catch (Exception e) {
      throw new IOException(e);
    }
    if (null == conn) {
      throw new IOException("Could not connect to database");
    }

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(dbConf.getInputTableName());
    sb.append(" SELECT * FROM EXTERNAL '");
    sb.append(this.fifoFile.getAbsolutePath());
    sb.append("' USING (REMOTESOURCE 'JDBC' ");
    sb.append("BOOLSTYLE 'TRUE_FALSE' ");
    sb.append("CRINSTRING FALSE ");
    sb.append("DELIMITER ',' ");
    sb.append("ENCODING 'internal' ");
    sb.append("ESCAPECHAR '\\' ");
    sb.append("FORMAT 'text' ");
    sb.append("INCLUDEZEROSECONDS TRUE ");
    sb.append("NULLVALUE 'null' ");
    sb.append(")");

    PreparedStatement ps = null;
    try {
      ps = conn.prepareStatement(sb.toString());
      ps.executeUpdate();
    } catch (SQLException sqlE) {
      throw new IOException(sqlE);
    } finally {
      if (null != ps) {
        try {
          ps.close();
        } catch (SQLException sqlE) {
          LOG.warn("Error closing statement: " + sqlE);
        }
      }
      ps = null;
    }
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    initImportProcess();
    try {
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
      cleanup(context);
    } finally {
      // Shut down the export process.
      try {
        closeHandles();
      } catch (SQLException sqlE) {
        throw new IOException(sqlE);
      }
    }
  }

  private void closeHandles() throws SQLException {
    // Try to close the FIFO handle. An exception here does not cause task
    // failure.
    if (null != this.exportStream) {
      try {
        this.exportStream.close();
      } catch (IOException ioe) {
        LOG.warn("Error closing FIFO stream: " + ioe);
      } finally {
        this.exportStream = null;
      }
    }

    // Attempt to commit the transaction. If this throws an exception, we
    // propagate it up. In either case, close the connection. An exception
    // closing the connection does not fail the task.
    if (null != this.conn) {
      try {
        this.conn.commit();
      } finally {
        try {
          this.conn.close();
        } catch (SQLException sqlE) {
          LOG.error("Exception closing connection: " + sqlE);
        } finally {
          this.conn = null;
        }
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException {
    this.conf = context.getConfiguration();
    Class<? extends SqoopRecord> recordClass = (Class<? extends SqoopRecord>)
        this.conf.getClass(ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY, null);
    if (null != recordClass) {
      // Try to instantiate the user's record class.
      this.inputRecord = ReflectionUtils.newInstance(recordClass, conf);
    }

    this.outputDelimiters = new DelimiterSet(',', '\n', (char) 0, '\\', false);
  }

  /**
   * Takes a delimited text record (e.g., the output of a 'Text' object),
   * re-encodes it for consumption by mysqlimport, and writes it to the pipe.
   * @param record A delimited text representation of one record.
   */
  protected void writeRecord(Text record) throws IOException {

    try {
      inputRecord.parse(record);
    } catch (RecordParser.ParseError e) {
      throw new IOException(e);
    }

    writeRecord(inputRecord);
  }

  protected void writeRecord(SqoopRecord r) throws IOException {
    String outputStr = r.toString(outputDelimiters);
    byte [] outputBytes = outputStr.getBytes("UTF-8");
    this.exportStream.write(outputBytes, 0, outputBytes.length);
  }
}

