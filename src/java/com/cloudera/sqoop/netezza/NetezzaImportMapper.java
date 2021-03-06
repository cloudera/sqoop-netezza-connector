// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza;

import com.cloudera.sqoop.netezza.util.NetezzaUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.io.NamedFifo;
import org.apache.sqoop.manager.MySQLUtils;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.util.TaskId;

import static com.cloudera.sqoop.netezza.util.NetezzaConstants.*;

/**
 * Mapper that performs a direct-mode import from Netezza.
 */
public class NetezzaImportMapper
    extends Mapper<Integer, NullWritable, String, Object> {

  public static final Log LOG =
      LogFactory.getLog(NetezzaImportMapper.class.getName());

  private Configuration conf;

  /** The FIFO being used to communicate with netezza. */
  private File fifoFile;

  /** The Reader we are using to read the fifo data. */
  private BufferedReader importReader;

  private class JdbcThread extends Thread {
    private SQLException sqlException;
    private Connection conn;
    private int sliceId;
    private Context context;

    public JdbcThread(int slice) {
      this.conn = null;
      this.sliceId = slice;
    }

    public SQLException getException() {
      return sqlException;
    }

    /**
     * Create the connection instance.
     */
    public void initConnection() throws SQLException {
      // Use JDBC to connect to the database.
      DBConfiguration dbConf = new DBConfiguration(conf);
      try {
        conn = dbConf.getConnection();
      } catch (ClassNotFoundException cnfe) {
        throw new SQLException(cnfe);
      }
      if (null == conn) {
        throw new SQLException("Could not connect to database");
      }

      LOG.debug("Opened database connection");
    }

    public void run() {
      PreparedStatement ps = null;
      LOG.debug("Starting JDBC comm thread.");

      try {
        int numMappers = ConfigurationHelper.getConfNumMaps(conf);
        DBConfiguration dbConf = new DBConfiguration(conf);
        StringBuilder sb = new StringBuilder();

        char fieldDelim = (char) conf.getInt(
            MySQLUtils.OUTPUT_FIELD_DELIM_KEY, (int) ',');
        char escape = (char) conf.getInt(
            MySQLUtils.OUTPUT_ESCAPED_BY_KEY, '\000');
        boolean ctrlChars =
          conf.getBoolean(DirectNetezzaManager.NZ_CTRLCHARS_CONF, false);

        sb.append("CREATE EXTERNAL TABLE '");
        sb.append(NetezzaImportMapper.this.fifoFile.getAbsolutePath());
        sb.append("' USING (REMOTESOURCE 'JDBC' ");
        sb.append("BOOLSTYLE 'T_F' ");
        sb.append("CRINSTRING FALSE ");
        sb.append("DELIMITER ");
        sb.append(Integer.toString(fieldDelim)); // Specified here in base 10.
        sb.append(" ENCODING 'internal' ");
        if (escape != '\000') {
          sb.append("ESCAPECHAR '\\' ");
        }
        sb.append("FORMAT 'text' ");
        sb.append("INCLUDEZEROSECONDS TRUE ");
        sb.append("NULLVALUE ? ");
        sb.append("CTRLCHARS ");
        sb.append(ctrlChars ? "true " : "false ");

        int maxErrors = conf.getInt(DirectNetezzaManager.NZ_MAXERRORS_CONF, 1);
        sb.append("MAXERRORS " + maxErrors + " ");
        String logDir = conf.get(DirectNetezzaManager.NZ_LOGDIR_CONF);
        if (logDir != null && logDir.trim().length() > 0) {
          sb.append("LOGDIR " + logDir + " ");
        }

        sb.append(") AS SELECT ");
        String [] fields = dbConf.getInputFieldNames();
        if (null == fields || fields.length == 0) {
          sb.append("* ");
        } else {
          boolean first = true;
          for (String f : fields) {
            if (!first) {
              sb.append(", ");
            }
            sb.append(f);
            first = false;
          }
        }
        sb.append(" FROM ");
        String schema = conf.get(DirectNetezzaManager.NETEZZA_SCHEMA_OPT);
        String tableName = dbConf.getInputTableName();
        if (schema != null) {
          tableName = schema + "." + tableName;
        }
        sb.append(tableName);
        sb.append(" WHERE MOD(DATASLICEID, " + numMappers);
        sb.append(") = " + sliceId);

        // If the user has specified a subset of rows to import,
        // or an incremental import, ensure that the appropriate conditions
        // are added here.
        String userWhereClause = dbConf.getInputConditions();
        if (null != userWhereClause) {
          sb.append(" AND ( ");
          sb.append(userWhereClause);
          sb.append(" ) ");
        }

        String sql = sb.toString();
        LOG.info("Executing SQL statement: " + sql);

        try {
          ps = conn.prepareStatement(sql);
          ps.setString(1, conf.get(PROPERTY_NULL_STRING, "null"));
          ps.execute();
        } finally {
          if (null != ps) {
            ps.close();
          }
        }
      } catch (SQLException sqlE) {
        // Save this exception for the parent thread to use to fail the task.
        LOG.error("Saving SQL exception from JDBC thread: "
            + StringUtils.stringifyException(sqlE));
        this.sqlException = sqlE;
      } finally {
        if (null != conn) {
          try {
            conn.close();
          } catch (SQLException sqlE) {
            // Exception closing the connection does not fail the task.
            LOG.error("Exception closing connection: " + sqlE);
          }
        }
        NetezzaUtil.uploadLogsToHdfsIfSpecified(conf, context.getTaskAttemptID().toString());
      }
    }
  }

  /** Thread which executes the SQL query to import over the FIFO. */
  private JdbcThread jdbcThread;

  /**
   * Create a named FIFO, and bind the JDBC connection to the FIFO.
   * A File object representing the FIFO is in 'fifoFile'.
   */
  private void initImportProcess(int slice, Context context) throws IOException {
    // Create the FIFO where we'll put the data.
    File taskAttemptDir = TaskId.getLocalWorkPath(conf);
    this.fifoFile = new File(taskAttemptDir, "netezza-" + slice + ".txt");

    NamedFifo nf = new NamedFifo(this.fifoFile);
    nf.create();

    // Start the JDBC thread which connects to the database
    // and opens the read side of the FIFO.
    this.jdbcThread = new JdbcThread(slice);
    this.jdbcThread.setDaemon(true);
    this.jdbcThread.context = context;
    try {
      this.jdbcThread.initConnection();
    } catch (SQLException sqlE) {
      throw new IOException(sqlE);
    }

    // Create log directory if specified
    NetezzaUtil.createLogDirectoryIfSpecified(conf);

    this.jdbcThread.start();

    // Open the read side of the FIFO.
    this.importReader = new BufferedReader(new InputStreamReader(
        new FileInputStream(nf.getFile())));
  }

  @Override
  public void map(Integer slice, NullWritable ignored, Context context)
      throws IOException, InterruptedException {

    // Configure and execute a direct-mode export.

    this.conf = context.getConfiguration();
    char recordDelimChar = (char) conf.getInt(
            MySQLUtils.OUTPUT_RECORD_DELIM_KEY, '\n');
    String recordDelim = "" + recordDelimChar;

    initImportProcess(slice, context);
    try {
      String line = this.importReader.readLine();
      while (null != line) {
        context.write(line.toString() + recordDelim, NullWritable.get());
        line = this.importReader.readLine();
      }
    } finally {
      try {
        this.importReader.close();
      } catch (IOException ioe) {
        LOG.warn("IOException during close: "
            + StringUtils.stringifyException(ioe));
      }

      this.jdbcThread.join();
      SQLException exception = this.jdbcThread.getException();
      if (null != exception) {
        throw new IOException(exception);
      }
    }
  }
}
