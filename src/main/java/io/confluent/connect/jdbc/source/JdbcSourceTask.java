/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import java.io.File;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLNonTransientException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.Version;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TransactionIsolationMode;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class JdbcSourceTask extends SourceTask {
  // When no results, periodically return control flow to caller to give it a chance to pause us.
  private static final int CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN = 3;

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  private Time time;
  private JdbcSourceTaskConfig config;
  private DatabaseDialect dialect;
  //Visible for Testing
  Connection connection;
  List<TableQuerier> tableQuerierList = new ArrayList<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicLong taskThreadId = new AtomicLong(0);

  private static final Pattern ext = Pattern.compile("(?<=.)\\.[^.]+$");

  int maxRetriesPerQuerier;

  public JdbcSourceTask() {
    this.time = new SystemTime();
  }

  public JdbcSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    log.info("Starting JDBC source task");
    try {
      config = new JdbcSourceTaskConfig(properties);
    } catch (ConfigException e) {
      throw new ConfigException("Couldn't start JdbcSourceTask due to configuration error", e);
    }
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping JDBC source task");

    // In earlier versions of Kafka, stop() was not called from the task thread. In this case, all
    // resources are closed at the end of 'poll()' when no longer running or if there is an error.

    if (taskThreadId.longValue() == Thread.currentThread().getId()) {
      shutdown();
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.trace("Polling for new data");
    final List<SourceRecord> results = new ArrayList<>();

    File directory =
            new File(config.getString(JdbcSourceConnectorConfig.ACCESS_DIRECTORY_PATH_CONFIG));
    File[] accessFiles = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".accdb"));

    for (File file : accessFiles) {

      List<String> tableList = new ArrayList<>();

      Connection connection;

      try {
        Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      try {
        connection = DriverManager.getConnection("jdbc:ucanaccess://" + file.getAbsolutePath());
        DatabaseMetaData md = connection.getMetaData();
        ResultSet rs = md.getTables(null, null, "%", null);
        while (rs.next()) {
          tableList.add(rs.getString(3));
        }

        final String dialectName = config.getString(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG);
        if (dialectName != null && !dialectName.trim().isEmpty()) {
          dialect = DatabaseDialects.create(dialectName, config);
        } else {
          DatabaseDialects.create("GenericDatabaseDialect", config);
        }
        log.info("Using JDBC dialect {}", dialect.name());

        dialect.setConnectionIsolationMode(
                connection,
                TransactionIsolationMode
                        .valueOf(
                                config.getString(
                                        JdbcSourceConnectorConfig
                                                .TRANSACTION_ISOLATION_MODE_CONFIG
                                )
                        )
        );

      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      for (String table : tableList) {
        tableQuerierList.add(
                new BulkTableQuerier(
                        dialect,
                        TableQuerier.QueryMode.TABLE,
                        table,
                        config.getString(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG)
                                + "_" + getFileNameWithoutExtension(file),
                        ""
                )
        );

      }

      for (TableQuerier querier : tableQuerierList) {

        if (!querier.querying()) {
          // If not in the middle of an update, wait for next update time
          final long nextUpdate = querier.getLastUpdate()
                  + config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
          final long now = time.milliseconds();
          final long sleepMs = Math.min(nextUpdate - now, 100);

          if (sleepMs > 0) {
            log.trace("Waiting {} ms to poll {} next", nextUpdate - now, querier.toString());
            time.sleep(sleepMs);
            continue; // Re-check stop flag before continuing
          }
        }

        try {
          log.debug("Checking for next block of results from {}", querier.toString());
          querier.maybeStartQuery(connection);
          results.add(querier.extractRecord());
        } catch (SQLNonTransientException sqle) {
          log.error("Non-transient SQL exception while running query "
                         + "for table: {} from database: {}",
                  querier, getFileNameWithoutExtension(file), sqle);
          closeResources(connection, getFileNameWithoutExtension(file));
          throw new ConnectException(sqle);
        } catch (SQLException sqle) {
          log.error(
                  "SQL exception while running query for table: {}, from db file {}.",
                  querier,
                  getFileNameWithoutExtension(file),
                  sqle
          );
          return null;
        } catch (Throwable t) {
          log.error("Failed to run query for table: {}", querier, t);
          closeResources(connection, getFileNameWithoutExtension(file));
          throw t;
        }
      }
    }
    closeAllResources();
    log.debug("Returning {} records", results.size());
    return results;
  }

  protected void closeResources(Connection connection, String fileName) {
    log.info("Closing db connection for file {}", fileName);
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (Throwable t) {
      log.warn("Error while closing the connection", t);
    }
  }

  protected void closeAllResources() {
    log.info("Closing db connection");
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (Throwable t) {
      log.warn("Error while closing the connection", t);
    }
  }

  private void shutdown() {
    closeAllResources();
  }

  public static String getFileNameWithoutExtension(File file) {
    return ext.matcher(file.getName()).replaceAll("");
  }
}
