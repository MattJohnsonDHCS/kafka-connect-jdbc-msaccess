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

package io.confluent.connect.jdbc;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.util.Version;

/**
 * JdbcConnector is a Kafka Connect Connector implementation that watches a
 * file directory for MS Access DB Files and
 * generates tasks to ingest the database contents.
 */
public abstract class JdbcSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);

  private Map<String, String> configProperties;
  private JdbcSourceConnectorConfig config;
  private DatabaseDialect dialect;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) throws ConnectException {
    log.info("Starting JDBC Source Connector");
    try {
      configProperties = properties;
      if (configProperties.get(JdbcSourceConnectorConfig.ACCESS_DIRECTORY_PATH_CONFIG).isEmpty()) {
        throw new ConfigException("missing MS Access Directory path");
      }
      config = new JdbcSourceConnectorConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceConnector due to configuration error",
              e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return JdbcSourceTask.class;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    Config config = super.validate(connectorConfigs);
    JdbcSourceConnectorConfig jdbcSourceConnectorConfig
            = new JdbcSourceConnectorConfig(connectorConfigs);
    jdbcSourceConnectorConfig.validateMultiConfigs(config);
    return config;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>(1);
    Map<String, String> taskProps = new HashMap<>(configProperties);
    taskConfigs.add(taskProps);
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException {
  }

  @Override
  public ConfigDef config() {
    return JdbcSourceConnectorConfig.CONFIG_DEF;
  }
}
