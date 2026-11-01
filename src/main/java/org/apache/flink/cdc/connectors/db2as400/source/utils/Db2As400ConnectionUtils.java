/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.db2as400.source.utils;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400ConnectorConfig;
import io.debezium.connector.db2as400.As400EventMetadataProvider;
import io.debezium.connector.db2as400.As400JdbcConnection;
import io.debezium.connector.db2as400.As400RpcConnection;
import io.debezium.connector.db2as400.As400TaskContext;
import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils for Db2As400 connection. */
public class Db2As400ConnectionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(Db2As400ConnectionUtils.class);

  public static As400JdbcConnection createDb2As400Connection(
      RelationalDatabaseConnectorConfig connectorConfig) {
    Configuration dbzConnectorConfig = connectorConfig.getJdbcConfig();
    return new As400JdbcConnection(JdbcConfiguration.adapt(dbzConnectorConfig));
  }

  public static As400RpcConnection createAs400RpcConnection(
      RelationalDatabaseConnectorConfig connectorConfig) {
    As400ConnectorConfig sourceConfig = (As400ConnectorConfig) connectorConfig;
    As400JdbcConnection jdbcConnection = createDb2As400Connection(sourceConfig);

    List<FileFilter> shortIncludes =
        jdbcConnection.shortIncludes(sourceConfig.getSchema(), sourceConfig.tableIncludeList());

    As400StreamingChangeEventSourceMetrics metrics =
        new As400StreamingChangeEventSourceMetrics(
            new As400TaskContext(
                sourceConfig,
                Db2As400Utils.createDb2DatabaseSchema(sourceConfig),
                new HashMap<>(3)),
            null,
            new As400EventMetadataProvider());

    return new As400RpcConnection(sourceConfig, metrics, shortIncludes);
  }

  public static List<TableId> listTables(JdbcConnection jdbc, RelationalTableFilters tableFilters)
      throws SQLException {
    final List<TableId> capturedTableIds = new ArrayList<>();
    Set<TableId> tableIdSet = new HashSet<>();
    String queryTablesSql =
        "SELECT TABLE_SCHEMA AS TABSCHEMA, TABLE_NAME AS TABNAME FROM QSYS2.SYSTABLES WHERE TABLE_TYPE = 'T'";
    As400JdbcConnection db2Connection = (As400JdbcConnection) jdbc;
    try {
      jdbc.query(
          queryTablesSql,
          rs -> {
            while (rs.next()) {
              String schemaName = rs.getString(1);
              String tableName = rs.getString(2);
              TableId tableId =
                  new TableId(db2Connection.getRealDatabaseName(), schemaName, tableName);
              tableIdSet.add(tableId);
            }
          });
    } catch (SQLException e) {
      LOG.warn(" SQL execute error, sql:{}", queryTablesSql, e);
    }

    for (TableId tableId : tableIdSet) {
      if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
        capturedTableIds.add(tableId);
        LOG.info("\t including '{}' for further processing", tableId);
      } else {
        LOG.debug("\t '{}' is filtered out of capturing", tableId);
      }
    }
    LOG.info("included '{}' after processing", capturedTableIds);
    return capturedTableIds;
  }
}
