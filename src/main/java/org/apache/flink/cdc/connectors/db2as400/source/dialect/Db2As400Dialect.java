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

package org.apache.flink.cdc.connectors.db2as400.source.dialect;

import static org.apache.flink.cdc.connectors.db2as400.source.utils.Db2As400ConnectionUtils.createAs400RpcConnection;
import static org.apache.flink.cdc.connectors.db2as400.source.utils.Db2As400ConnectionUtils.createDb2As400Connection;
import static org.apache.flink.cdc.connectors.db2as400.source.utils.Db2As400Utils.currentLsn;

import io.debezium.connector.db2as400.As400JdbcConnection;
import io.debezium.connector.db2as400.As400RpcConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask.Context;
import org.apache.flink.cdc.connectors.db2as400.source.config.Db2As400SourceConfig;
import org.apache.flink.cdc.connectors.db2as400.source.fetch.Db2As00SourceFetchTaskContext;
import org.apache.flink.cdc.connectors.db2as400.source.fetch.Db2As400ScanFetchTask;
import org.apache.flink.cdc.connectors.db2as400.source.fetch.Db2As400StreamFetchTask;
import org.apache.flink.cdc.connectors.db2as400.source.utils.Db2As400ConnectionUtils;
import org.apache.flink.util.FlinkRuntimeException;

/** The {@link JdbcDataSourceDialect} implementation for Db2 datasource. */
@Experimental
public class Db2As400Dialect implements JdbcDataSourceDialect {

  private static final long serialVersionUID = 1L;
  private final Db2As400SourceConfig sourceConfig;
  private transient Db2As400Schema db2As400Schema;
  private transient Tables.TableFilter filters;

  public Db2As400Dialect(Db2As400SourceConfig sourceConfig) {
    this.sourceConfig = sourceConfig;
  }

  @Override
  public String getName() {
    return "Db2As400";
  }

  @Override
  public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
    try (As400RpcConnection rpcConnection =
        Db2As400ConnectionUtils.createAs400RpcConnection(sourceConfig.getDbzConnectorConfig())) {
      return currentLsn(rpcConnection);
    } catch (Exception e) {
      throw new FlinkRuntimeException("Read the redoLog offset error", e);
    }
  }

  @Override
  public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
    return true;
  }

  @Override
  public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
    return createDb2As400Connection(sourceConfig.getDbzConnectorConfig());
  }

  @Override
  public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
    return new Db2As400ChunkSplitter(sourceConfig, this);
  }

  @Override
  public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
    return new Db2As400PooledDataSourceFactory();
  }

  @Override
  public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
    Db2As400SourceConfig db2As400SourceConfig = (Db2As400SourceConfig) sourceConfig;
    try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
      return Db2As400ConnectionUtils.listTables(
          jdbcConnection, db2As400SourceConfig.getDbzConnectorConfig().getTableFilters());
    } catch (SQLException e) {
      throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
    }
  }

  @Override
  public Map<TableId, TableChange> discoverDataCollectionSchemas(JdbcSourceConfig sourceConfig) {
    final List<TableId> capturedTableIds = discoverDataCollections(sourceConfig);

    try (As400JdbcConnection jdbc =
        createDb2As400Connection(sourceConfig.getDbzConnectorConfig())) {
      // fetch table schemas
      Map<TableId, TableChange> tableSchemas = new HashMap<>();
      for (TableId tableId : capturedTableIds) {
        TableChange tableSchema = queryTableSchema(jdbc, tableId);
        tableSchemas.put(tableId, tableSchema);
      }
      return tableSchemas;
    } catch (Exception e) {
      throw new FlinkRuntimeException("Error to discover table schemas: " + e.getMessage(), e);
    }
  }

  @Override
  public TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
    if (db2As400Schema == null) {
      db2As400Schema = new Db2As400Schema();
    }
    return db2As400Schema.getTableSchema(
        jdbc,
        tableId,
        sourceConfig.getDbzConnectorConfig().getTableFilters().dataCollectionFilter());
  }

  @Override
  public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
    if (sourceSplitBase.isSnapshotSplit()) {
      return new Db2As400ScanFetchTask(sourceSplitBase.asSnapshotSplit());
    } else {
      return new Db2As400StreamFetchTask(sourceSplitBase.asStreamSplit());
    }
  }

  /**
   * The task context used for fetch task to fetch data from external systems.
   *
   * @param sourceConfig
   */
  @Override
  public Context createFetchTaskContext(JdbcSourceConfig sourceConfig) {
    final As400RpcConnection jdbcConnection =
        createAs400RpcConnection(sourceConfig.getDbzConnectorConfig());
    final As400JdbcConnection metaDataConnection =
        createDb2As400Connection(sourceConfig.getDbzConnectorConfig());
    return new Db2As00SourceFetchTaskContext(
        sourceConfig, this, jdbcConnection, metaDataConnection);
  }

  /**
   * Check if the tableId is included in SourceConfig.
   *
   * @param sourceConfig
   * @param tableId
   */
  @Override
  public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
    if (filters == null) {
      this.filters = sourceConfig.getTableFilters().dataCollectionFilter();
    }

    return filters.isIncluded(tableId);
  }
}
