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

package org.apache.flink.cdc.connectors.db2as400.source.fetch;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.EmbeddedFlinkDatabaseHistory;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.db2as400.source.config.Db2As400SourceConfig;
import org.apache.flink.cdc.connectors.db2as400.source.dialect.Db2As400Dialect;
import org.apache.flink.cdc.connectors.db2as400.source.handler.Db2As400SchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.db2as400.source.offset.JournalOffset;
import org.apache.flink.cdc.connectors.db2as400.source.utils.Db2As400Utils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.ChangeEventQueue.Builder;
import io.debezium.connector.db2as400.As400ConnectorConfig;
import io.debezium.connector.db2as400.As400DatabaseSchema;
import io.debezium.connector.db2as400.As400JdbcConnection;
import io.debezium.connector.db2as400.As400OffsetContext;
import io.debezium.connector.db2as400.As400OffsetContext.Loader;
import io.debezium.connector.db2as400.As400Partition;
import io.debezium.connector.db2as400.As400RpcConnection;
import io.debezium.connector.db2as400.As400RpcConnector;
import io.debezium.connector.db2as400.As400TaskContext;
import io.debezium.connector.db2as400.SourceInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** The context for fetch task that fetching data of snapshot split from Db2 data source. */
public class Db2As00SourceFetchTaskContext extends JdbcSourceFetchTaskContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(Db2As00SourceFetchTaskContext.class);

  /** Connection used for reading CDC tables. */
  private final As400RpcConnection connection;

  private final As400JdbcConnection metaDataConnection;

  private final As400EventMetadataProvider metadataProvider;
  private As400OffsetContext offsetContext;
  private As400Partition partition;
  private As400DatabaseSchema databaseSchema;
  private JdbcSourceEventDispatcher<As400Partition> dispatcher;
  private ErrorHandler errorHandler;
  private ChangeEventQueue<DataChangeEvent> queue;
  private As400TaskContext taskContext;
  private TopicNamingStrategy topicSelector;
  private EventDispatcher.SnapshotReceiver<As400Partition> snapshotReceiver;
  private SnapshotChangeEventSourceMetrics<As400Partition> snapshotChangeEventSourceMetrics;
  private StreamingChangeEventSourceMetrics<As400Partition> streamingChangeEventSourceMetrics;

  public Db2As00SourceFetchTaskContext(
      JdbcSourceConfig sourceConfig,
      Db2As400Dialect dataSourceDialect,
      As400RpcConnection connection,
      As400JdbcConnection metaDataConnection) {
    super(sourceConfig, dataSourceDialect);
    this.connection = connection;
    this.metadataProvider = new As400EventMetadataProvider();
    this.metaDataConnection = metaDataConnection;
  }

  @Override
  public void configure(SourceSplitBase sourceSplitBase) {
    // initial stateful objects
    final As400ConnectorConfig connectorConfig = getDbzConnectorConfig();
    this.topicSelector =
        connectorConfig.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);

    EmbeddedFlinkDatabaseHistory.registerHistory(
        sourceConfig
            .getDbzConfiguration()
            .getString(EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
        sourceSplitBase.getTableSchemas().values());

    this.databaseSchema = Db2As400Utils.createDb2DatabaseSchema(connectorConfig);
    this.offsetContext = loadStartingOffsetState(new Loader(connectorConfig), sourceSplitBase);

    String serverName = connectorConfig.getLogicalName();
    this.partition = new As400Partition(serverName);

    validateAndLoadDatabaseHistory(offsetContext, databaseSchema);

    this.taskContext = new As400TaskContext(connectorConfig, databaseSchema, new HashMap<>(3));

    final int queueSize =
        sourceSplitBase.isSnapshotSplit()
            ? getSourceConfig().getSplitSize()
            : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();
    this.queue =
        new Builder<DataChangeEvent>()
            .pollInterval(connectorConfig.getPollInterval())
            .maxBatchSize(connectorConfig.getMaxBatchSize())
            .maxQueueSize(queueSize)
            .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
            .loggingContextSupplier(
                () -> taskContext.configureLoggingContext("Db2As400-cdc-connector-task"))
            // do not buffer any element, we use signal event
            //                         .buffering()
            .build();
    this.dispatcher =
        new JdbcSourceEventDispatcher<>(
            connectorConfig,
            topicSelector,
            databaseSchema,
            queue,
            connectorConfig.getTableFilters().dataCollectionFilter(),
            DataChangeEvent::new,
            metadataProvider,
            schemaNameAdjuster,
            new Db2As400SchemaChangeEventHandler());

    this.snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();

    final DefaultChangeEventSourceMetricsFactory<As400Partition> changeEventSourceMetricsFactory =
        new DefaultChangeEventSourceMetricsFactory<>();
    this.snapshotChangeEventSourceMetrics =
        changeEventSourceMetricsFactory.getSnapshotMetrics(taskContext, queue, metadataProvider);
    this.streamingChangeEventSourceMetrics =
        changeEventSourceMetricsFactory.getStreamingMetrics(taskContext, queue, metadataProvider);
    this.errorHandler = new ErrorHandler(As400RpcConnector.class, connectorConfig, queue, null);
  }

  /** Loads the connector's persistent offset (if present) via the given loader. */
  private As400OffsetContext loadStartingOffsetState(
      As400OffsetContext.Loader loader, SourceSplitBase sourceSplitBase) {
    Offset offset =
        sourceSplitBase.isSnapshotSplit()
            ? JournalOffset.INITIAL_OFFSET
            : sourceSplitBase.asStreamSplit().getStartingOffset();

    return loader.load(offset.getOffset());
  }

  private void validateAndLoadDatabaseHistory(
      As400OffsetContext offset, As400DatabaseSchema schema) {
    if (schema.tableIds().isEmpty()) {
      LOGGER.info("tableIds of As400DatabaseSchema is empty start pre init");
      final String schemaName = getDbzConnectorConfig().getSchema();
      final Tables tables = new Tables();
      try {
        getMetaDataConnection()
            .readSchema(
                tables,
                getMetaDataConnection().getRealDatabaseName(),
                schemaName,
                getDbzConnectorConfig().getTableFilters().eligibleDataCollectionFilter(),
                null,
                false);
        for (TableId id : tables.tableIds()) {
          final Table t = tables.forTable(id);
          if (t != null) {
            databaseSchema.addSchema(t);
          }
        }
        LOGGER.info("The tableIds size now is {} after pre init", schema.tableIds().size());
      } catch (Exception e) {
        throw new FlinkRuntimeException("Failed to preload schema into As400DatabaseSchema", e);
      }
    }
  }

  @Override
  public ChangeEventQueue<DataChangeEvent> getQueue() {
    return queue;
  }

  @Override
  public TableFilter getTableFilter() {
    return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
  }

  @Override
  public Offset getStreamOffset(SourceRecord record) {
    return Db2As400Utils.getLsn(record);
  }

  @Override
  public As400DatabaseSchema getDatabaseSchema() {
    return databaseSchema;
  }

  @Override
  public RowType getSplitType(Table table) {
    Column splitColumn = Db2As400Utils.getSplitColumn(table, sourceConfig.getChunkKeyColumn());
    return Db2As400Utils.getSplitType(splitColumn);
  }

  @Override
  public ErrorHandler getErrorHandler() {
    return errorHandler;
  }

  @Override
  public As400ConnectorConfig getDbzConnectorConfig() {
    return (As400ConnectorConfig) super.getDbzConnectorConfig();
  }

  @Override
  public Db2As400SourceConfig getSourceConfig() {
    return (Db2As400SourceConfig) sourceConfig;
  }

  @Override
  public JdbcSourceEventDispatcher<As400Partition> getDispatcher() {
    return dispatcher;
  }

  public EventDispatcher.SnapshotReceiver<As400Partition> getSnapshotReceiver() {
    return snapshotReceiver;
  }

  @Override
  public As400OffsetContext getOffsetContext() {
    return offsetContext;
  }

  @Override
  public As400Partition getPartition() {
    return partition;
  }

  public As400RpcConnection getConnection() {
    return connection;
  }

  public As400JdbcConnection getMetaDataConnection() {
    return metaDataConnection;
  }

  public SnapshotChangeEventSourceMetrics<As400Partition> getSnapshotChangeEventSourceMetrics() {
    return snapshotChangeEventSourceMetrics;
  }

  public StreamingChangeEventSourceMetrics<As400Partition> getStreamingChangeEventSourceMetrics() {
    return streamingChangeEventSourceMetrics;
  }

  @Override
  public void close() throws Exception {
    metaDataConnection.commit();
    metaDataConnection.close();
    connection.close();
  }

  @Override
  public SchemaNameAdjuster getSchemaNameAdjuster() {
    return schemaNameAdjuster;
  }

  /** Copied from debezium for accessing here. */
  public class As400EventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(
        DataCollectionId source, OffsetContext offset, Object key, Struct value) {
      if (value == null) {
        return null;
      }
      Long timestamp = value.getInt64(SourceInfo.TIMESTAMP_KEY);

      if (timestamp == null) {
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
          return null;
        }
        timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
      }

      return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> getEventSourcePosition(
        DataCollectionId source, OffsetContext offset, Object key, Struct value) {
      Map<String, ?> map = offset.getOffset();
      return (Map<String, String>) map;
    }

    @Override
    public String getTransactionId(
        DataCollectionId source, OffsetContext offset, Object key, Struct value) {
      // TODO Auto-generated method stub
      if (offset.getTransactionContext() == null) {
        return null;
      }
      return offset.getTransactionContext().getTransactionId();
    }
  }
}
