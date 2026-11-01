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

import static org.apache.flink.cdc.connectors.db2as400.source.utils.Db2As400Utils.buildSplitScanQuery;
import static org.apache.flink.cdc.connectors.db2as400.source.utils.Db2As400Utils.readTableSplitDataStatement;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400ConnectorConfig;
import io.debezium.connector.db2as400.As400DatabaseSchema;
import io.debezium.connector.db2as400.As400JdbcConnection;
import io.debezium.connector.db2as400.As400OffsetContext;
import io.debezium.connector.db2as400.As400Partition;
import io.debezium.connector.db2as400.As400SnapshotChangeEventSource;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.db2as400.source.fetch.Db2As400StreamFetchTask.StreamSplitReadTask;
import org.apache.flink.cdc.connectors.db2as400.source.utils.Db2As400ConnectionUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The task to work for fetching data of Db2 table snapshot split. */
public class Db2As400ScanFetchTask extends AbstractScanFetchTask {

  public Db2As400ScanFetchTask(SnapshotSplit split) {
    super(split);
  }

  @Override
  protected void executeDataSnapshot(Context context) throws Exception {
    Db2As00SourceFetchTaskContext sourceFetchContext = (Db2As00SourceFetchTaskContext) context;
    Db2SnapshotSplitReadTask snapshotSplitReadTask =
        new Db2SnapshotSplitReadTask(
            sourceFetchContext.getDbzConnectorConfig(),
            sourceFetchContext.getOffsetContext(),
            sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
            sourceFetchContext.getDatabaseSchema(),
            sourceFetchContext.getMetaDataConnection(),
            sourceFetchContext.getDispatcher(),
            sourceFetchContext.getSnapshotReceiver(),
            snapshotSplit,
            sourceFetchContext);
    Db2SnapshotSplitChangeEventSourceContext changeEventSourceContext =
        new Db2SnapshotSplitChangeEventSourceContext();
    SnapshotResult<As400OffsetContext> snapshotResult =
        snapshotSplitReadTask.execute(
            changeEventSourceContext,
            sourceFetchContext.getPartition(),
            sourceFetchContext.getOffsetContext());
    // execute stream read task
    if (!snapshotResult.isCompletedOrSkipped()) {
      taskRunning = false;
      throw new IllegalStateException(
          String.format("Read snapshot for As400 split %s fail", snapshotSplit));
    }
  }

  @Override
  protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
      throws Exception {
    Db2As00SourceFetchTaskContext sourceFetchContext = (Db2As00SourceFetchTaskContext) context;
    final As400OffsetContext.Loader loader =
        new As400OffsetContext.Loader(sourceFetchContext.getDbzConnectorConfig());
    final As400OffsetContext streamOffsetContext =
        loader.load(backfillStreamSplit.getStartingOffset().getOffset());

    final StreamSplitReadTask backfillBinlogReadTask =
        createBackFillLsnSplitReadTask(backfillStreamSplit, sourceFetchContext);
    backfillBinlogReadTask.execute(
        new Db2SnapshotSplitChangeEventSourceContext(),
        sourceFetchContext.getPartition(),
        streamOffsetContext);
  }

  private StreamSplitReadTask createBackFillLsnSplitReadTask(
      StreamSplit backfillBinlogSplit, Db2As00SourceFetchTaskContext context) {
    // we should only capture events for the current table,
    // otherwise, we may can't find corresponding schema
    Configuration dezConf =
        context
            .getDbzConnectorConfig()
            .getConfig()
            .edit()
            // table.include.list is schema.table format
            .with(
                "table.include.list",
                new TableId(
                    null, snapshotSplit.getTableId().schema(), snapshotSplit.getTableId().table()))
            // Disable heartbeat event in snapshot split fetcher
            .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
            .build();
    // task to read wal and backfill for current split
    return new StreamSplitReadTask(
        new As400ConnectorConfig(dezConf),
        context.getConnection(),
        context.getMetaDataConnection(),
        context.getDispatcher(),
        context.getErrorHandler(),
        context.getDatabaseSchema(),
        backfillBinlogSplit);
  }

  /** A wrapped task to fetch snapshot split of table. */
  public static class Db2SnapshotSplitReadTask extends As400SnapshotChangeEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(Db2SnapshotSplitReadTask.class);

    /** Interval for showing a log statement with the progress while scanning a single table. */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final As400ConnectorConfig connectorConfig;
    private final As400DatabaseSchema databaseSchema;
    private final As400JdbcConnection jdbcConnection;
    private final JdbcSourceEventDispatcher<As400Partition> dispatcher;
    private final Clock clock;
    private final SnapshotSplit snapshotSplit;
    private final As400OffsetContext offsetContext;
    private final SnapshotProgressListener<As400Partition> snapshotProgressListener;
    private final EventDispatcher.SnapshotReceiver<As400Partition> snapshotReceiver;

    public Db2SnapshotSplitReadTask(
        As400ConnectorConfig connectorConfig,
        As400OffsetContext previousOffset,
        SnapshotProgressListener<As400Partition> snapshotProgressListener,
        As400DatabaseSchema databaseSchema,
        As400JdbcConnection jdbcConnection,
        JdbcSourceEventDispatcher<As400Partition> dispatcher,
        EventDispatcher.SnapshotReceiver<As400Partition> snapshotReceiver,
        SnapshotSplit snapshotSplit,
        Db2As00SourceFetchTaskContext sourceFetchTaskContext) {
      super(
          connectorConfig,
          Db2As400ConnectionUtils.createAs400RpcConnection(connectorConfig),
          new DefaultMainConnectionProvidingConnectionFactory<>(
              () -> new As400JdbcConnection(connectorConfig.getJdbcConfig())),
          sourceFetchTaskContext.getDatabaseSchema(),
          sourceFetchTaskContext.getDispatcher(),
          Clock.SYSTEM,
          snapshotProgressListener,
          null,
          null);
      this.offsetContext = previousOffset;
      this.connectorConfig = connectorConfig;
      this.databaseSchema = databaseSchema;
      this.jdbcConnection = jdbcConnection;
      this.dispatcher = dispatcher;
      this.clock = Clock.SYSTEM;
      this.snapshotSplit = snapshotSplit;
      this.snapshotProgressListener = snapshotProgressListener;
      this.snapshotReceiver = snapshotReceiver;
    }

    public SnapshotResult<As400OffsetContext> execute(
        ChangeEventSourceContext context,
        As400Partition partition,
        As400OffsetContext previousOffset)
        throws InterruptedException {
      SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
      final SnapshotContext<As400Partition, As400OffsetContext> ctx;
      try {
        ctx = prepare(partition, false);
      } catch (Exception e) {
        LOG.error("Failed to initialize snapshot context.", e);
        throw new RuntimeException(e);
      }
      try {
        return doExecute(context, previousOffset, ctx, snapshottingTask);
      } catch (InterruptedException e) {
        LOG.warn("Snapshot was interrupted before completion");
        throw e;
      } catch (Exception t) {
        throw new DebeziumException(t);
      }
    }

    @Override
    public SnapshotResult<As400OffsetContext> doExecute(
        ChangeEventSourceContext context,
        As400OffsetContext previousOffset,
        SnapshotContext<As400Partition, As400OffsetContext> snapshotContext,
        SnapshottingTask snapshottingTask)
        throws Exception {
      snapshotContext.offset = offsetContext;

      createDataEvents(snapshotContext, snapshotSplit.getTableId());

      return SnapshotResult.completed(snapshotContext.offset);
    }

    private void createDataEvents(
        SnapshotContext<As400Partition, As400OffsetContext> snapshotContext, TableId tableId)
        throws Exception {
      LOG.info("Snapshotting table {}", tableId);
      createDataEventsForTable(snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
      snapshotReceiver.completeSnapshot();
    }

    /** Dispatches the data change events for the records of a single table. */
    private void createDataEventsForTable(
        SnapshotContext<As400Partition, As400OffsetContext> snapshotContext,
        EventDispatcher.SnapshotReceiver<As400Partition> snapshotReceiver,
        Table table)
        throws InterruptedException {

      long exportStart = clock.currentTimeInMillis();
      LOG.info("Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

      final String selectSql =
          buildSplitScanQuery(
              snapshotSplit.getTableId(),
              snapshotSplit.getSplitKeyType(),
              snapshotSplit.getSplitStart() == null,
              snapshotSplit.getSplitEnd() == null);
      LOG.info(
          "For split '{}' of table {} using select statement: '{}'",
          snapshotSplit.splitId(),
          table.id(),
          selectSql);

      try (PreparedStatement selectStatement =
              readTableSplitDataStatement(
                  jdbcConnection,
                  selectSql,
                  snapshotSplit.getSplitStart() == null,
                  snapshotSplit.getSplitEnd() == null,
                  snapshotSplit.getSplitStart(),
                  snapshotSplit.getSplitEnd(),
                  snapshotSplit.getSplitKeyType().getFieldCount(),
                  connectorConfig.getQueryFetchSize());
          ResultSet rs = selectStatement.executeQuery()) {

        ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
        long rows = 0;
        Threads.Timer logTimer = getTableScanLogTimer();

        while (rs.next()) {
          rows++;
          final Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);
          if (logTimer.expired()) {
            long stop = clock.currentTimeInMillis();
            LOG.info(
                "Exported {} records for split '{}' after {}",
                rows,
                snapshotSplit.splitId(),
                Strings.duration(stop - exportStart));
            snapshotProgressListener.rowsScanned(snapshotContext.partition, table.id(), rows);
            logTimer = getTableScanLogTimer();
          }
          dispatcher.dispatchSnapshotEvent(
              snapshotContext.partition,
              table.id(),
              getChangeRecordEmitter(snapshotContext, table.id(), row),
              snapshotReceiver);
        }
        LOG.info(
            "Finished exporting {} records for split '{}', total duration '{}'",
            rows,
            snapshotSplit.splitId(),
            Strings.duration(clock.currentTimeInMillis() - exportStart));
      } catch (SQLException e) {
        throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
      }
    }

    protected ChangeRecordEmitter<As400Partition> getChangeRecordEmitter(
        SnapshotContext<As400Partition, As400OffsetContext> snapshotContext,
        TableId tableId,
        Object[] row) {
      snapshotContext.offset.event(tableId, clock.currentTime());
      return new SnapshotChangeRecordEmitter<>(
          snapshotContext.partition, snapshotContext.offset, row, clock, connectorConfig);
    }

    private Threads.Timer getTableScanLogTimer() {
      return Threads.timer(clock, LOG_INTERVAL);
    }

    protected static class Db2SnapshotContext
        extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
            As400Partition, As400OffsetContext> {

      public Db2SnapshotContext(As400Partition partition, String catalogName, boolean onDemand)
          throws SQLException {
        super(partition, catalogName, onDemand);
      }
    }
  }

  /**
   * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded stream task
   * of a snapshot split task.
   */
  public class Db2SnapshotSplitChangeEventSourceContext
      implements ChangeEventSource.ChangeEventSourceContext {

    public void finished() {
      taskRunning = false;
    }

    @Override
    public boolean isPaused() {
      return false;
    }

    @Override
    public boolean isRunning() {
      return taskRunning;
    }

    @Override
    public void resumeStreaming() throws InterruptedException {}

    @Override
    public void waitSnapshotCompletion() throws InterruptedException {}

    @Override
    public void streamingPaused() {}

    @Override
    public void waitStreamingPaused() throws InterruptedException {}
  }
}
