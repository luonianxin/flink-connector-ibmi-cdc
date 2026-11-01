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

import io.debezium.connector.db2as400.As400ConnectorConfig;
import io.debezium.connector.db2as400.As400DatabaseSchema;
import io.debezium.connector.db2as400.As400JdbcConnection;
import io.debezium.connector.db2as400.As400OffsetContext;
import io.debezium.connector.db2as400.As400Partition;
import io.debezium.connector.db2as400.As400RpcConnection;
import io.debezium.connector.db2as400.As400StreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.util.Clock;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The task to work for fetching data of Db2 table stream split . */
public class Db2As400StreamFetchTask implements FetchTask<SourceSplitBase> {

  private final StreamSplit split;
  private volatile boolean taskRunning = false;

  public Db2As400StreamFetchTask(StreamSplit split) {
    this.split = split;
  }

  @Override
  public void execute(Context context) throws Exception {
    Db2As00SourceFetchTaskContext sourceFetchContext = (Db2As00SourceFetchTaskContext) context;
    sourceFetchContext.getOffsetContext().preSnapshotCompletion();
    taskRunning = true;
    StreamSplitReadTask streamSplitReadTask =
        new StreamSplitReadTask(
            sourceFetchContext.getDbzConnectorConfig(),
            sourceFetchContext.getConnection(),
            sourceFetchContext.getMetaDataConnection(),
            sourceFetchContext.getDispatcher(),
            sourceFetchContext.getErrorHandler(),
            sourceFetchContext.getDatabaseSchema(),
            split);
    RedoLogSplitChangeEventSourceContext changeEventSourceContext =
        new RedoLogSplitChangeEventSourceContext();

    streamSplitReadTask.execute(
        changeEventSourceContext,
        sourceFetchContext.getPartition(),
        sourceFetchContext.getOffsetContext());
  }

  @Override
  public boolean isRunning() {
    return taskRunning;
  }

  @Override
  public StreamSplit getSplit() {
    return split;
  }

  @Override
  public void close() {
    taskRunning = false;
  }

  /**
   * A wrapped task to read all redo logs for table and also supports read bounded (from
   * lowWatermark to highWatermark) redo logs.
   */
  public static class StreamSplitReadTask extends As400StreamingChangeEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(StreamSplitReadTask.class);
    private final StreamSplit lsnSplit;
    private final JdbcSourceEventDispatcher<As400Partition> dispatcher;
    private final ErrorHandler errorHandler;
    private ChangeEventSourceContext context;

    public StreamSplitReadTask(
        As400ConnectorConfig connectorConfig,
        As400RpcConnection connection,
        As400JdbcConnection metadataConnection,
        JdbcSourceEventDispatcher<As400Partition> dispatcher,
        ErrorHandler errorHandler,
        As400DatabaseSchema schema,
        StreamSplit lsnSplit) {
      super(
          connectorConfig,
          connection,
          metadataConnection,
          dispatcher,
          errorHandler,
          Clock.system(),
          schema);
      this.lsnSplit = lsnSplit;
      this.dispatcher = dispatcher;
      this.errorHandler = errorHandler;
    }

    @Override
    public void execute(
        ChangeEventSourceContext context,
        As400Partition partition,
        As400OffsetContext offsetContext)
        throws InterruptedException {
      this.context = context;
      super.execute(context, partition, offsetContext);
    }
  }

  /** The {@link ChangeEventSourceContext} implementation for redo logs split task. */
  private class RedoLogSplitChangeEventSourceContext implements ChangeEventSourceContext {
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
