package org.apache.flink.cdc.connectors.db2as400.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.connectors.db2as400.source.Db2As400SourceBuilder.Db2As400IncrementalSource;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

/** IT tests for {@link Db2As400IncrementalSource}. */
public class Db2As400SourceITCase {

  @Rule public final Timeout timeoutPerTest = Timeout.seconds(600);

  @Rule
  public final MiniClusterWithClientResource miniClusterResource =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
              .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
              .withHaLeadershipControl()
              .build());

  protected static final int DEFAULT_PARALLELISM = 4;

  @Before
  public void skipWithoutExternalEnvironment() {
    assumeExternalEnvironmentConfigured();
  }

  private static final String IBM_I_HOST =
      System.getProperty("ibmi.test.host", "<ibmi-host>");
  private static final String IBM_I_PORT = System.getProperty("ibmi.test.port", "50000");
  private static final String IBM_I_USER =
      System.getProperty("ibmi.test.user", "<ibmi-user>");
  private static final String IBM_I_PASSWORD =
      System.getProperty("ibmi.test.password", "<ibmi-password>");
  private static final String IBM_I_DATABASE =
      System.getProperty("ibmi.test.database", "<ibmi-database>");
  private static final String IBM_I_TABLE =
      System.getProperty("ibmi.test.table", "<ibmi-schema>.<ibmi-table>");
  private static final String JDBC_URL =
      System.getProperty("ibmi.test.jdbc-url", "jdbc:as400://<ibmi-host>");

  @Test
  public void testReadSingleTableWithSingleParallelism() throws Exception {
    testDb2ParallelSource(
        1, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"DB2INST1.CUSTOMERS"});
  }

  @Test
  public void testReadSingleTableWithMultipleParallelism() throws Exception {
    testDb2ParallelSource(
        4, FailoverType.NONE, FailoverPhase.NEVER, new String[] {"DB2INST1.CUSTOMERS"});
  }

  // Failover tests
  @Test
  public void testTaskManagerFailoverInSnapshotPhase() throws Exception {
    testDb2ParallelSource(
        FailoverType.TM, FailoverPhase.SNAPSHOT, new String[] {"DB2INST1.CUSTOMERS"});
  }

  @Test
  public void testTaskManagerFailoverInRedoLogsPhase() throws Exception {
    testDb2ParallelSource(
        FailoverType.TM, FailoverPhase.STREAM, new String[] {"DB2INST1.CUSTOMERS"});
  }

  @Test
  public void testJobManagerFailoverInSnapshotPhase() throws Exception {
    testDb2ParallelSource(
        FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"DB2INST1.CUSTOMERS"});
  }

  @Test
  public void testJobManagerFailoverInRedoLogsPhase() throws Exception {
    testDb2ParallelSource(
        FailoverType.JM, FailoverPhase.STREAM, new String[] {"DB2INST1.CUSTOMERS"});
  }

  @Test
  public void testJobManagerFailoverSingleParallelism() throws Exception {
    testDb2ParallelSource(
        1, FailoverType.JM, FailoverPhase.SNAPSHOT, new String[] {"DB2INST1.CUSTOMERS"});
  }

  @Test
  public void testReadSingleTableWithSingleParallelismAndSkipBackfill() throws Exception {
    testDb2ParallelSource(
        DEFAULT_PARALLELISM,
        FailoverType.TM,
        FailoverPhase.SNAPSHOT,
        new String[] {"DB2INST1.CUSTOMERS"},
        true);
  }

  private void testDb2ParallelSource(
      FailoverType failoverType, FailoverPhase failoverPhase, String[] captureCustomerTables)
      throws Exception {
    testDb2ParallelSource(DEFAULT_PARALLELISM, failoverType, failoverPhase, captureCustomerTables);
  }

  private void testDb2ParallelSource(
      int parallelism,
      FailoverType failoverType,
      FailoverPhase failoverPhase,
      String[] captureCustomerTables)
      throws Exception {
    testDb2ParallelSource(parallelism, failoverType, failoverPhase, captureCustomerTables, false);
  }

  private void testDb2ParallelSource(
      int parallelism,
      FailoverType failoverType,
      FailoverPhase failoverPhase,
      String[] captureCustomerTables,
      boolean skipSnapshotBackfill)
      throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    env.setParallelism(parallelism);
    env.enableCheckpointing(30000L);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

    String sourceDDL =
        format(
            "CREATE TABLE CUSTOMERS ("
                + " ID INT NOT NULL,"
                + " NAME STRING,"
                + " AGE INT,"
                + " UPDATE_TIME TIMESTAMP(3),"
                + " primary key (ID) not enforced"
                + ") WITH ("
                + " 'connector' = 'ibmi-cdc',"
                + " 'hostname' = '%s',"
                + " 'port' = '%s',"
                + " 'username' = '%s',"
                + " 'password' = '%s',"
                + " 'database-name' = '%s',"
                + " 'table-name' = '%s',"
                + " 'scan.incremental.snapshot.enabled' = 'true',"
                + " 'scan.incremental.snapshot.chunk.size' = '4',"
                + " 'scan.incremental.snapshot.backfill.skip' = '%s',"
                + "'debezium.topic.prefix' = 'db2as400_server'"
                + ")",
            IBM_I_HOST,
            IBM_I_PORT,
            IBM_I_USER,
            IBM_I_PASSWORD,
            IBM_I_DATABASE,
            IBM_I_TABLE,
            skipSnapshotBackfill);

    // first step: check the snapshot data
    String[] snapshotForSingleTable =
        new String[] {
          "+I[101, user_1, Shanghai, 123567891234]",
          "+I[102, user_2, Shanghai, 123567891234]",
          "+I[103, user_3, Shanghai, 123567891234]",
          "+I[109, user_4, Shanghai, 123567891234]",
          "+I[110, user_5, Shanghai, 123567891234]",
          "+I[111, user_6, Shanghai, 123567891234]",
          "+I[118, user_7, Shanghai, 123567891234]",
          "+I[121, user_8, Shanghai, 123567891234]",
          "+I[123, user_9, Shanghai, 123567891234]",
          "+I[1009, user_10, Shanghai, 123567891234]",
          "+I[1010, user_11, Shanghai, 123567891234]",
          "+I[1011, user_12, Shanghai, 123567891234]",
          "+I[1012, user_13, Shanghai, 123567891234]",
          "+I[1013, user_14, Shanghai, 123567891234]",
          "+I[1014, user_15, Shanghai, 123567891234]",
          "+I[1015, user_16, Shanghai, 123567891234]",
          "+I[1016, user_17, Shanghai, 123567891234]",
          "+I[1017, user_18, Shanghai, 123567891234]",
          "+I[1018, user_19, Shanghai, 123567891234]",
          "+I[1019, user_20, Shanghai, 123567891234]",
          "+I[2000, user_21, Shanghai, 123567891234]"
        };
    tEnv.executeSql(sourceDDL);
    TableResult result = tEnv.executeSql("select * from CUSTOMERS");
    try (CloseableIterator<Row> iterator = result.collect()) {
      JobID jobId = result.getJobClient().get().getJobID();

      // 5. 校验快照阶段 (Snapshot Phase)
      List<String> expectedSnapshot = Arrays.asList("+I[1, Alice, 20]", "+I[2, Bob, 25]");
      validateResults(iterator, expectedSnapshot);

      // 🧩 架构师思维：在流转增量阶段前模拟故障
      triggerFailover(FailoverType.TM, jobId, miniClusterResource.getMiniCluster(), () -> {});

      // 6. 模拟数据库增量变更 (Stream Phase)
      executeJdbcSql("INSERT INTO CUSTOMERS VALUES (3, 'Charlie', 30)");
      executeJdbcSql("UPDATE CUSTOMERS SET AGE = 21 WHERE ID = 1");

      // 7. 校验增量数据
      List<String> expectedStream =
          Arrays.asList("+I[3, Charlie, 30]", "-U[1, Alice, 20]", "+U[1, Alice, 21]");
      validateResults(iterator, expectedStream);
    } finally {
      result.getJobClient().ifPresent(client -> client.cancel());
    }

    List<String> expectedSnapshotData = new ArrayList<>();
    for (int i = 0; i < captureCustomerTables.length; i++) {
      expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
    }
  }

  private void sleepMs(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
    }
  }

  private static List<String> fetchRows(Iterator<Row> iter, int size) {
    List<String> rows = new ArrayList<>(size);
    while (size > 0 && iter.hasNext()) {
      Row row = iter.next();
      rows.add(row.toString());
      size--;
    }
    return rows;
  }

  /** The type of failover. */
  protected enum FailoverType {
    TM,
    JM,
    NONE
  }

  /** The phase of failover. */
  protected enum FailoverPhase {
    SNAPSHOT,
    STREAM,
    NEVER
  }

  protected static void triggerFailover(
      FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
      throws Exception {
    switch (type) {
      case TM:
        restartTaskManager(miniCluster, afterFailAction);
        break;
      case JM:
        triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
        break;
      case NONE:
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + type);
    }
  }

  protected static void triggerJobManagerFailover(
      JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
    final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
    haLeadershipControl.revokeJobMasterLeadership(jobId).get();
    afterFailAction.run();
    haLeadershipControl.grantJobMasterLeadership(jobId).get();
  }

  protected static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
      throws Exception {
    miniCluster.terminateTaskManager(0).get();
    afterFailAction.run();
    miniCluster.startTaskManager();
  }

  private static final String USER = IBM_I_USER;
  private static final String PASS = IBM_I_PASSWORD;

  // --- 工具方法 ---

  private static void assumeExternalEnvironmentConfigured() {
    Assume.assumeFalse(
        "Set ibmi.test.* system properties to run the IBM i integration test.",
        IBM_I_HOST.startsWith("<")
            || IBM_I_USER.startsWith("<")
            || IBM_I_PASSWORD.startsWith("<")
            || IBM_I_DATABASE.startsWith("<")
            || IBM_I_TABLE.startsWith("<"));
  }

  private void validateResults(Iterator<Row> iterator, List<String> expected) {
    List<String> actual = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 60000; // 60秒超时

    while (actual.size() < expected.size() && System.currentTimeMillis() < deadline) {
      if (iterator.hasNext()) {
        actual.add(iterator.next().toString());
      }
    }

    List<String> sortedActual = actual.stream().sorted().collect(Collectors.toList());
    List<String> sortedExpected = expected.stream().sorted().collect(Collectors.toList());
    Assert.assertEquals("数据不一致！", sortedExpected, sortedActual);
  }

  private void executeJdbcSql(String sql) throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASS);
        Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  private void setupInitialData() throws Exception {
    executeJdbcSql("DELETE FROM CUSTOMERS");
    executeJdbcSql("INSERT INTO CUSTOMERS VALUES (1, 'Alice', 20)");
    executeJdbcSql("INSERT INTO CUSTOMERS VALUES (2, 'Bob', 25)");
  }
}
