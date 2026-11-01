  # flink-connector-ibmi-cdc 开发指南

本文件适用于 `flink-connector-ibmi-cdc` 模块。模块位于仓库根目录 `D:/workspace/idea_2025/connector` 下；仓库根目录的 `AGENTS.md` 仍然适用。修改前先阅读本文件和目标类的现有实现。

## 模块定位

这是一个单 Maven JAR 模块，为 IBM i / DB2 for i（AS400）提供 Flink CDC Source。它不是独立实现完整的 Flink Enumerator/Reader，而是：

- 复用 Flink CDC 3.2.1 `JdbcIncrementalSource` 的增量快照框架；
- 实现 IBM i 专用 Dialect、表发现、schema 读取、chunk SQL、JDBC/RPC 连接、Journal offset 和 FetchTask；
- 复用 Debezium IBM i 2.7.3.Final 的 snapshot/streaming event source；
- 在 `org.apache.flink.cdc.*` 和 `io.debezium.*` 原包名下包含若干兼容补丁类，并通过 Shade 配置替换或隔离依赖中的同名类。

当前主要技术版本以 `pom.xml` 为准：Java 21（`maven.compiler.source/target`）、Flink 1.18.1、Flink CDC 3.2.1、Debezium 2.7.3.Final、JT400 20.0.6。仓库上层文档仍可能写 Java 11；改动版本或构建配置时必须以模块 POM 和实际编译结果为准，不要自行降级。

## 验证环境参数

当前已确认的 IBM i / DB2 验证环境：

| 参数 | 值 |
|---|---|
| 数据库产品 | DB2 UDB for AS/400 |
| 数据库版本标识 | `07.05.0000` |
| IBM i 操作系统版本 | `V7R5M0`（用户提供标识 `V7R5m0`，文档统一使用大写格式） |
| 验证范围 | IBM i JDBC/JT400 连接、QSYS2 元数据查询、表快照、Journal 增量读取及 offset 恢复 |

该环境参数是连接器兼容性验证基线，不代表所有 IBM i 版本都已验证。涉及 JDBC URL、QSYS2 catalog SQL、长名称/系统短名称、主键发现、Journal receiver、`FETCH FIRST` 分片 SQL 或时间/字符集转换的改动，优先在上述 `DB2 UDB for AS/400 07.05.0000 / V7R5M0` 环境回归。

每次新增验证记录时，同时记录：IBM i PTF/CUM、JT400 版本、JDK 版本、Flink/Flink CDC 版本、Debezium 版本、连接属性（CCSID、库列表、SSL）以及测试表的 schema/主键/Journal 配置。凭据、主机名和真实业务数据不得写入本文件或提交记录。

## 目录与职责

```text
src/main/java/
├── org/apache/flink/cdc/connectors/db2as400/
│   ├── Db2As400Source.java                 # 旧 SourceFunction builder
│   ├── table/                               # Flink Table Factory/TableSource/metadata
│   ├── source/
│   │   ├── config/                          # Flink + Debezium 配置装配
│   │   ├── dialect/                         # IBM i Dialect、schema、chunk、连接池
│   │   ├── fetch/                           # snapshot、backfill、journal stream FetchTask
│   │   ├── offset/                          # JournalOffset/JournalOffsetFactory
│   │   ├── handler/                         # schema change 事件适配
│   │   └── utils/                           # SQL、表发现、类型、连接工具
│   └── utils/                               # Table 层配置打印等工具
├── org/apache/flink/cdc/connectors/base/    # 原包名下的 CDC Base 兼容补丁
├── org/apache/flink/cdc/debezium/           # Debezium Source/deserializer/history 补丁
└── io/debezium/                              # IBM i JDBC/offset/事件相关补丁

src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory
                                             # Table Factory SPI 注册
src/test/java/                                # 当前主要是外部 IBM i 集成测试草稿
pom.xml                                       # 依赖、Shade、SPI 合并和类排除规则
```

### 入口和运行分支

1. Flink Table 通过 SPI 加载 `Db2As400TableSourceFactory`。
2. Factory identifier 是 `ibmi-cdc`，因此 SQL 使用：
   ```sql
   'connector' = 'ibmi-cdc'
   ```
3. Factory 解析必填项 `hostname`、`database-name`、`table-name`、`username`、`password`，并读取 `scan.*`、连接、chunk、startup 和 `debezium.*` 选项。
4. `Db2As400TableSource#getScanRuntimeProvider` 根据 `scan.incremental.snapshot.enabled` 分支：
   - `true`：`Db2As400SourceBuilder` → `JdbcIncrementalSource` → Dialect → Snapshot/Stream FetchTask；
   - `false`：`Db2As400Source.Builder` → `DebeziumSourceFunction` → Debezium Embedded Engine。
5. 两条路径都最终使用 Debezium IBM i 和 JT400 访问 IBM i DB2/Journal。

### 并行增量路径的关键调用点

- `Db2As400SourceBuilder` 创建 `Db2SourceConfigFactory`、`JournalOffsetFactory` 和 `Db2As400Dialect`。
- `Db2As400Dialect` 负责表发现、schema 查询、chunk splitter、连接池以及按 split 类型创建 FetchTask。
- `Db2As00SourceFetchTaskContext`（类名中的 `As00` 是现有名称，修改时不要未经计划擅自重命名）负责连接、schema history、offset、队列、dispatcher 和 metrics 的装配。
- `Db2As400ScanFetchTask` 执行 JDBC 分片快照，并通过 `StreamSplit` 执行 snapshot backfill。
- `Db2As400StreamFetchTask` 委托 Debezium `As400StreamingChangeEventSource` 读取 Journal 增量。
- `JdbcSourceEventDispatcher` 将数据、schema/history 和 watermark 事件送入共享队列；后续 Reader/Enumerator 生命周期来自 `flink-cdc-base`，不是本模块自有实现。

## 配置约定

### Flink Table 选项

必填：

- `hostname`
- `database-name`
- `table-name`
- `username`
- `password`

常用可选项：

- `port`（Factory 默认 50000）
- `server-time-zone`
- `scan.startup.mode`：当前只支持 `initial` 和 `latest-offset`
- `scan.incremental.snapshot.enabled`
- `scan.incremental.snapshot.chunk.size`
- `scan.incremental.snapshot.chunk.key-column`
- `scan.snapshot.fetch.size`
- `chunk-meta-group-size`
- `connect.timeout`、`connect.max-retries`、`connection.pool.size`
- split key distribution 上下界
- `scan.incremental.close-idle-reader.enabled`
- `scan.incremental.snapshot.backfill.skip`
- `debezium.*`：透传给 Debezium 的配置

修改选项时必须同时检查 `Db2As400TableSourceFactory`、`Db2As400TableSource`、`Db2SourceConfigFactory` 和 builder 的传递链。不要只在 Factory 增加选项而遗漏 SourceConfig 或 Debezium property。

### IBM i 命名和过滤

- Flink Table 层的 `table-name`、builder 的 `tableList`、Debezium `table.include.list` 和 Journal file filter 不是完全相同的命名层。
- `As400JdbcConnection#shortIncludes` 会将包含项拆成 schema/table，并查询 IBM i system name；不要假设任意正则都能直接用于 Journal 过滤。
- `Db2As400Dialect#isDataCollectionIdCaseSensitive` 当前返回 `true`。改大小写、schema 或长表名转换时，要同时验证 JDBC 表发现和 RPC Journal 过滤。
- `databaseList` 的现有实现实际上在配置生成时使用第一个数据库项；扩展多数据库前应先设计完整语义。

## 代码和实现规则

- 遵循现有 Apache License header、Google Java Format 和 Java 标准命名风格；避免无关格式化。
- 以现有 Flink CDC/Debezium API 版本为准。不要直接套用其他版本的类签名或配置名。
- 公共配置、Source、Dialect、Offset 和 FetchTask 改动必须保持序列化、恢复和 `equals/hashCode` 语义一致。
- 任何新增或修改的 SQL 都要使用参数绑定处理值，并正确区分 IBM i 标识符引用与字符串字面量；在真实 IBM i/JT400 上验证 SQL。
- 不在日志、测试、异常或文档中新增密码、token、主机凭据等敏感信息。配置打印使用 `OptionUtils` 的敏感值隐藏能力。
- 保持 JDBC 与 RPC 两条连接通道的生命周期清晰：创建连接的地方必须能定位对应的关闭路径；关闭时使用尽量可靠的 finally/try-with-resources 语义。
- 修改 `org.apache.flink.cdc.*` 或 `io.debezium.*` 下的类前，先确认它是上游补丁而不是普通业务类；同时检查 `pom.xml` 的 Shade relocation、artifact filter 和最终 JAR 中的类来源。
- 修改 SPI 实现时同步检查 `src/main/resources/META-INF/services/org.apache.flink.table.factories.Factory`，并验证最终 shaded JAR 仍能发现 `ibmi-cdc`。

## 必须理解的正确性边界

这些是当前实现的高风险区域；涉及它们的改动必须增加针对性验证并在变更说明中写明行为：

1. **Journal offset 顺序**：`JournalOffset#compareTo` 当前按 receiver 字符串和 event sequence 字符串比较。日志序号若是数值语义，必须确认多位数、receiver 切换和 receiver library 的顺序，不能仅凭字符串排序判断先后。
2. **快照 backfill 是否有界**：`Db2As400ScanFetchTask` 通过 stream task 执行 backfill；`Db2As400StreamFetchTask` 的包装层必须正确处理 low/high watermark、结束条件和上下文完成。不能只因为类名包含 `StreamSplit` 就认为 backfill 会结束。
3. **UPDATE before image**：RowData deserializer 的 changelog 处理依赖 Debezium 事件中的 before/after。缺少 before image 时，必须明确是降级为 after-only 还是拒绝/丢弃事件，并用测试固定语义。
4. **schema history/DDL**：`EmbeddedFlinkDatabaseHistory`、`JdbcSourceEventDispatcher` 和 `Db2As400SchemaChangeEventHandler` 是一条链。当前 schema change handler 仍是适配骨架，不能把“存在 schema change 类”当成 DDL 演进已完整支持。
5. **split 键与类型**：增量快照要求可用主键；chunk key 必须来自主键列集合。`Db2As400TypeUtils` 只覆盖当前列映射的一部分 JDBC 类型，新增 IBM i 类型时要同时覆盖 split 类型、Debezium schema 和 RowData converter。
6. **快照边界和 SQL**：分片查询的边界、复合主键、`FETCH FIRST`、行数查询和标识符大小写必须在 IBM i 上验证，不能只依赖普通数据库的 SQL 经验。
7. **配置覆盖顺序**：并行路径和旧 SourceFunction 路径对 `snapshot.mode` 与 `debeziumProperties` 的覆盖顺序不同。变更启动模式时两条路径都要检查。
8. **恢复与 history**：offset、split state、database history 必须一起验证；只验证首次启动不能证明 checkpoint/failover 恢复正确。

## 测试与验证

### 推荐验证顺序

1. 先做静态检查：确认新增类已被 Maven 源集包含、包名正确、SPI 文件存在，且没有无意修改上层目录中的其他模块。
2. 对纯逻辑代码增加单元测试：offset 反序列化/比较、split SQL 生成、类型映射、启动模式校验、metadata converter。
3. 做编译和打包验证：在本模块目录执行项目已有 Maven 生命周期，例如：
   ```bash
   mvn -DskipTests compile
   mvn test
   mvn -DskipTests package
   ```
   这些命令依赖父 POM和本地/远程 Maven 仓库；当前环境曾因离线模式缺少 `org.mockito:mockito-core:5.21.0` 而无法完成 compile，遇到依赖解析失败时要记录原始错误，不要宣称构建通过。
4. 发布前检查 shaded JAR：确认 SPI、重定位类、排除类和依赖签名文件符合预期；检查是否出现重复类或错误包名。
5. 只有具备明确授权的 IBM i 测试环境和临时凭据时才运行外部集成测试；测试结束后撤销/清理凭据，并确认没有写回源码或日志。

### 当前集成测试的使用边界

`src/test/java/org/apache/flink/cdc/connectors/db2as400/source/Db2As400SourceITCase.java` 是面向外部 IBM i 的测试草稿，包含硬编码连接信息、与 DDL/预期数据不完全一致的场景以及未完全接线的 failover 参数。修改测试前先整理为可配置、可复现的测试；在此之前不能把它作为并行快照、backfill、故障恢复或数据一致性已验证的证据。

## 变更完成清单

提交前逐项确认：

- [ ] 说明了改动属于 Table、Source、Dialect、Fetch、Debezium 补丁还是 Shade/发布层。
- [ ] 配置项从 SQL Factory 到 SourceConfig/Debezium 的传递链完整。
- [ ] 若改了 offset、split、schema history 或 backfill，已有恢复/边界测试覆盖。
- [ ] 若改了原包名补丁，检查了 POM relocation/filter 和最终 JAR 类来源。
- [ ] SPI 文件和 shaded JAR 可发现 `ibmi-cdc`。
- [ ] 运行了能运行的 Maven 检查，并记录依赖、环境或 IBM i 不可用导致的跳过项。
- [ ] 没有引入真实凭据、无关生成物或大范围格式化。
- [ ] 仅在验证结果真实可复现时，才宣称测试或构建通过。
