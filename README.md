# Apache Flink CDC Connector for IBM i DB2

An Apache Flink CDC connector for IBM i DB2, also known as DB2 UDB for AS/400.

This connector is based on the official open-source Apache Flink CDC DB2 connector architecture and implementation. It has been extended and adapted for IBM i-specific requirements, including IBM i catalog discovery, JT400 connectivity, DB2 Journal change capture, IBM i system-name mapping, and Journal-based offset management.

> **Project status:** Active development. Validate the connector against your own IBM i environment before using it in production.

## Features

- Capture `INSERT`, `UPDATE`, and `DELETE` changes from IBM i DB2 tables.
- Read initial table snapshots through JDBC.
- Read ongoing changes from the IBM i DB2 Journal.
- Use the connector through Flink SQL and the Table API.
- Support incremental snapshot execution through the Flink CDC framework.
- Support Debezium-based snapshot and streaming change events.
- Discover tables and schemas through IBM i `QSYS2` catalog views.
- Map IBM i long table names to system table names used by Journal access.
- Manage IBM i Journal positions as Flink CDC offsets.
- Restore source progress through Flink checkpoint state.
- Pass Debezium connector properties using the `debezium.*` option prefix.
- Expose table and operation metadata through Flink metadata columns.

## Architecture

The connector integrates the following components:

```text
Flink SQL / Table API
          |
          v
Flink Table Factory SPI: ibmi-cdc
          |
          v
Db2As400TableSource
          |
          +-- Incremental snapshot enabled
          |       |
          |       v
          |   Flink CDC JdbcIncrementalSource
          |       |
          |       +-- IBM i Dialect and chunk splitter
          |       +-- JDBC snapshot FetchTask
          |       +-- Journal streaming FetchTask
          |       +-- Journal offset and checkpoint state
          |
          +-- Incremental snapshot disabled
                  |
                  v
              Debezium SourceFunction / Embedded Engine

Both runtime paths use Debezium IBM i and JT400 to access IBM i DB2 and its Journal.
```

The connector reuses the Flink CDC source framework rather than implementing an independent Enumerator/Reader framework. IBM i-specific behavior is implemented mainly in the Dialect, connection utilities, schema handling, FetchTasks, offset model, and Debezium compatibility classes.

## Compatibility

The current module is configured with the following versions:

| Component | Version |
|---|---:|
| Java | 21 |
| Apache Flink | 1.18.1 |
| Apache Flink CDC | 3.2.1 |
| Debezium IBM i | 2.7.3.Final |
| JT400 | 20.0.6 |

### Validation environment

The current validation baseline is:

| Parameter | Value |
|---|---|
| Database product | DB2 UDB for AS/400 |
| Database version | `07.05.0000` |
| IBM i version | `V7R5M0` |
| Validated scope | IBM i JDBC/JT400 connectivity, `QSYS2` metadata queries, table snapshots, Journal change reading, and offset recovery |

This validation baseline does not imply compatibility with every IBM i PTF or cumulative update level. For a formal compatibility assessment, also record the IBM i PTF/CUM level, JDK, JT400, Flink, Flink CDC, and Debezium versions, together with CCSID, library-list, SSL, table-key, and Journal configuration.

## Quick Start with Flink SQL

Create a source table using the `ibmi-cdc` connector:

```sql
CREATE TABLE source_table (
    id INT NOT NULL,
    name STRING,
    updated_at TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'ibmi-cdc',
    'hostname' = '<ibmi-host>',
    'port' = '50000',
    'username' = '<ibmi-user>',
    'password' = '<ibmi-password>',
    'database-name' = '<library-or-database>',
    'table-name' = '<schema>.<table>',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '8096'
);
```

Consume the change stream with a normal Flink SQL query:

```sql
SELECT * FROM source_table;
```

Use secret management or deployment-level configuration for credentials. Do not put real passwords in SQL files, source code, README files, or test cases.

## Configuration

### Required options

| Option | Description |
|---|---|
| `hostname` | IBM i host name or IP address. |
| `port` | IBM i database port. The current Table Factory default is `50000`. |
| `username` | IBM i database user. |
| `password` | IBM i database password. |
| `database-name` | Database or library used by the connector. |
| `table-name` | Captured table identifier. Use the schema/table form expected by the configured IBM i and Debezium filters. |

### Startup and snapshot options

| Option | Description |
|---|---|
| `scan.startup.mode` | `initial` or `latest-offset`. Other startup modes are not currently supported. |
| `scan.incremental.snapshot.enabled` | Enables the Flink CDC incremental snapshot path. |
| `scan.incremental.snapshot.chunk.size` | Number of rows targeted by a snapshot chunk. |
| `scan.incremental.snapshot.chunk.key-column` | Optional primary-key column used as the snapshot chunk key. |
| `scan.snapshot.fetch.size` | JDBC fetch size used for snapshot reads where supported by the driver/configuration. |
| `scan.incremental.snapshot.backfill.skip` | Controls whether snapshot backfill is skipped. Review consistency implications before enabling it. |
| `scan.incremental.close-idle-reader.enabled` | Controls idle-reader closing and depends on Flink checkpoint-after-tasks-finish support. |

### Connection and split options

The connector also exposes the applicable Flink CDC JDBC/source options for connection timeout, retry count, connection pool size, chunk metadata grouping, and split-key distribution factors. Option names and defaults are defined by the Flink CDC 3.2.1 APIs used by this module.

Debezium properties can be passed with the `debezium.` prefix, for example:

```sql
'debezium.topic.prefix' = 'ibmi-cdc'
```

## Runtime Modes

### Incremental snapshot mode

When `scan.incremental.snapshot.enabled = true`, the connector uses the Flink CDC JDBC incremental source framework:

1. Discover captured tables and schemas through the IBM i JDBC connection.
2. Determine snapshot chunks using primary-key metadata.
3. Read each snapshot chunk through JDBC.
4. Read Journal changes through the Debezium IBM i streaming source.
5. Coordinate offsets, watermarks, backfill, and checkpoint state through the Flink CDC framework.

Incremental snapshots require a usable primary key or supported split key. Validate composite keys, IBM i key metadata, and boundary SQL on the target system.

### Debezium SourceFunction mode

When incremental snapshots are disabled, the connector uses the Debezium Embedded Engine through the Flink `DebeziumSourceFunction` compatibility path. This mode is useful for a simpler single-source execution path but does not use the same split coordination path as incremental snapshots.

## IBM i Considerations

- IBM i long table names and ten-character system names may require translation before Journal filtering.
- JDBC metadata discovery and Journal file filtering use different naming layers; validate both when changing schema or table filters.
- The connector uses JDBC for metadata and snapshot access and RPC/JT400-backed access for Journal streaming.
- Journal receiver rollover, retention, and permissions must be configured and tested on the IBM i system.
- CCSID and character-set settings can affect table names, column values, and metadata conversion.
- Snapshot splitting currently depends on primary-key metadata and a limited set of supported split-column types.
- SQL involving `QSYS2`, quoted identifiers, `FETCH FIRST`, and row-count queries must be validated on IBM i rather than assumed to behave like another DB2 platform.

## Known Limitations and Areas Requiring Validation

The following areas require additional validation before production use:

- Journal receiver rollover and long-running stream behavior.
- Snapshot backfill low/high watermark handling and termination.
- Checkpoint recovery and failover during both snapshot and streaming phases.
- Schema evolution and DDL propagation.
- Composite primary-key snapshot splitting.
- Tables without a conventional primary key.
- IBM i data types not covered by the current type mapping.
- UPDATE events when the Journal does not provide a before image.
- CCSID, timestamp, decimal, binary, and character conversion edge cases.
- Connection lifecycle and resource cleanup during repeated failover.

## Build

From this module directory, the standard Maven lifecycle commands are:

```bash
mvn -DskipTests compile
mvn test
mvn -DskipTests package
```

The build requires access to the parent Flink CDC build metadata and all Maven dependencies. Integration tests that access IBM i require an authorized test system and must be configured with externalized credentials. Do not run external integration tests against production data.

The package uses Maven Shade to assemble dependencies, merge the Flink Table Factory service file, relocate selected dependency packages, and exclude selected upstream classes that are overridden by this connector. Before publishing a release, inspect the shaded JAR and verify that the `ibmi-cdc` SPI entry and class relocation rules are correct.

## Development Layout

```text
src/main/java/
├── org/apache/flink/cdc/connectors/db2as400/
│   ├── table/       # Flink Table Factory, TableSource, startup and metadata
│   ├── source/      # config, Dialect, FetchTasks, offsets and IBM i utilities
│   └── utils/       # connector utilities
├── org/apache/flink/cdc/connectors/base/
│                   # Flink CDC Base compatibility adaptations
├── org/apache/flink/cdc/debezium/
│                   # Debezium source, history and RowData adaptations
└── io/debezium/
                    # IBM i JDBC, offset and event adaptations

src/main/resources/META-INF/services/
└── org.apache.flink.table.factories.Factory
```

## Relationship to Upstream Projects

This project is a derivative and extension of the official open-source Apache Flink CDC DB2 connector. It preserves the upstream Flink CDC source architecture and adds IBM i-specific integration code and compatibility adaptations.

This project is not affiliated with, endorsed by, or sponsored by:

- The Apache Software Foundation or Apache Flink;
- Debezium;
- IBM; or
- The JT400 project.

Please retain the original license headers and attribution notices in upstream-derived source files. Before distributing this repository, include the appropriate `LICENSE` and `NOTICE` files and review the license obligations of every upstream dependency, including Apache Flink CDC, Debezium, and JT400.

## Contributing

Contributions are welcome. Before submitting a change:

1. Identify whether the change affects the Table API, Source builder, IBM i Dialect, FetchTask, offset model, Debezium compatibility layer, or shaded packaging.
2. Add or update focused tests for offset ordering, split SQL, type conversion, startup validation, metadata conversion, or recovery behavior as applicable.
3. Validate the change on the IBM i baseline when it affects JDBC, `QSYS2`, Journal, naming, CCSID, or database-specific SQL behavior.
4. Check Maven formatting, SPI registration, shaded-JAR contents, and dependency/license notices.
5. Keep credentials, host names, tokens, and business data outside the repository.

## License

The repository contains code derived from Apache Flink CDC and integrates other open-source projects. Use the repository's `LICENSE` and `NOTICE` files as the authoritative licensing documents. Upstream license headers and attribution requirements remain applicable to their respective source files and dependencies.
