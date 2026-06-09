# Arcane Framework Release Notes Draft (from v2.2.1 to HEAD)

This draft contains a comprehensive changelog of commits and updates introduced between tag **v2.2.1** and the current **HEAD** (spanning Pull Requests #390 through #430).

---

## Framework / Core
- **Introduce StructuredZStream**: Introduced `StructuredZStream`, which allows schema information to be passed along with a stream. Now schema migration can be carried out once (for the first batch) rather than at every batch when `isUnifiedSchema` is set to `false`.
- **Backfill**: Backfill OVERWRITE has been re-written to utilize source sharding to drastically improve backfill times. OVERWRITE mode now supports resumable backfills when the same STREAMCONTEXT__BACKFILL_ID is provided on a restart.
- **Backfill**: Backfill MERGE mode (`DefaultBackfillMergeGraphBuilder`) has been re-written to provide a single-changeset catchup stream, followed by a watermark update. This mode is equivalent to running a regular stream for a single changeset.
- **Batch Disposal via REST Catalog**: Refactored the batch disposal execution from JDBC-based queries to the REST Catalog API via `CatalogDisposeServiceClient` and `DisposeServiceClient` to decouple concerns.
- **Staging Table Prefix Removal**: Dropped the staging table name prefix setting (`stagingTablePrefix`). All dynamic names are now provided via a separate service and ownership is maintained by having `streamId` in table names (except for target tables).
- **Streaming Age Tracking**: Improved stream watermark age metric to guarantee that streaming age metric reflects the time since last update on the source. For infrequently updated tables the age will slide forward with source watermark updates to prevent age creep.
- **S3 List Memory Optimization**: Optimized the `streamPrefixes` paginator memory footprint within `S3BlobStorageReader`, improving memory usage for buckets with large number of objects under a prefix.
- **ZIO Logging Enhancements**: Added custom helper method for WARN-level ZIO log and upgraded merge service retry logs from INFO to WARN to enable better operations visibility (port from 221-postrelease).
- **Schema Migration**:
  - Row schema grouping has been removed.
  - Schema migration logic moved to `SchemaMigrationProcessor`. It handles caching of the schema and a cache update based on results of a schema update, when it happens. Provides a no-op pass-through in case schemas match.
  - Ensured schema converter from Apache Iceberg to `ArcaneSchema` adds the `mergeKey` strictly once, eliminating duplicate column declarations when reading an Iceberg schema that has `ARCANE_MERGE_KEY` in it.
- **Maintenance** Table maintenance moved to a separate `TableMaintenanceProcessor` and fully decoupled from the merge client.

## Microsoft Synapse
- **StreamingSource**: Refactored SynapseReader to implement a new consolidated `StreamingSource` trait.
- **Backfill**: Added sharding provider for OVERWRITE
  - Added tests for sharded backfill with 10.000 row source tables. Both fresh and interrupt scenarios are tested.
- **Backfill**: Added MERGE backfill data provider

## Microsoft SQL Server (MsSql)
- **StreamingSource**: Refactored MsSqlReader to implement a new consolidated `StreamingSource` trait.
- **Backfill**: Added sharding provider for OVERWRITE
- **Backfill**: Added MERGE backfill data provider

## Blob List (CSV / JSON / Parquet)
- **StreamingSource**: Refactored BlobSource to implement a new consolidated `StreamingSource` trait.
- **Backfill**: Added sharding provider for OVERWRITE
- **Backfill**: Added MERGE backfill data provider

## Dependencies

- **ZIO**:
  - `dev.zio:zio` and `zio-streams` upgraded from `2.1.24` to `2.1.26`
  - `dev.zio:zio-test` and `zio-test-sbt` upgraded from `2.1.24` to `2.1.26`
  - `dev.zio:zio-metrics-connectors` and `zio-metrics-connectors-datadog` upgraded from `2.5.5` to `2.5.6`
- **Trino**:
  - `io.trino:trino-jdbc` driver version upgraded from `478` to `481`
- **Azure**:
  - `com.azure:azure-identity` security dependency upgraded from `1.18.1` to `1.18.3`
- **Logging**:
  - `ch.qos.logback:logback-classic` upgraded from `1.5.32` to `1.5.34`
