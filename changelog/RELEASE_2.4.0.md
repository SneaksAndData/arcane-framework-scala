## Framework / Core
- **Sink Migrations**: Introduced a versioned sink migration mechanism (`migrations.base.Migration`, `MigrationService`, `DefaultMigrationService`) and a dedicated migration stream graph (`DefaultMigrationGraphBuilder`). The applied migration set is recorded in the target table property `arcane.core.migration-identifier`. Migrations are declared per source via `StreamingSource.definedMigrations`, so each plugin runs the framework migrations followed by its own, and `MergeServiceClient.execute(query, queryName, operation)` was added to run the required DDL/DML.
  - **Run Mode**: Migrations are executed by starting the application with `STREAMCONTEXT__RUNMODE=Migrate`. `ApplicationRunMode` now enumerates `Stream`, `Backfill` and `Migrate`, exposed as `BaseStreamContext.isMigrating`; `StreamGraphResolver` resolves the migration graph in that mode, and an unrecognized run mode fails with a `FatalStreamFailException`.
- **Migration Gate**: `DefaultStreamBootstrapper.checkMigrations` now runs before the stream graph starts. When the sink is missing migrations or is not at the current migration set, the stream fails fast with a `FatalStreamFailException` instructing the operator to run the application in `Migrate` mode. The check is skipped in `Migrate` mode itself.
- **Metadata Key Namespacing**: `MetadataKeys` now exposes `getFrameworkKey(name)` for `arcane.core.*` keys and `getPluginKey(pluginName, name)` for `arcane.plugin.<plugin>.*` keys. All framework property reads and writes were moved to the new keys, including watermarking, throughput estimates and backfill completion.
- **Legacy Watermark Fallback Removed**: The deprecated fallback that read the watermark from the Iceberg table `comment` property was dropped. Watermarks are now required to exist under `arcane.core.watermark`; tables from 2.3 need the migration set applied before restart.
- **Table Documentation**: Added `StreamDocumentation` (`STREAMCONTEXT__PLUGIN_NAME`, `STREAMCONTEXT__PLUGIN_GITHUB_REPO_LINK`) and target table `comment` set to `Table managed by {plugin name} {repository link}` both on table creation and via migration.
- **Row Modifications & Surrogate Columns**: Reworked `DataRowModification` into a single ADT under `models.settings.sources.modification`, configured through `SupportedModifications` (`modifications.fieldSelector` with `include`/`exclude`, `modifications.surrogateTimestamp`) and applied uniformly to source schemas and data rows by the source base classes. The same pipeline synthesizes `ARCANE_MERGE_KEY` from the configured primary-key columns and `ARCANE_VERSION` from the configured version column. Merge keys are now computed in-process with `HashUtils.murmur3` (Murmur3 x64 128, UTF-8, seed 0), matching the Trino expression `lower(to_hex(murmur3(to_utf8(...))))` and replacing the previous per-plugin SHA-256/SQL-side implementations. Missing or NULL primary-key values fail the stream with a `FatalStreamFailException`.
- **Load Timestamp**: Enabling `modifications.surrogateTimestamp` (`SurrogateTimestampSetting`) adds the `ARCANE_LOAD_TIMESTAMP` column (`TimestampType`, UTC) to the source schema and to every emitted row, recording when Arcane loaded the batch. The column is only added when the schema does not already declare it (case-insensitive), and the `FrozenSurrogateTimestamp` modification allows a preset value to be supplied instead of the current time.
- **Source Hierarchy**: Sources that need the full INSERT/UPDATE/DELETE operation set are now first-class citizens through `InsertUpdateDeleteSource`, which only asks the implementation for a primary key and a version field (`PrimaryKeyProvider`/`VersionProvider`) and derives both surrogate columns for it. MsSql, Synapse Link, Blob Listing and Pull Stream all extend it; `BlobStreamingSource` changed from a trait to an abstract class accordingly.
  - `StreamingSource` — the bare contract: source schema (`getSourceSchema`), sharding and `definedMigrations`.
  - `DefaultStreamingSource` — applies the configured modifications (field selector, load timestamp) to the schema and rows, without assuming any merge semantics.
  - `InsertUpdateDeleteSource` — extends `DefaultStreamingSource` and guarantees `ARCANE_MERGE_KEY`/`ARCANE_VERSION` are present in both schema and rows, so merge, deduplication and delete handling work for any source that can provide a primary key and a version.
- **Field Selection Moved Into Modifications**: Removed `FieldSelectionRuleSettings`, `FieldsFilteringService`, `FieldFilteringTransformer`, `ColumnSummaryFieldsFilteringService` and `ColumnSummaryFieldSelector`. Field filtering is no longer a pipeline stage and is applied by the source itself through the field selector modification.
- **Merge Batch Simplification**: Batch models (`SqlServerChangeTrackingMergeBatch`, `SynapseLinkMergeBatch`, `UpsertBlobMergeBatch`, `PullStreamChangeTrackingMergeBatch`) no longer receive a merge-key parameter and now reference `MergeKeyField`/`VersionField` directly, so deduplication always uses the framework-managed columns.
- **Change Capture Window**: `DefaultSourceDataProvider.changeStream` now accepts both the previous and the current watermark, and all change streams (`MsSql`, `Blob`, `Synapse`) apply the upper bound from `changeCaptureRangeLimit`. Previously the limit only shortened the recorded watermark while the source still read up to the latest available version.
- **Settings Overrides**: Introduced `Mergeable` settings with `Override*` counterparts (`OverrideStreamContext`, `DefaultOverrideStreamContext`, overrides for sink, staging, source, throughput, stream mode, observability, backfill, change capture, table maintenance and merge client). `PluginStreamContext.getLayer` now merges the JSON document supplied in `STREAMCONTEXT__SPEC_OVERRIDE` on top of `STREAMCONTEXT__SPEC`, enabling operator-driven partial configuration patches.
- **Iceberg Catalog Credentials**: The catalog no longer installs a framework-built `S3FileIO` with static credentials; file IO is delegated to the REST catalog (remote signing / vended credentials). The `ARCANE_FRAMEWORK__S3_CATALOG_ENDPOINT`, `ARCANE_FRAMEWORK__S3_CATALOG_ACCESS_KEY_ID`, `ARCANE_FRAMEWORK__S3_CATALOG_SECRET_ACCESS_KEY` and `ARCANE_FRAMEWORK__S3_CATALOG_REGION` environment variables are no longer read. `IcebergCatalogSettings.maxCatalogInstanceLifetime` was removed and the catalog is now created once per application lifetime.
- **Iceberg V3 / Variant**: `CreateTableRequest` accepts creation-time `properties` and automatically pins `format-version=3` when the schema contains a `variant` column (`CreateTableRequest.effectiveProperties`), since the format version cannot be raised after creation. This is applied consistently to target, staging and backfill tables. Iceberg `Types.VariantType` now converts to `ArcaneType.ObjectType`.
- **Schema Handling**: Avro-to-Arcane schema conversion no longer appends `ARCANE_MERGE_KEY` implicitly. Added the opaque `MergeableArcaneSchema` type with a strict Iceberg `Schema` conversion that re-tags an existing merge-key column instead of dropping the tag or duplicating it. `ArcaneSchema.isIndexed` no longer throws on empty schemas, and `nextFieldId`/`addIndexedField(fieldName, fieldType)` were added.
- **Merge & Maintenance Interruption**: `JdbcMergeServiceClient` and `TargetMaintenanceProcessor` now execute statements with `attemptBlockingCancelable` and cancel the in-flight statement server-side on fiber interruption. This prevents orphaned long-running queries on the query engine (e.g. Trino) when a pod is terminated mid-merge.
- **Failure Handling**: Added `FatalStreamFailExceptionWithCause`, `TransientStreamFailExceptionWithCause`, `UnsupportedPluginOperationException` and the `unsupported(operation, component)` helper for members that cannot be meaningfully implemented by a plugin.
- **Removed Dead Code**: Removed `StreamLifetimeService`/`PosixStreamLifetimeService` and the stream lifetime guard from `GenericStreamRunnerService` (the stream now completes when the graph completes; SIGTERM/SIGINT are handled by `handleAppFailure`). Removed `CdmParser` (replaced by `CSVParser`), unused catalog settings and other 2.1-era leftovers.
- **CI / Build**: Moved test execution to `just` recipes (`just test`, `just test-only`, `just fmt-check`), moved test resources to `test-resources/`, added Dependabot for GitHub Actions with auto-merge and gated CI to the top PR of a stacked pull request.

## Migrations for this release
Applied once by starting the application with `STREAMCONTEXT__RUNMODE=Migrate`.
- `WatermarkKeyMigration` — moves the watermark from the legacy `arcane-watermark` property to `arcane.core.watermark`.
- `MbsEstimatesKeyMigration` — moves throughput estimates from `mbs-estimates` to `arcane.core.throughput.mbs.estimates`, preserving an already migrated value.
- `TableCommentMigration` — aligns the target table `comment` property with the stream documentation string (`Table managed by {plugin name} {repository link}`).

## Plugin Compatibility
- `StreamSourceSettings` now requires `modifications: DataRowModificationSettings`; `fieldSelectionRule` was removed and plugin stream contexts must be updated accordingly.
- Plugins should provide `STREAMCONTEXT__PLUGIN_NAME` and `STREAMCONTEXT__PLUGIN_GITHUB_REPO_LINK`, used to build the target table `comment`. When unset, the `UNSPECIFIED_PLUGIN` and `UNKNOWN_REPOSITORY` fallbacks are used, so the table comment ends up uninformative.
- Remove `FieldsFilteringService`, `FieldFilteringTransformer` and `StreamLifetimeService` from plugin ZLayer assemblies; `MigrationService` is now part of `FrameworkProvidedPipelineServices`.
- `IcebergCatalogSettings.maxCatalogInstanceLifetime` must be removed from plugin stream contexts and CRDs, along with any `ARCANE_FRAMEWORK__S3_CATALOG_*` configuration.
- Existing sinks created by 2.3.x must run once with `STREAMCONTEXT__RUNMODE=Migrate` before a normal restart; otherwise the stream fails the migration check.

## Pull Stream (DynamoDB)
- **New Source**: Added the `PullStream` plugin (`PullStreamingSource`, `PullStreamSourceDataProvider`, `PullStreamStreamingDataProvider`, `PullStreamStagedBatchFactory`) that reads Arcane push-stream output from a DynamoDB table. Configuration is provided by `PullStreamSourceSettings`.
- **Watermarking**: Introduced `PullStreamWatermark` over the configured version field, with epoch fallback for empty sources. Change queries use `#pk = :pk AND #wm > :t`, and watermark values are normalized to UTC ISO-8601 so string range comparisons stay correct when producers use mixed offsets.
- **Pagination**: DynamoDB queries transparently follow `LastEvaluatedKey`, logging per-page item counts, and decoded rows are chunked explicitly to bound peak memory for large pages.
- **Payload Decoding**: Each item's `payload` is decoded against the sink table schema with `AvroJsonDecoder`; the DynamoDB `id` and version attribute are merged into the document under `dattr__`-prefixed field names so they can feed `ARCANE_MERGE_KEY`/`ARCANE_VERSION` without colliding with payload fields, and are stripped before emitting final `DataRow`. Nested documents and arrays are stored as Iceberg `variant` columns.
- **Table-Driven Source Settings**: `json-pointer-expression`, `page-size` and `decode-chunk-size` are stored as sink table properties (`arcane.plugin.push-stream.*`) and are required when the source layer is built, so the reader uses the same layout the producer provisioned.
- **Merging**: `PullStreamChangeTrackingMergeBatch` deduplicates by merge key and keeps the row with the highest `ARCANE_VERSION` before merging, matching the behavior of other change-tracking sources.
- **Backfill**: Backfill and sharding are not supported. No-op backfill providers keep the plugin startable, while unsupported shard operations fail with `UnsupportedPluginOperationException`.
- **Migrations**: Added `PullStreamArcaneMergeKeyMigration` (murmur3 merge key), `PullStreamVersionFieldMigration` (moves the legacy watermark column into `ARCANE_VERSION` as epoch millis and drops it) and `PullStreamPropertiesMigration` (moves source settings into the namespaced table properties).

## Microsoft Synapse
- **Surrogate Columns**: `SynapseLinkStreamingSource` now derives `ARCANE_MERGE_KEY` from `Id` using murmur3 and `ARCANE_VERSION` from `versionnumber`; both technical columns are excluded from emitted rows. Merge queries compare `ARCANE_VERSION` instead of `versionnumber`.
- **Version-Bounded Batches**: `getChanges(startFrom, endAt)` limits eligible date folders to the requested capture window. Batch folders that fail eligibility checks are filtered out before their schema is read, so malformed batches no longer abort the stream.
- **Schema & Modifications**: Root-level and batch-level schemas now go through the same row-modification pipeline, replacing the ad-hoc field-selection logic. CSV rows are produced by the renamed `CSVParser.parseCSVLineToRow`, and CDM schema conversion no longer injects the merge key.
- **Migrations**: Added `SynapseLinkArcaneMergeKeyMigration` (recomputes `ARCANE_MERGE_KEY` with murmur3 and drops the legacy `Id` column) and `SynapseLinkArcaneVersionMigration` (adds `ARCANE_VERSION` when missing, copies `versionnumber` into it and drops the legacy column).

## Microsoft SQL Server (MsSql)
- **Cached Schema**: `MsSqlStreamingSource` now resolves the table schema and column summaries once when the source is constructed (`SqlSchema`, `ColumnSummary`) instead of querying `INFORMATION_SCHEMA` and rebuilding the schema for every batch. `getFilteredSummaries` restricts PK/column summaries to the currently selected fields.
- **Change Capture Window**: Added `getChangesForVersionRangeQuery`, which reads changes between two change-tracking versions and applies the upper bound in SQL (`WHERE CAST(lastId AS bigint) <= maxId`), so `changeCaptureRangeLimit` is enforced by the source itself.
- **Surrogate Columns**: `ChangeTrackingVersion` is now used internally to derive `ARCANE_VERSION` and is excluded from emitted rows; `ARCANE_MERGE_KEY` is computed in the framework with murmur3 over the primary-key columns. The SHA-256 merge-key expression was removed from the generated select queries.
- **Migrations**: Added `SqlServerArcaneMergeKeyMigration` (murmur3 merge key) and `SqlServerArcaneVersionMigration` (adds `ARCANE_VERSION`, copies `ChangeTrackingVersion` into it and drops the legacy column).

## Blob List (CSV / JSON / Parquet)
- **Merge Key**: Primary keys are now hashed into `ARCANE_MERGE_KEY` with the shared murmur3 implementation instead of the previous SHA-256/Base64 scheme; `BlobBatchCommons.enrichBatchRow` only adds the version column now.
- **ARCANE_VERSION**: File `createdon` (epoch seconds) is used to populate `ARCANE_VERSION`, which drives merge deduplication; `createdon` is no longer emitted as a sink column.
- **JSON Decoding**: Replaced the inlined JSON handling with `AvroJsonDecoder` (optional-union encoding, tolerant/strict missing-field modes, JSON pointer support) and `JsonVariantConverter` (Iceberg `Variant` construction). The `jsonArrayPointers` setting and array-explode behavior were removed: nested objects and arrays are always written as Iceberg `variant`, requiring format version 3.
- **Change Capture Window**: `getChanges(startFrom, endAt)` now filters listed files by `createdOn` within the requested window, so the change capture range limit is applied correctly.
- **Migrations**: Added `BlobListingArcaneMergeKeyMigration` (murmur3 merge key) and `BlobListingArcaneVersionMigration` (adds `ARCANE_VERSION`, copies `createdon` into it and drops the legacy column).

## Dependencies
- **Language / Build**:
  - Scala compiler upgraded from `3.8.3` to `3.9.0`
  - `sbt` upgraded from `1.12.9` to `2.0.9`
- **ZIO**:
  - `dev.zio:zio`, `zio-streams` and `zio-test` upgraded from `2.1.26` to `2.1.27`
  - `dev.zio:zio-cache` upgraded from `0.2.8` to `0.3.0`
  - `dev.zio:zio-metrics-connectors` and `zio-metrics-connectors-datadog` upgraded from `2.5.6` to `2.6.0`
- **Iceberg**:
  - `org.apache.iceberg:iceberg-api`, `iceberg-core`, `iceberg-parquet` and `iceberg-aws` upgraded from `1.10.1` to `1.12.0`
  - `org.apache.hadoop:hadoop-common` and `hadoop-mapreduce-client-core` upgraded from `3.4.3` to `3.5.0`
- **AWS**:
  - `software.amazon.awssdk:s3`, `auth`, `http-auth-aws`, `sts` and `kms` upgraded from `2.33.13` to `2.56.0`
  - Added `software.amazon.awssdk:dynamodb` `2.56.0` for the Pull Stream source
- **Azure**:
  - `com.azure:azure-storage-blob` upgraded from `12.32.0` to `12.36.0`
  - `com.azure:azure-identity` upgraded from `1.18.3` to `1.18.7`
- **Trino**:
  - `io.trino:trino-jdbc` driver upgraded from `481` to `483`
- **Logging**:
  - `org.apache.logging.log4j:log4j-to-slf4j` upgraded from `2.25.3` to `2.26.1`
  - `ch.qos.logback:logback-classic` upgraded from `1.5.34` to `1.6.5`
