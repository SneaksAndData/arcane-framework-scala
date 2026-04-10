## Framework
- Developer API: Reworked `StreamContext` implementation. Plugins should now extend `DefaultPluginStreamContext` and define `source` val, which is source-specific settings group. Everything else is now standard and provided by the framework out-of-box.
- Developer API: Classes that extend `DefaultPluginStreamContext` are now natively JSON serializable. Plugins no longer need to define custom serialization proxy for their stream contexts.
- Doc: Updated `README` with an example of a plugin project in accordance with the new API.
- Developer API: properties coming from `BaseStreamContext` are now read via `zio.System` to allow usage of `TestSystem` in tests.
- Support for table partitioning has been removed until 2.3 release
- Developer API: Iceberg DDL is available via CatalogEntity/PropertyManager implementations, separately for Sink and Staging.
- Schema Management: Merge Service (Trino) is no longer used for the following operations:
  - Table creation
  - Table deletion
  - Schema discovery
  - Schema migrations
All DDL operations are now performed directly via CatalogEntityManager implementations for Staging and Sink.
- Schema Management: Removed usage of JDBC client when comparing target schema to batch schema. Iceberg REST Catalog now supplies schema information for existing tables.
- Schema Management: Removed all but one place where unindexed schemas were used. Schema comparison still ignored Iceberg fieldId when comparing schemas, to avoid massively breaking changes for existing streams.
- Developer API: Added `upickle` serializers for commonly used types like datalake paths, datetimes, durations and more.
- Developer API: Removed usage of scala3 `enum` in favor of `sealed trait` paired with `case class ... derives ReadWriter` nested into another `case class` that implements the trait.
This allows adding arbitrary configuration parameters to enum options, at the same time making them JSON-serializable.
- Developer API: Reworked all settings to be JSON-serializable and conformant with `PluginStreamContext` settings groups
- Developer API: Unified table name notation across multiple users via `TableNaming` string type alias
- Developer API: Added IcebergSinkEntityManager, IcebergSinkTablePropertyManager, IcebergStagingEntityManager, IcebergStagingTablePropertyManager for Iceberg DDL operations.
- Developer API: Merged backfill and streaming mode settings under `streamMode` settings group
- Developer API: Updated all `ZLayer` injectable services to only require `PluginStreamContext` to be available.
- Added support for retrying JDBC queries issued by `MergeServiceClient`
- Added support for random jitter in change capture sleeping iterations
- Moved pre-stream code (cleanups, table creation, schema migration on start) to a new `DefaultStreamBootstrapper` service
- Reworked `IcebergCatalogFactory` so instances of `RESTSessionCatalog` are properly closed on expiry and are not piled up in memory until we run out of free ports.
- Removed `MutableSchemaCache` and schema caching code. The need for schema caching will be assessed for 2.3 release.
- Unified source CDC and backfill stream implementations under `DefaultSourceDataProvider` which wraps source-specific `ZStream` code into a generic CDC and backfill `ZStream` objects.
`DefaultSourceDataProvider` also ensures watermarking and throughput shaping work identically across all source implementations.
- Added `ThroughputShaper` and two implementations: `MemoryBoundShaper` and `StaticShaper`. 
Primary goal is to improve memory usage for all streams by providing automatic row grouping based on available memory.
- Removed `RowGroupTransformer` and related code. Rows are now grouped via `rechunk` hint of a `ThroughputShaper`
- Fixed a bug related to exotic buffering stage behaviour, which caused 1-row chunks to be emitted occasionally.
- Added support for modifying connection properties of `MergeServiceClient` JDBC connection URL.
- Fixed a bug that caused empty batches to be emitted occasionally from the `StagingProcessor`, causing throughput drops.
- Updated CI to Java25 (scala version update will follow in 2.2.1)

## Microsoft Synapse
- Updated related settings groups in accordance with framework changes
- Made Azure SDK http client settings truly configurable
- Improved credential configuration readability for AzureStorageReader. It is now JSON-serializable.

## Microsoft SQL Server
- Updated related settings groups in accordance with framework changes
- Unified source settings by implementing `DatabaseSourceSettings`, which is supposed to be used with any JDBC source.
- Added support for modifying connection properties of a JDBC connection to the SQL Server.

## Blob List - Parquet
- Updated related settings groups in accordance with framework changes
- Properly fixed `StructType` usage, tables with `Struct` fields of any complexity can now be streamed.

## Blob List - JSON
- Updated related settings groups in accordance with framework changes

## Dependencies
- Added `zio-cache:0.2.8`
- Bump `upickle` to `4.4.3`
- Bump Iceberg dependencies to `1.10.1`
- Bump Hadoop dependencies to `3.4.3`
- Bump ZIO Logging to `2.5.3`
- Bump logback-classic to `1.5.32`
