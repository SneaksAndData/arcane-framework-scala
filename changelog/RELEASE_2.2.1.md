## Framework
- Observability: metric tags are now globally assigned to all metrics emitted from the runner, regardless if its Arcane-provided or external metrics like JVM.
- Throughput Shaper (Memory Bound): Throttle settings (chunkCost/Scale) are now correctly applied on a per-row basis.
  - Plugins should review MBS settings and update accordingly to avoid throughput issues. 
- Throughput Shaper (Memory Bound): Row output rate (r/s) is now computed by the shaper automatically based on GC statistics.
- Throughput Shaper (Memory Bound): Removed settings with no visible effect and refined naming to better reflect each setting's purpose.
- Throughput Shaper (Memory Bound): `meanStringTypeSizeEstimate` renamed to `fallbackStringTypeSizeEstimate` and is now only used when string length cannot be determined automatically.
- Iceberg REST API: Fixed an issue that can cause Trino to merge an empty staging table due to a delay in fast append propagation between catalog backend database replicas.
  - This change resolves a potential *data loss* with certain catalog backends like Aurora PostgreSQL. 
- Merge Service: Schema name and catalog removed from JDBC (Trino) connection strings.
  - This change requires plugins to update their *CRD*.
  - This change requires plugins to update *JDBC secret format*.
- Fixed a bug with `ArcaneType` comparison for `Short` type, causing SQL Server streams to perform an erroneous table migration, corrupting the latest snapshot.
- Merge Service: Retry attempts are now logged.
- Runtime: Arcane now uses Java 25 as a default runtime.
- Language: Arcane now uses Scala 3.8.3 complier version with sbt 1.12.

## Microsoft Synapse
- No changes

## Microsoft SQL Server
- No changes

## Blob List - Parquet
- No changes

## Blob List - JSON
- No changes

## Dependencies
- No changes
