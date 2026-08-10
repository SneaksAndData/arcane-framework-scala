## Framework / Core
- **Error Mapping & Recovery**: Introduced structured exception mapping with `FatalStreamFailException` (for non-recoverable schema or configuration errors) and `TransientStreamFailException` (for recoverable/temporary errors). Added a ZIO `handleAppFailure` extension to gracefully capture application outcomes (SIGTERM/SIGINT stops gracefully with exit code `0`, fatal failures/die with `1`, recoverables with `2`). This should prevent streams from exhausting Kubernetes `backoffLimit` from transient errors, when running under Arcane Operator.
- **Memory Bound Shaper**: Added previously missed account for memory allocations `List[DataCell]` itself. This adds 100 bytes **per each value** for the estimate - update shaper parameters before release! Added a `chunkSizeCap` configuration to cap the generated chunk size, to prevent chunk sizes beyond reasonable hardware capabilities.
- **Data Modification Configurations**: Introduced `DataRowModificationSettings` and the `DataRowModification` trait/ADT. This data model lays foundation for 2.4 release works.
- **Ingestion & Staging Metrics**: Introduced the `rowsStaged` gauge metric (`arcane.stream.rows.staged`) inside `StagingProcessor` and `ShardStagingProcessor` to record rows staged. The `rowsIncoming` counter metric (`arcane.stream.rows.incoming`) now reports row emission rate directly from `DefaultSourceDataProvider`. Comparing this two metrics can help identify sources backpressure.
- **Dependency Injection**: Grouped `ZLayer` vals via `ZLayer.makeSome` to compact plugin DI declarations. Framework now provides plugin-specific `ZLayer` out of box.
- **Logging**: Added framework version print at runtime.
- **Dead Code**: Removed dead code from 2.1 era.
- **CI / Build**: Refactored the release workflow to resolve versions in a separate GitHub Actions job, exposing the final released version as a job output and writing it directly to the build step summary.

## Microsoft Synapse
- **Metrics**: Refactored `SynapseLinkDataProvider` to accept and inject `DeclaredMetrics`, so it can report `rowsIncoming`.

## Microsoft SQL Server (MsSql)
- **Metrics**: Refactored `MsSqlDataProvider` to accept and inject `DeclaredMetrics`, so it can report `rowsIncoming`.

## Blob List (CSV / JSON / Parquet)
- **GZIP Support**: Upgraded `JsonScanner` to seamlessly support decompressing gzip-compressed (`.gz`) input files on-the-fly (`ZPipeline.gunzipAuto()`).
- **Metrics**: Refactored `BlobSourceDataProvider` to accept and inject `DeclaredMetrics`, so it can report `rowsIncoming`.

## Dependencies
- No changes
