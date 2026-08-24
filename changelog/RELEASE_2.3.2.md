## Framework / Core
- **Watermarking Property Migration**: Moved table watermark storage from the Iceberg `comment` property to a dedicated `arcane-watermark` metadata property. Added automatic fallback and migration logic that reads legacy `comment` watermarks when `arcane-watermark` is not present and migrates them to the new property upon reading or updating.
- **Large Changeset Catchup Split**: Added `changeCaptureRangeLimit` to `ChangeCaptureSettings`. This setting allows to limit the amount of changes included in a single changeset and increase frequency of target watermark updates. This addresses a problem when streams suffering from backpressure or frequent failures can never catch up, as the size of a changeset they need to process to advance watermark grows with each subsequent failure.
- **Memory Bound Shaper Caching**: Added `maxStatisticsAge` configuration parameter to `MemoryBound` throughput settings. The `MemoryBoundShaper` now serializes and caches computed statistics (`recordCount`, `physicalSize`, `rowSize`, `partitions`) directly in target table properties under `mbs-estimates`. Cached statistics are reused for subsequent estimations until `maxStatisticsAge` expires. This reduces the frequency of table scans required to analyze target.
- **Iceberg Sort Order Support**: Restored support for Iceberg table sort orders (`SortOrder`) during target table creation in `DefaultStreamBootstrapper`. Table properties configured with `sortedBy` fields will now construct and apply the Iceberg `SortOrder` (descending with `NULLS_LAST`) on table creation.

## Microsoft Synapse
- **Field Selection Rules**: Updated `SynapseLinkStreamingSource` to consistently apply source field selection rules (`FieldSelectionRuleSettings`) to both root-level and batch-level schemas (`getSchema`, `getBatchSchema`) and emitted data rows.

## Microsoft SQL Server (MsSql)
- No changes

## Blob List (CSV / JSON / Parquet)
- **Field Selection Rules**: Updated blob streaming sources (`BlobListingStreamingSource`, `BlobListingParquetStreamingSource`, `BlobListingJsonStreamingSource`, `BlobListingCsvStreamingSource`) to apply field selection rules (`FieldSelectionRuleSettings`) to both source schema and emitted data rows.

## Dependencies
- No changes

