## Framework
- Unified implementation of Streaming Data Providers for all sources under `DefaultStreamDataProvider`
- Unified change detection for all source with `SourceWatermark`:
  - Current watermark carries source-specific version value. This version corresponds to the latest fully committed changeset
  - All changes between current watermark and latest available in the source are emitted as a separate `ZStream`
  - Once the changeset exhausts, an updated watermark is emitted and applied to target
- Deprecated use of `lookbackInterval` - only used as a fallback on non-watermarked targets.
- Developer API: Started specialization of Iceberg API usage by cutting DDL functionality out of IcebergCatalogWriter.

## Microsoft Synapse
- Updated version tracking and change emission to support `DefaultStreamDataProvider`

## Microsoft SQL Server
- Updated version tracking and change emission to support `DefaultStreamDataProvider`

## Blob List - Parquet
- Updated version tracking and change emission to support `DefaultStreamDataProvider`
- Added support externally provided schema

## Blob List - JSON
- Updated version tracking and change emission to support `DefaultStreamDataProvider`

## Dependencies
- Bump ZIO to `2.1.24`