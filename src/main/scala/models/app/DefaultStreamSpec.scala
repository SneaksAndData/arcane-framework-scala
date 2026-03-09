package com.sneaksanddata.arcane.framework
package models.app

import models.settings.backfill.{BackfillBehavior, DefaultBackfillSettings}
import models.settings.observability.DefaultObservabilitySettings
import models.settings.sink.{IcebergSinkSettings, TableMaintenanceSettings}
import models.settings.sources.BufferingStrategy
import models.settings.streaming.ThroughputShaperImpl
import models.settings.{
  DefaultFieldSelectionRuleSettings,
  FieldSelectionRule,
  FieldSelectionRuleSettings,
  JdbcQueryRetryMode,
  TableFormat
}

import upickle.{ReadWriter, macroRW}

import java.time.{Duration, OffsetDateTime}

case class DefaultStreamSpec(
    observability: DefaultObservabilitySettings,
    backfill: DefaultBackfillSettings,
    fieldSelectionRule: DefaultFieldSelectionRuleSettings
) extends StreamSpec:

  // observability
  override def customTags: Map[String, String] = observability.metricTags

  override def merge(other: Option[StreamSpec]): StreamSpec = ???

  // backfillSettings
  override val backfillTableFullName: String             = backfill.backfillTableFullName
  override val backfillStartDate: Option[OffsetDateTime] = backfill.backfillStartDate
  override val backfillBehavior: BackfillBehavior        = backfill.backfillBehavior

  override val rule: FieldSelectionRule = ???

  /** The set of essential fields that must ALWAYS be included in the field selection rule. Fields from this list are
    * used in SQL queries and ALWAYS must be present in the result set. This list is provided by the Arcane streaming
    * plugin and should not be configurable.
    */
  override val essentialFields: Set[String] = ???
  override val isServerSide: Boolean        = ???

  /** Optional max rows per file. Default value is set by catalog writer
    */
  override val maxRowsPerFile: Option[Int] = ???

  /** The namespace (schema) of the catalog.
    */
  override val namespace: String = ???

  /** The warehouse name of the catalog.
    */
  override val warehouse: String = ???

  /** The catalog server URI.
    */
  override val catalogUri: String = ???

  /** The catalog additional properties.
    */
  override val additionalProperties: Map[String, String] = ???

  /** The connection URL.
    */
  override val connectionUrl: String = ???

  /** Optional extra connection parameters for the merge client (tags, session properties etc.)
    */
  override val extraConnectionParameters: Map[String, String] = ???

  /** Enable query retries for JDBC merge
    */
  override val queryRetryMode: JdbcQueryRetryMode = ???

  /** Exp retry base duration
    */
  override val queryRetryBaseDuration: zio.Duration = ???

  /** Exp retry scale factor
    */
  override val queryRetryScaleFactor: Double = ???

  /** Exp retry max attempts
    */
  override val queryRetryMaxAttempts: Int = ???

  /** Exception messages to retry
    */
  override val queryRetryOnMessageContents: List[String] = ???

  /** The name of the target table
    */
  override val targetTableFullName: String = ???

  /** The maintenance settings for the target table
    */
  override val maintenanceSettings: TableMaintenanceSettings = ???

  /** Settings for Iceberg Catalog instance associated with the sink
    */
  override val icebergSinkSettings: IcebergSinkSettings = ???

  /** The buffering strategy to use.
    */
  override val bufferingStrategy: BufferingStrategy = ???

  /** Indicates whether buffering is enabled.
    */
  override val bufferingEnabled: Boolean = ???

  /** How often to check for changes in the source data
    */
  override val changeCaptureIntervalSeconds: Int = ???

  /** The prefix for the staging table name
    */
  override val stagingTablePrefix: String = ???

  /** The name of the catalog where the staging table is located
    */
  override val stagingCatalogName: String = ???

  /** The name of the schema where the staging table is located
    */
  override val stagingSchemaName: String = ???

  /** Indicates that all batches have the same schema. This setting should be hard-coded in the plugin and not exposed
    * in the Stream Spec
    */
  override val isUnifiedSchema: Boolean = ???

  /** Iceberg partitioning expressions to be used in CREATE TABLE ... WITH (partitioning = ARRAY[..], ...) Check out
    * Iceberg specification: https://iceberg.apache.org/spec/#partitioning Trino SQL API:
    * https://trino.io/docs/current/connector/iceberg.html#partitioned-tables
    */
  override val partitionExpressions: Array[String] = ???

  /** Optionally specifies the format of table data files; either PARQUET, ORC, or AVRO. Defaults to the value of the
    * iceberg.file-format catalog configuration property, which defaults to PARQUET.
    */
  override val format: TableFormat = ???

  /** The sort order to be applied during writes to the content of each file written to the table. If the table files
    * are sorted by columns c1 and c2, the sortedBy should be ["c1", "c2"] The sort order applies to the contents
    * written within each output file independently and not the entire dataset.
    */
  override val sortedBy: Array[String] = ???

  /** List of columns to use for Parquet bloom filter. It improves the performance of queries using Equality and IN
    * predicates when reading Parquet files. Requires Parquet format
    */
  override val parquetBloomFilterColumns: Array[String] = ???
  override val shaperImpl: ThroughputShaperImpl         = ???
  override val advisedChunkSize: Int                    = ???
  override val advisedRateChunks: Int                   = ???
  override val advisedRatePeriod: Duration              = ???
  override val advisedChunksBurst: Int                  = ???

  /** The interval for periodic change capture operation.
    */
  override val changeCaptureInterval: Duration = ???

  /** Variance to apply to the `changeCaptureInterval`
    */
  override val changeCaptureJitterVariance: Double = ???

  /** Seed for `changeCaptureJitterVariance`
    */
  override val changeCaptureJitterSeed: Long = ???

  /** Field selection rule settings
    */
  override val fieldSelectionRuleSettings: FieldSelectionRuleSettings = fieldSelectionRule

object DefaultStreamSpec:
  implicit val rw: ReadWriter[DefaultStreamSpec] = macroRW
