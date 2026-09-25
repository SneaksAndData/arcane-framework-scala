package com.sneaksanddata.arcane.framework
package models.settings.staging

import upickle.ReadWriter

/** A partial override of `StagingTableSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideStagingTableSettings:
  /** Optional override for the staging catalog name.
    */
  val stagingCatalogName: Option[String]

  /** Optional override for the staging schema name.
    */
  val stagingSchemaName: Option[String]

  /** Optional override for whether all batches share the same schema.
    */
  val isUnifiedSchema: Option[Boolean]

  /** Optional override for the maximum rows per file.
    */
  val maxRowsPerFile: Option[Int]

/** Default implementation for `OverrideStagingTableSettings` using optional values.
  */
case class DefaultOverrideStagingTableSettings(
    override val stagingCatalogName: Option[String] = None,
    override val stagingSchemaName: Option[String] = None,
    override val isUnifiedSchema: Option[Boolean] = None,
    override val maxRowsPerFile: Option[Int] = None
) extends OverrideStagingTableSettings derives ReadWriter
