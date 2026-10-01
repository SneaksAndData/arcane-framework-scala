package com.sneaksanddata.arcane.framework
package models.settings.iceberg

import models.serialization.ZIODurationRW.*

import upickle.ReadWriter

/** A partial override of `IcebergCatalogSettings` where every field is optional to support override/patch-style JSON
  * deserialization.
  */
trait OverrideIcebergCatalogSettings:
  /** Optional override for the catalog namespace.
    */
  val namespace: Option[String]

  /** Optional override for the warehouse name.
    */
  val warehouse: Option[String]

  /** Optional override for the catalog URI.
    */
  val catalogUri: Option[String]

  /** Optional override for the additional catalog properties.
    */
  val additionalProperties: Option[Map[String, String]]

  /** Optional override for the maximum lifetime of the catalog instance.
    */
  val maxCatalogInstanceLifetime: Option[zio.Duration]

/** Default implementation for `OverrideIcebergCatalogSettings` using optional values.
  */
case class DefaultOverrideIcebergCatalogSettings(
    override val namespace: Option[String] = None,
    override val warehouse: Option[String] = None,
    override val catalogUri: Option[String] = None,
    override val additionalProperties: Option[Map[String, String]] = None,
    override val maxCatalogInstanceLifetime: Option[zio.Duration] = None
) extends OverrideIcebergCatalogSettings derives ReadWriter
