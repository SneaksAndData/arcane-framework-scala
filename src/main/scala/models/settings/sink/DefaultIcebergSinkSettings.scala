package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.serialization.ZIODurationRW.*
import models.settings.Mergeable
import models.settings.iceberg.{IcebergCatalogSettings, OverrideIcebergCatalogSettings}
import services.iceberg.IcebergCatalogCredential
import services.iceberg.base.S3CatalogFileIO

import upickle.ReadWriter

case class DefaultIcebergSinkSettings(
    catalogProperties: Map[String, String],
    override val namespace: String,
    override val catalogUri: String,
    override val warehouse: String,
    override val maxCatalogInstanceLifetime: zio.Duration
) extends IcebergCatalogSettings,
      Mergeable derives ReadWriter:
  /** Important to note that currently we do not provide separation between Sink and Staging catalog auth and FileIO
    * implementations. This should be fixed in the future.
    */
  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
    case Some(_) => S3CatalogFileIO.properties ++ catalogProperties
    case None    => S3CatalogFileIO.properties ++ IcebergCatalogCredential.oAuth2Properties ++ catalogProperties

  override type MergeableFrom = OverrideIcebergCatalogSettings
  override type MergeResult   = DefaultIcebergSinkSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultIcebergSinkSettings(
      catalogProperties = overrides.flatMap(_.additionalProperties).getOrElse(this.additionalProperties),
      namespace = overrides.flatMap(_.namespace).getOrElse(this.namespace),
      catalogUri = overrides.flatMap(_.catalogUri).getOrElse(this.catalogUri),
      warehouse = overrides.flatMap(_.warehouse).getOrElse(this.warehouse),
      maxCatalogInstanceLifetime = overrides.flatMap(_.maxCatalogInstanceLifetime).getOrElse(this.maxCatalogInstanceLifetime)
    )
