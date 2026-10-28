package com.sneaksanddata.arcane.framework
package models.settings.iceberg

import models.settings.Mergeable
import services.iceberg.IcebergCatalogCredential

import upickle.ReadWriter

case class DefaultIcebergStagingSettings(
    catalogProperties: Map[String, String],
    override val namespace: String,
    override val catalogUri: String,
    override val warehouse: String
) extends IcebergCatalogSettings,
      Mergeable derives ReadWriter:
  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
    case Some(_) => catalogProperties
    case None    => IcebergCatalogCredential.oAuth2Properties ++ catalogProperties

  override type MergeableFrom = OverrideIcebergCatalogSettings
  override type MergeResult   = DefaultIcebergStagingSettings

  override def merge(overrides: Option[MergeableFrom]): MergeResult =
    DefaultIcebergStagingSettings(
      catalogProperties = overrides.flatMap(_.additionalProperties).getOrElse(this.additionalProperties),
      namespace = overrides.flatMap(_.namespace).getOrElse(this.namespace),
      catalogUri = overrides.flatMap(_.catalogUri).getOrElse(this.catalogUri),
      warehouse = overrides.flatMap(_.warehouse).getOrElse(this.warehouse)
    )
