package com.sneaksanddata.arcane.framework
package models.settings.iceberg

import models.serialization.ZIODurationRW.*
import services.iceberg.IcebergCatalogCredential
import services.iceberg.base.S3CatalogFileIO
import com.sneaksanddata.arcane.framework.models.settings.Mergeable
import upickle.ReadWriter

case class DefaultIcebergStagingSettings(
    catalogProperties: Map[String, String],
    override val namespace: String,
    override val catalogUri: String,
    override val warehouse: String,
    override val maxCatalogInstanceLifetime: zio.Duration
) extends IcebergCatalogSettings, Mergeable[DefaultIcebergStagingSettings] derives ReadWriter:
  /** Important to note that currently we do not provide separation between Sink and Staging catalog auth and FileIO
    * implementations. This should be fixed in the future.
    */
  override val additionalProperties: Map[String, String] = sys.env.get("ARCANE_FRAMEWORK__CATALOG_NO_AUTH") match
    case Some(_) => S3CatalogFileIO.properties ++ catalogProperties
    case None    => S3CatalogFileIO.properties ++ IcebergCatalogCredential.oAuth2Properties ++ catalogProperties

  override def merge(base: DefaultIcebergStagingSettings,
                     overrides: DefaultIcebergStagingSettings):
  DefaultIcebergStagingSettings = DefaultIcebergStagingSettings(

    catalogProperties = overrides.catalogProperties,
    namespace = overrides.namespace,
    catalogUri = overrides.catalogUri,
    warehouse = overrides.warehouse,
    maxCatalogInstanceLifetime = overrides.maxCatalogInstanceLifetime
  )

