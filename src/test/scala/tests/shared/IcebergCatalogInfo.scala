package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.iceberg.IcebergCatalogSettings
import services.iceberg.IcebergCatalogCredential
import services.iceberg.base.S3CatalogFileIO

object IcebergCatalogInfo:
  val defaultNamespace  = "test"
  val defaultWarehouse  = "demo"
  val defaultCatalogUri = "http://localhost:20001/catalog"

  val defaultIcebergStagingSettings: IcebergCatalogSettings = new IcebergCatalogSettings:
    override val namespace: String  = defaultNamespace
    override val warehouse: String  = defaultWarehouse
    override val catalogUri: String = defaultCatalogUri
    override val additionalProperties: Map[String, String] =
      S3CatalogFileIO.properties ++ IcebergCatalogCredential.oAuth2Properties

  val defaultSinkSettings: IcebergCatalogSettings = new IcebergCatalogSettings:
    override val namespace: String                         = defaultNamespace
    override val warehouse: String                         = defaultWarehouse
    override val catalogUri: String                        = defaultCatalogUri
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
