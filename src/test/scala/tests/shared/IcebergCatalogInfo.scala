package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.iceberg.IcebergStagingSettings
import models.settings.sink.IcebergSinkSettings
import services.iceberg.IcebergCatalogCredential
import services.iceberg.base.S3CatalogFileIO

object IcebergCatalogInfo:
  val defaultNamespace  = "test"
  val defaultWarehouse  = "demo"
  val defaultCatalogUri = "http://localhost:20001/catalog"

  val defaultStagingSettings: IcebergStagingSettings = new IcebergStagingSettings:
    override val namespace: String  = defaultNamespace
    override val warehouse: String  = defaultWarehouse
    override val catalogUri: String = defaultCatalogUri
    override val additionalProperties: Map[String, String] =
      S3CatalogFileIO.properties ++ IcebergCatalogCredential.oAuth2Properties
    override val s3CatalogFileIO: S3CatalogFileIO = S3CatalogFileIO
    override val stagingLocation: Option[String]  = None
    override val maxRowsPerFile: Option[Int]      = Some(1000)

  val defaultSinkSettings: IcebergSinkSettings = new IcebergSinkSettings:
    override val namespace: String                         = defaultNamespace
    override val warehouse: String                         = defaultWarehouse
    override val catalogUri: String                        = defaultCatalogUri
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
