package com.sneaksanddata.arcane.framework
package tests.shared

import services.iceberg.IcebergCatalogCredential
import services.iceberg.base.S3CatalogFileIO
import com.sneaksanddata.arcane.framework.models.settings.IcebergCatalogSettings

object IcebergCatalogInfo:
  val defaultNamespace  = "test"
  val defaultWarehouse  = "demo"
  val defaultCatalogUri = "http://localhost:20001/catalog"

  val defaultSettings: IcebergCatalogSettings = new IcebergCatalogSettings:
    override val namespace: String                         = defaultNamespace
    override val warehouse: String                         = defaultWarehouse
    override val catalogUri: String                        = defaultCatalogUri
    override val additionalProperties: Map[String, String] = IcebergCatalogCredential.oAuth2Properties
    override val s3CatalogFileIO: S3CatalogFileIO          = S3CatalogFileIO
    override val stagingLocation: Option[String]           = None
    override val maxRowsPerFile: Option[Int]               = Some(1000)
