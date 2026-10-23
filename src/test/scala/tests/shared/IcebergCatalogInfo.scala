package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.iceberg.IcebergCatalogSettings
import services.iceberg.IcebergCatalogCredential

object IcebergCatalogInfo:
  val defaultNamespace  = "test"
  val defaultWarehouse  = "demo"
  val defaultCatalogUri = "http://localhost:20001/catalog"

  val defaultIcebergStagingSettings: IcebergCatalogSettings = new IcebergCatalogSettings:
    override val namespace: String  = defaultNamespace
    override val warehouse: String  = defaultWarehouse
    override val catalogUri: String = defaultCatalogUri
    override val additionalProperties: Map[String, String] =
      Map(
        "header.X-Iceberg-Access-Delegation" -> "vended-credentials"
      ) ++ IcebergCatalogCredential.oAuth2Properties
    override val maxCatalogInstanceLifetime: zio.Duration = zio.Duration.fromSeconds(3600)

  val defaultSinkSettings: IcebergCatalogSettings = new IcebergCatalogSettings:
    override val namespace: String  = defaultNamespace
    override val warehouse: String  = defaultWarehouse
    override val catalogUri: String = defaultCatalogUri
    override val additionalProperties: Map[String, String] =
      Map(
        "header.X-Iceberg-Access-Delegation" -> "vended-credentials"
      ) ++ IcebergCatalogCredential.oAuth2Properties
    override val maxCatalogInstanceLifetime: zio.Duration = zio.Duration.fromSeconds(3600)
