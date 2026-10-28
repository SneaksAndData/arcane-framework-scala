package com.sneaksanddata.arcane.framework
package tests.shared

import models.settings.iceberg.IcebergCatalogSettings

object TestIcebergStagingSettings extends IcebergCatalogSettings:
  /** The namespace of the catalog.
    */
  override val namespace: String = "namespace"

  /** The warehouse name of the catalog.
    */
  override val warehouse: String = "warehouse"

  /** The catalog server URI.
    */
  override val catalogUri: String = "http://localhost:8080"

  override val additionalProperties: Map[String, String] = Map(
    "header.X-Iceberg-Access-Delegation" -> "vended-credentials"
  )
