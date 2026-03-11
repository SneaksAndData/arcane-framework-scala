package com.sneaksanddata.arcane.framework
package models.settings.iceberg

import upickle.ReadWriter

case class DefaultIcebergStagingSettings(
    override val additionalProperties: Map[String, String],
    override val namespace: String,
    override val catalogUri: String,
    override val warehouse: String
) extends IcebergCatalogSettings derives ReadWriter
