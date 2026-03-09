package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.iceberg.IcebergCatalogSettings

import upickle.ReadWriter

/** Connection settings for the Iceberg Catalog associated with the sink
  */
trait IcebergSinkSettings extends IcebergCatalogSettings

case class DefaultIcebergSinkSettings(
    override val additionalProperties: Map[String, String],
    override val namespace: String,
    override val catalogUri: String,
    override val warehouse: String
) extends IcebergSinkSettings derives ReadWriter
