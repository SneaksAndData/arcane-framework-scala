package com.sneaksanddata.arcane.framework
package models.settings.sink

import models.settings.iceberg.IcebergCatalogSettings

import upickle.ReadWriter

case class DefaultIcebergSinkSettings(
    override val additionalProperties: Map[String, String],
    override val namespace: String,
    override val catalogUri: String,
    override val warehouse: String
) extends IcebergCatalogSettings derives ReadWriter
