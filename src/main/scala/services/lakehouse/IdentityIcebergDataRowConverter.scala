package com.sneaksanddata.arcane.framework
package services.lakehouse

import models.DataRow
import services.lakehouse.base.IcebergDataRowConverter

import zio.{ULayer, ZLayer}

/**
 * IdentityIcebergDataRowConverter is a simple implementation of IcebergDataRowConverter that
 * converts a DataRow to a Map[String, Any] without any transformation.
 * Can be used when the DataRow is already in the format that can be used by IcebergCatalogWriter or
 * for backward compatibility with the previous versions of the code.
 *
 * @param dataRow The DataRow to convert.
 * @return A Map of Field names to values that can be used by IcebergCatalogWriter.
 */
class IdentityIcebergDataRowConverter extends IcebergDataRowConverter:
  /**
   * @inheritdoc
   */
  override def convert(dataRow: DataRow): Map[String, Any] = dataRow.map { cell => cell.name -> cell.value }.toMap


object IdentityIcebergDataRowConverter:
  /**
   * The environment required to create an instance of IdentityIcebergDataRowConverter.
   */
  type Environment = Any

  /**
   * Factory method to create an instance of IdentityIcebergDataRowConverter.
   *
   * @return An instance of IdentityIcebergDataRowConverter.
   */
  def apply(): IdentityIcebergDataRowConverter = new IdentityIcebergDataRowConverter

  /**
   * Factory method to create an instance of IdentityIcebergDataRowConverter.
   *
   * @return An instance of IdentityIcebergDataRowConverter.
   */
  val layer: ZLayer[Any, Nothing, IdentityIcebergDataRowConverter] =
    ZLayer.succeed(IdentityIcebergDataRowConverter())