package com.sneaksanddata.arcane.framework
package services.mssql

import services.lakehouse.base.IcebergDataRowConverter
import models.{ArcaneType, DataCell, DataRow}

import zio.ZLayer

import java.nio.ByteBuffer
import java.sql.Timestamp

/**
 * Converts JDBC types to Iceberg types.
 *
 * @param dataRow The DataRow to convert.
 * @return A Map of Field names to values that can be used by IcebergCatalogWriter.
 */
class JdbcToIcebergTypeConverter extends IcebergDataRowConverter:

  /**
   * @inheritdoc
   */
  override def convert(dataRow: DataRow): Map[MsSqlQuery, Any] =
    dataRow.map({
      case DataCell(name, ArcaneType.TimestampType, value) => name -> value.asInstanceOf[Timestamp].toLocalDateTime
      case DataCell(name, ArcaneType.ByteArrayType, value) => name -> ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
      case DataCell(name, _, value) => name -> value
    }).toMap


object JdbcToIcebergTypeConverter:

  /**
   * The environment required to create an instance of JdbcToIcebergTypeConverter.
   */
  type Environment = Any

  /**
   * Factory method to create an instance of JdbcToIcebergTypeConverter.
   *
   * @return An instance of JdbcToIcebergTypeConverter.
   */
  def apply(): JdbcToIcebergTypeConverter = new JdbcToIcebergTypeConverter

  /**
   * Factory method to create an instance of JdbcToIcebergTypeConverter.
   *
   * @return An instance of JdbcToIcebergTypeConverter.
   */
  val layer: ZLayer[Environment, Nothing, IcebergDataRowConverter] = ZLayer.succeed(JdbcToIcebergTypeConverter())
