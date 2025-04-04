package com.sneaksanddata.arcane.framework
package services.mssql

import services.lakehouse.base.IcebergDataRowConverter

import models.{ArcaneType, DataCell, DataRow}

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
