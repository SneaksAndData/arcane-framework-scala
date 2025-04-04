package com.sneaksanddata.arcane.framework
package services.mssql

import services.base.CanAdd
import utils.SqlUtils.toArcaneType

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Represents the schema of a table in a Microsoft SQL Server database.
 * The schema is represented as a sequence of tuples, where each tuple contains
 * the column name, type (java.sql.Types), precision, and scale.
 */
type SqlSchema = Seq[(String, Int, Int, Int)]

/**
 * Companion object for [[SqlSchema]].
 */
object SqlSchema:

  /**
   * Converts a SQL schema to an Arcane schema and normalizes the field names.
   *
   * @param sqlSchema The SQL schema to convert.
   * @param schema The Arcane schema to add the fields to.
   * @return The Arcane schema with the fields added.
   */
  @tailrec
  def toSchema[Schema: CanAdd](sqlSchema: SqlSchema, schema: Schema): Try[Schema] =
    sqlSchema match
      case Nil => Success(schema)
      case (name, fieldType, precision, scale) +: xs =>
        toArcaneType(fieldType, precision, scale) match
          case Success(arcaneType) => toSchema(xs, schema.addField("\\W+".r.replaceAllIn(name, ""), arcaneType))
          case Failure(exception) => Failure[Schema](exception)

