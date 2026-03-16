package com.sneaksanddata.arcane.framework
package services.mssql

import models.schemas.{ArcaneSchema, MergeKeyField, given_CanAdd_ArcaneSchema}
import utils.SqlUtils.{JdbcFieldInfo, toArcaneType}

/** Represents the schema of a table in a Microsoft SQL Server database. The schema is represented as a sequence of
  * tuples, where each tuple contains the column name, type (java.sql.Types), precision, and scale.
  */
type SqlSchema = Seq[(String, Int, Int, Int)]

given Conversion[SqlSchema, ArcaneSchema]:
  // assume that sqlSchema contains merge key and it always comes first
  // check resources/get_select_delta_query.sql
  override def apply(sqlSchema: SqlSchema): ArcaneSchema = sqlSchema
    .foldLeft((ArcaneSchema.empty(), 0)) { case ((agg, fieldIndex), (name, fieldType, precision, scale)) =>
      (
        agg.addIndexedField(
          fieldId = fieldIndex,
          fieldName = "\\W+".r.replaceAllIn(name, ""),
          // propagate failure by resolving Try
          fieldType =
            toArcaneType(new JdbcFieldInfo(name = name, typeId = fieldType, precision = precision, scale = scale)).get
        ),
        fieldIndex + 1
      )
    }
    ._1
