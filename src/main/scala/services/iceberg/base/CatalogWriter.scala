package com.sneaksanddata.arcane.framework
package services.iceberg.base

import models.schemas.DataRow

import org.apache.iceberg.{Schema, Table}
import zio.logging.LogAnnotation
import zio.Task

trait CatalogWriter[CatalogImpl, TableImpl, SchemaImpl]:

  /** Creates a table published to the configured Catalog from the data provided.
    * @param data
    *   Rows to append to the table
    * @param name
    *   Name for the table in the catalog
    * @param logAnnotations
    *   Annotations to pass for all log messages
    * @return
    *   Reference to the created table
    */
  def write(
      data: Iterable[DataRow],
      name: String,
      schema: SchemaImpl,
      logAnnotations: Seq[(LogAnnotation[String], String)]
  ): Task[TableImpl]

  /** Appends provided rows to the table.
    * @param data
    *   Rows to append
    * @param name
    *   Table to append to
    * @return
    *   Reference to the updated table
    */
  def append(
      data: Iterable[DataRow],
      name: String,
      schema: SchemaImpl,
      logAnnotations: Seq[(LogAnnotation[String], String)]
  ): Task[TableImpl]
