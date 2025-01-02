package com.sneaksanddata.arcane.framework
package services.base

import models.ArcaneType

import scala.concurrent.Future

/**
 * Type class that represents the ability to add a field to a schema.
 * @tparam Schema The type of the schema.
 */
trait CanAdd[Schema] :
  /**
   * Adds a field to the schema.
   *
   * @return The schema with the field added.
   */
  extension (a: Schema) def addField(fieldName: String, fieldType: ArcaneType): Schema

/**
 * Represents a provider of a schema for a data produced by Arcane.
 *
 * @tparam Schema The type of the schema.
 */
trait SchemaProvider[Schema: CanAdd] {

  type SchemaType = Schema
  /**
   * Gets the schema for the data produced by Arcane.
   *
   * @return A future containing the schema for the data produced by Arcane.
   */
  def getSchema: Future[SchemaType]

  /**
   * Gets an empty schema.
   *
   * @return An empty schema.
   */
  def empty: SchemaType
}
