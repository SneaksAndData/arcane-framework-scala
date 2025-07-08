package com.sneaksanddata.arcane.framework
package services.base

import models.schemas.ArcaneSchema

import zio.Task

/** A trait that represents a mapping between table names and schema providers.
  */
trait SchemaCache:

  /** Gets the schema provider for the specified schema name.
    * @param schemaName
    *   The name of the schema.
    * @param orElse
    *   The function to call if the schema provider is not found.
    * @return
    *   The schema provider.
    */
  def getSchemaProvider(schemaName: String, orElse: String => SchemaProvider[ArcaneSchema]): Task[ArcaneSchema]

  /** Refreshes the schema provider for the specified schema name.
    * @param schemaName
    *   The name of the schema.
    * @param refresher
    *   The function to call to refresh the schema provider.
    */
  def refreshSchemaProvider(schemaName: String, refresher: String => SchemaProvider[ArcaneSchema]): Task[Unit]
