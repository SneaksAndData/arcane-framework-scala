package com.sneaksanddata.arcane.framework
package services.caching.schema_cache

import models.schemas.{ArcaneSchema, given_CanAdd_ArcaneSchema}
import services.base.{SchemaCache, SchemaProvider}
import services.caching.schema_cache.FrozenSchemaProvider.freeze

import zio.{Task, ZIO, ZLayer}

import scala.collection.mutable

/**
 * A class that represents a mutable schema provider manager.
 * NOTE: This class is not thread-safe.
 */
class MutableSchemaCache extends SchemaCache:

  private val schemaProviders = mutable.Map.empty[String, FrozenSchemaProvider[ArcaneSchema]]

  def getSchemaProvider(schemaName: String, orElse: String => SchemaProvider[ArcaneSchema]): Task[ArcaneSchema] =
    for
      schemaProvider <- ZIO.succeed(schemaProviders.get(schemaName))
      resolvedSchemaProvider <- schemaProvider match
        case Some(provider) => ZIO.succeed(provider)
        case None =>
          for newProvider <- orElse(schemaName).freeze
            yield {
              schemaProviders.put(schemaName, newProvider)
              newProvider
            }
    yield resolvedSchemaProvider.getSchema

  def refreshSchemaProvider(schemaName: String, refresher: String => SchemaProvider[ArcaneSchema]): Task[Unit] =
    for
      schemaProvider <- refresher(schemaName).freeze
      _ <- ZIO.succeed(schemaProviders.put(schemaName, schemaProvider))
    yield ()


/**
 * Companion object for the MutableSchemaCache class.
 */
object MutableSchemaCache:

  /**
   * The required environment for the MutableSchemaCache.
   */
  type Environment = Any

  /**
   * Creates a new mutable schema cache.
   *
   * @return A new mutable schema cache.
   */
  def apply(): MutableSchemaCache = new MutableSchemaCache()

  /**
   * The ZLayer for the MutableSchemaCache.
   */
  val layer: zio.ZLayer[Environment, Nothing, MutableSchemaCache] = ZLayer.succeed(MutableSchemaCache())
