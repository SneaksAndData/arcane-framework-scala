package com.sneaksanddata.arcane.framework
package services.merging

import services.base.{FrozenSchemaProvider, SchemaProvider}

import com.sneaksanddata.arcane.framework.models.ArcaneSchema
import com.sneaksanddata.arcane.framework.services.base.FrozenSchemaProvider.freeze
import com.sneaksanddata.arcane.framework.models.given_CanAdd_ArcaneSchema
import zio.{Task, ZIO, ZLayer}

import scala.collection.mutable

/**
 * A class that represents a mutable schema provider manager.
 * NOTE: This class is not thread-safe.
 */
class MutableSchemaCache:

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
