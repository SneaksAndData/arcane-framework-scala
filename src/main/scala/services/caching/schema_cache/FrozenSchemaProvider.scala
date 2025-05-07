package com.sneaksanddata.arcane.framework
package services.caching.schema_cache

import services.base.{CanAdd, SchemaProvider}

import zio.{IO, Promise}

/** A trait that represents a schema provider that holds a schema and not reads it from a storage on every call.
  * @param schema
  *   The schema to provide.
  * @param canAdd$T$0
  *   The type class that provides the ability to add schemas.
  * @tparam T
  *   The type of the schema.
  */
class FrozenSchemaProvider[T: CanAdd](schema: T):

  /** The type of the schema.
    */
  type SchemaType = T

  /** The schema.
    */
  lazy final val getSchema = schema

object FrozenSchemaProvider:

  /** Freezes the schema provider.
    * @param inner
    *   The schema provider to freeze.
    * @tparam T
    *   The type of the schema.
    * @return
    *   A frozen schema provider.
    */
  extension [T: CanAdd](inner: SchemaProvider[T])
    def freeze: IO[Throwable, FrozenSchemaProvider[T]] =
      for
        promise <- Promise.make[Throwable, T]
        fiber   <- promise.complete(inner.getSchema).fork
        s       <- promise.await
        _       <- fiber.join
      yield FrozenSchemaProvider[T](s)

  /** Creates a frozen schema provider.
    * @param inner
    *   The schema provider to freeze.
    * @tparam T
    *   The type of the schema.
    * @return
    *   A frozen schema provider instance.
    */
  def apply[T: CanAdd](inner: T): FrozenSchemaProvider[T] = new FrozenSchemaProvider[T](inner)
