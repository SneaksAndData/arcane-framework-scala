package com.sneaksanddata.arcane.framework
package services.base

import zio.{IO, Promise, Task}

class FrozenSchemaProvider[T: CanAdd](schema: T):

  type SchemaType = T

  lazy final val getSchema = schema


object FrozenSchemaProvider:
  extension [T: CanAdd](inner: SchemaProvider[T]) def freeze: IO[Throwable, FrozenSchemaProvider[T]] = 
    for promise <- Promise.make[Throwable, T]
      fiber <- promise.complete(inner.getSchema).fork
      s <- promise.await
      _ <- fiber.join
    yield  FrozenSchemaProvider[T](s)

  def apply[T: CanAdd](inner: T): FrozenSchemaProvider[T] = new FrozenSchemaProvider[T](inner)
