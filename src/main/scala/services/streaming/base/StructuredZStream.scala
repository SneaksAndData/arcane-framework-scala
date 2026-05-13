package com.sneaksanddata.arcane.framework
package services.streaming.base

import models.schemas.{ArcaneSchema, DataRow}

import zio.stream.ZStream

/** ZStream that carries a schema information
  */
type StructuredZStream = (ZStream[Any, Throwable, DataRow], ArcaneSchema)
