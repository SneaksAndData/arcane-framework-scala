package com.sneaksanddata.arcane.framework
package services.iceberg.base

import extensions.ZExtensions.onComplete
import logging.ZIOLogAnnotations.zlog
import models.schemas.DataRow

import zio.stream.ZStream
import zio.{Task, ZIO}

import java.io.File

abstract class BlobScanner:
  protected def getRowStream: ZStream[Any, Throwable, DataRow]

  /**
   * A row stream from scanned blobs. Note: temporarily this returns a stream of DataRow objects. Once
   * * https://github.com/SneaksAndData/arcane-framework-scala/issues/181 is resolved, conversion from GenericRecord to
   * * DataRow will be removed.
   *
   * @return
   */
  def getRows: ZStream[Any, Throwable, DataRow] = getRowStream.onComplete(cleanup)

  /**
   * Blob cleanup method
   * @return
   */
  def cleanup: Task[Unit]
  
