package com.sneaksanddata.arcane.framework
package services.streaming.processors.transformers

import models.schemas.ArcaneType.TimestampType
import models.schemas.DataCell.isWatermark
import models.schemas.{ArcaneSchema, DataCell, Field}
import services.streaming.base.RowProcessor
import services.time.{CurrentTimeUTCProviderService, TimeProviderService}

import zio.{ULayer, ZLayer}
import zio.stream.ZPipeline

class TimeAppender(
  val cellName: String,
  timeProviderService: TimeProviderService
) extends RowProcessor:

  def appendToSchema(schema: ArcaneSchema): ArcaneSchema = {
    require(
      !schema.exists(_.name.equalsIgnoreCase(cellName)),
      s"Reserved metadata column already exists: $cellName"
    )
    schema.prepended(Field(cellName, TimestampType))
  }

  override def process: ZPipeline[Any, Throwable, Element, Element] =
    ZPipeline.mapChunks { elements =>
      val timeCell = DataCell(cellName, TimestampType, timeProviderService.time)

      elements.map { element =>
        if element.isWatermark then element
        else timeCell :: element
      }
    }

object IngestionTimeAppender
  extends TimeAppender(
    "arcane_ingestion_time",
    CurrentTimeUTCProviderService
  ):

  val layer: ULayer[TimeAppender] = ZLayer.succeed(this)
