package com.sneaksanddata.arcane.framework
package services.blobsource

import models.schemas.DataRow
import services.blobsource.readers.BlobSourceReader

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type BlobSourceBatch          = BlobSourceReader#OutputRow

case class BlobSourceVersion(versionNumber: String, waterMarkTime: OffsetDateTime)

object BlobSourceVersion:
  val epoch: BlobSourceVersion = 
    val start = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
    BlobSourceVersion(versionNumber = start.toEpochSecond.toString, waterMarkTime = start)
  
