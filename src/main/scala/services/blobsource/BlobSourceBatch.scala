package com.sneaksanddata.arcane.framework
package services.blobsource

import models.schemas.DataRow
import services.blobsource.readers.BlobSourceReader

import java.time.OffsetDateTime

type BlobSourceBatch          = BlobSourceReader#OutputRow

case class BlobSourceVersion(versionNumber: String, waterMarkTime: OffsetDateTime)
