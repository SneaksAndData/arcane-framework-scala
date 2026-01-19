package com.sneaksanddata.arcane.framework
package services.blobsource

import models.schemas.DataRow
import services.blobsource.readers.BlobSourceReader

import java.time.{Instant, OffsetDateTime, ZoneOffset}

type BlobSourceBatch = BlobSourceReader#OutputRow

